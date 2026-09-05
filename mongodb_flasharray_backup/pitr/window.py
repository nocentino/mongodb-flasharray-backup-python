"""PITR window validation - pure functions shared by invoke_oplog_replay (pre-replay, all-or-nothing)
and restore (optional pre-overwrite gate).

Concepts:
  T1 / anchor  - the backup cursor's snapshot timestamp (mongo:t1ts tag). Segments ending at/before it
                 are already contained in the volume snapshot and are filtered from replay.
  floor        - the snapshot's true earliest restorable point-in-time: the per-shard oplog head read
                 right AFTER the FlashArray snapshot fired (mongo:floor tag). A volume snapshot restores
                 to its on-disk state (~creation), and oplog replay only rolls FORWARD - so a PIT target
                 in [anchor, floor) is unreachable and would silently return MORE data than requested
                 (an undone drop can come back). The floor is read after the FA snap, so it is >= the
                 true on-disk state: the guard can only over-reject, never permit an over-restore.
  window       - the (T1, target] slice of captured segments a replay will actually apply. All
                 contiguity/coverage checks are scoped to this window: a gap AFTER the target must not
                 abort a valid restore, and a window that does not start at/before T1 or does not reach
                 the target is incomplete and must abort BEFORE anything is mutated.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

_SEGMENT_RE = re.compile(r"^(\d+)_(\d+)\.oplogs$")


def parse_segment_name(name: str) -> Optional[tuple[int, int]]:
    """(startTs, endTs) from an OM segment filename '<startTs>_<endTs>.oplogs', else None."""
    m = _SEGMENT_RE.match(name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def list_segments(seg_dir: Path) -> list[tuple[int, int, str]]:
    """Sorted [(startTs, endTs, filename)] for every parseable .oplogs file in seg_dir."""
    out: list[tuple[int, int, str]] = []
    if not seg_dir.exists():
        return out
    for f in seg_dir.iterdir():
        if not f.is_file():
            continue
        parsed = parse_segment_name(f.name)
        if parsed:
            out.append((parsed[0], parsed[1], f.name))
    return sorted(out)


def window_segments(
    segments: list[tuple[int, int, str]], t1: int, target: int
) -> list[tuple[int, int, str]]:
    """The (T1, target] replay window: drops segments entirely at/before T1 and entirely after the
    target. Mirrors the replay plan's filter exactly. t1/target of 0 disable that bound."""
    out = []
    for start, end, name in segments:
        if t1 > 0 and end <= t1:
            continue
        if target > 0 and start > target:
            continue
        out.append((start, end, name))
    return out


def find_hole(windowed: list[tuple[int, int, str]]) -> Optional[tuple[int, int]]:
    """(prev_end, next_start) of the first discontinuity between consecutive windowed segments, or
    None when the chain is contiguous. Duplicate/overlapping coverage is not a hole."""
    for (_, prev_end, _), (next_start, _, _) in zip(windowed, windowed[1:]):
        if next_start > prev_end:
            return (prev_end, next_start)
    return None


def anchor_hole(windowed: list[tuple[int, int, str]], t1: int) -> bool:
    """True when T1 is known and the first windowed segment starts AFTER it - oplog between the
    snapshot anchor and the first captured segment was never captured (unrecoverable hole)."""
    if t1 <= 0 or not windowed:
        return False
    return windowed[0][0] > t1


def covers(windowed: list[tuple[int, int, str]], target: int) -> bool:
    """True when the window reaches the target (target 0 = replay-all: nothing to reach)."""
    if target <= 0:
        return True
    if not windowed:
        return False
    return max(end for _, end, _ in windowed) >= target


def floors_from_tag(tag_value: Optional[str]) -> dict[str, tuple[int, int]]:
    """Floors parsed from the mongo:floor tag JSON; {} when absent/unparseable.

    Two formats: the compact cluster-wide form '{"t":..,"i":..,"shards":N}' (the MAX floor across
    shards — written by the snapshot; scale-proof for FA tag value limits) is returned as
    {"cluster-max": (t, i)}; the legacy per-shard form '{"<shard>":{"t":..,"i":..},...}' is returned
    as {shardId: (t, i)}. The refusal decision is identical either way (target must be >= every
    floor <=> target >= the max)."""
    if not tag_value:
        return {}
    try:
        raw = json.loads(tag_value)
        if isinstance(raw, dict) and "t" in raw:
            return {"cluster-max": (int(raw["t"]), int(raw.get("i", 0)))}
        out = {}
        for shard, ts in raw.items():
            out[str(shard)] = (int(ts["t"]), int(ts.get("i", 0)))
        return out
    except Exception:  # noqa: BLE001 - a malformed tag disables the guard rather than crashing
        return {}


def floor_violations(floors: dict[str, tuple[int, int]], target: int) -> dict[str, tuple[int, int]]:
    """{shardId: floor} for every shard whose floor the target falls strictly below (in whole
    seconds - a target equal to the floor second is allowed; --oplogLimit trims within it)."""
    if target <= 0:
        return {}
    return {shard: fl for shard, fl in floors.items() if target < fl[0]}


def gap_is_relevant(gap: dict, t1: int, target: int) -> bool:
    """Whether a tailer gap marker intersects the (T1, target] window. A gap spans
    [storedLastEnd -> omPreviousEnd]; one entirely at/before T1 is inside the snapshot, one at/after
    the target is beyond the replay. Unparseable markers are treated as relevant (conservative)."""
    try:
        gap_lo = int(gap["storedLastEnd"]["time"])
        gap_hi = int(gap["omPreviousEnd"]["time"])
    except Exception:  # noqa: BLE001
        return True
    if t1 > 0 and gap_hi <= t1:
        return False
    if target > 0 and gap_lo >= target:
        return False
    return True


def validate_shard_window(
    shard_id: str,
    segments: list[tuple[int, int, str]],
    t1: int,
    target: int,
) -> list[str]:
    """Problems (empty = valid) with one shard's captured stream for a replay to `target`:
    missing segments (when a target must be reached), an anchor hole, an interior hole, or a window
    that stops short of the target. Pure - callers decide whether a problem refuses or warns."""
    problems: list[str] = []
    windowed = window_segments(segments, t1, target)
    if not windowed:
        if target > 0:
            problems.append(
                f"{shard_id}: no captured segments in the (T1, target] window - the target cannot be reached"
            )
        return problems
    if anchor_hole(windowed, t1):
        problems.append(
            f"{shard_id}: hole between the snapshot anchor (T1={t1}) and the first captured segment "
            f"(starts {windowed[0][0]}) - oplog in between was never captured"
        )
    hole = find_hole(windowed)
    if hole:
        problems.append(
            f"{shard_id}: hole inside the replay window - coverage stops at {hole[0]} and resumes at "
            f"{hole[1]} (a mid-stream segment is missing)"
        )
    if not covers(windowed, target):
        last_end = max(end for _, end, _ in windowed)
        problems.append(
            f"{shard_id}: captured window ends at {last_end}, before the target {target} - drain the "
            "tailer past the target (segments lag live writes by ~2-3 min) or lower the target"
        )
    return problems
