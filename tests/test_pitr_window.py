"""Unit tests for pitr/window.py — the pure PITR window/floor/gap validation shared by
invoke-oplog-replay (all-or-nothing pre-replay gate) and restore (--pitr-target pre-overwrite gate)."""

import sys
from pathlib import Path

# Make the package importable when running from the repo root without an editable install.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mongodb_flasharray_backup.pitr import window  # noqa: E402


def seg(start, end):
    return (start, end, f"{start}_{end}.oplogs")


# --------------------------------------------------------------------------------------------------------
# Filename parsing + directory listing
# --------------------------------------------------------------------------------------------------------

def test_parse_segment_name():
    assert window.parse_segment_name("100_200.oplogs") == (100, 200)
    assert window.parse_segment_name("gap-20260801-101010.json") is None
    assert window.parse_segment_name("100_200.oplogs.partial") is None
    assert window.parse_segment_name("state.json") is None


def test_list_segments_sorted_and_filtered(tmp_path):
    d = tmp_path / "segments"
    d.mkdir()
    for name in ("300_400.oplogs", "100_200.oplogs", "200_300.oplogs", "junk.txt"):
        (d / name).write_text("")
    assert window.list_segments(d) == [seg(100, 200), seg(200, 300), seg(300, 400)]
    assert window.list_segments(tmp_path / "missing") == []


# --------------------------------------------------------------------------------------------------------
# Windowing — mirrors the replay plan's filter exactly
# --------------------------------------------------------------------------------------------------------

def test_window_drops_pre_t1_and_post_target():
    segs = [seg(100, 200), seg(200, 300), seg(300, 400), seg(410, 500)]
    # end <= t1 dropped; start > target dropped. A segment starting exactly AT the target would stay
    # (oplogLimit trims inside it) — 410 > 400 is the strictly-after case that drops.
    assert window.window_segments(segs, t1=200, target=400) == [seg(200, 300), seg(300, 400)]
    # 0 disables each bound.
    assert window.window_segments(segs, t1=0, target=0) == segs


def test_window_keeps_boundary_spanning_segments():
    segs = [seg(100, 200), seg(200, 300)]
    # A segment SPANNING t1 (start < t1 < end) stays — it carries the first post-anchor entries.
    assert window.window_segments(segs, t1=150, target=0) == segs
    # A segment starting exactly AT the target stays (its first entries may be <= target).
    assert window.window_segments([seg(100, 200), seg(200, 300)], t1=0, target=200) == [
        seg(100, 200), seg(200, 300)
    ]


# --------------------------------------------------------------------------------------------------------
# Holes / anchor / coverage
# --------------------------------------------------------------------------------------------------------

def test_find_hole():
    assert window.find_hole([seg(100, 200), seg(200, 300)]) is None
    assert window.find_hole([seg(100, 200), seg(260, 300)]) == (200, 260)
    # Overlap is not a hole (duplicate coverage is harmless — oplog application is idempotent).
    assert window.find_hole([seg(100, 220), seg(200, 300)]) is None
    assert window.find_hole([]) is None


def test_anchor_hole():
    # First windowed segment starts after T1 -> entries in (T1, start) were never captured.
    assert window.anchor_hole([seg(260, 300)], t1=200) is True
    assert window.anchor_hole([seg(200, 300)], t1=200) is False
    assert window.anchor_hole([seg(150, 300)], t1=200) is False
    assert window.anchor_hole([seg(260, 300)], t1=0) is False  # unknown anchor: cannot judge
    assert window.anchor_hole([], t1=200) is False


def test_covers():
    assert window.covers([seg(100, 200), seg(200, 300)], target=250) is True
    assert window.covers([seg(100, 200), seg(200, 300)], target=300) is True
    assert window.covers([seg(100, 200)], target=300) is False
    assert window.covers([], target=300) is False
    assert window.covers([], target=0) is True  # replay-all has nothing to reach


# --------------------------------------------------------------------------------------------------------
# Floor tag parsing + violations
# --------------------------------------------------------------------------------------------------------

def test_floors_from_tag():
    tag = '{"aen-shard_1":{"t":1784831329,"i":5},"config":{"t":1784831330}}'
    assert window.floors_from_tag(tag) == {
        "aen-shard_1": (1784831329, 5),
        "config": (1784831330, 0),
    }
    assert window.floors_from_tag(None) == {}
    assert window.floors_from_tag("") == {}
    assert window.floors_from_tag("not json") == {}


def test_floor_violations():
    floors = {"s1": (1000, 2), "s2": (2000, 1)}
    # Below s2's floor only.
    assert window.floor_violations(floors, 1500) == {"s2": (2000, 1)}
    # Equal to the floor second is allowed (oplogLimit trims within the second).
    assert window.floor_violations(floors, 2000) == {}
    # Above every floor.
    assert window.floor_violations(floors, 3000) == {}
    # target 0 = replay-all: floor cannot be violated.
    assert window.floor_violations(floors, 0) == {}


# --------------------------------------------------------------------------------------------------------
# Gap-marker relevance (scoped to the replay window)
# --------------------------------------------------------------------------------------------------------

def gap(lo, hi):
    return {"storedLastEnd": {"time": lo, "inc": 1}, "omPreviousEnd": {"time": hi, "inc": 1}}


def test_gap_relevance_scoping():
    # Inside the window -> relevant.
    assert window.gap_is_relevant(gap(250, 260), t1=200, target=400) is True
    # Entirely at/before T1 -> already inside the snapshot, irrelevant.
    assert window.gap_is_relevant(gap(100, 200), t1=200, target=400) is False
    # Entirely at/after the target -> beyond the replay, irrelevant.
    assert window.gap_is_relevant(gap(400, 500), t1=200, target=400) is False
    # Straddling T1 -> relevant.
    assert window.gap_is_relevant(gap(150, 260), t1=200, target=400) is True
    # target 0 (replay-all): anything after T1 is relevant.
    assert window.gap_is_relevant(gap(400, 500), t1=200, target=0) is True
    # Unparseable marker -> conservative: relevant.
    assert window.gap_is_relevant({}, t1=200, target=400) is True


# --------------------------------------------------------------------------------------------------------
# validate_shard_window — the composed check
# --------------------------------------------------------------------------------------------------------

def test_validate_ok():
    segs = [seg(100, 200), seg(200, 300), seg(300, 400)]
    assert window.validate_shard_window("s1", segs, t1=150, target=350) == []


def test_validate_reports_interior_hole():
    segs = [seg(100, 200), seg(260, 300), seg(300, 400)]
    problems = window.validate_shard_window("s1", segs, t1=150, target=350)
    assert len(problems) == 1 and "hole inside the replay window" in problems[0]


def test_validate_reports_anchor_hole():
    segs = [seg(260, 300), seg(300, 400)]
    problems = window.validate_shard_window("s1", segs, t1=200, target=350)
    assert len(problems) == 1 and "anchor" in problems[0]


def test_validate_reports_short_coverage():
    segs = [seg(200, 300)]
    problems = window.validate_shard_window("s1", segs, t1=200, target=500)
    assert len(problems) == 1 and "before the target" in problems[0]


def test_validate_empty_window():
    # Bounded target + nothing captured in the window -> failure.
    problems = window.validate_shard_window("s1", [seg(100, 150)], t1=200, target=400)
    assert len(problems) == 1 and "cannot be reached" in problems[0]
    # Replay-all + nothing captured -> no problem (no-op shard).
    assert window.validate_shard_window("s1", [seg(100, 150)], t1=200, target=0) == []


def test_validate_gap_past_target_is_ignored():
    # The hole sits beyond the target; the window that will be replayed is complete.
    segs = [seg(200, 300), seg(300, 400), seg(460, 500)]
    assert window.validate_shard_window("s1", segs, t1=200, target=380) == []
