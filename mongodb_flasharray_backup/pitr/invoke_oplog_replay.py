#!/usr/bin/env python3
"""Apply captured oplog segments to a cluster restored from FA snapshot.

After restoring from a FlashArray snapshot (which puts the cluster at time T1), this script
applies the oplog segments captured by the oplog tailer up to a target timestamp T2,
advancing the cluster to exactly T2 without any additional human intervention.

Each shard's .oplogs segments are applied in capture order directly to that shard's primary
using mongorestore --oplogReplay --oplogFile. --oplogLimit is applied to any segment whose
end timestamp extends beyond --target-timestamp. Replay is per-shard (not via mongos) because
the oplog is shard-local.

Usage:
  python -m mongodb_flasharray_backup.pitr.invoke_oplog_replay --snapshot-tag "om-20260505-200455"
  python -m mongodb_flasharray_backup.pitr.invoke_oplog_replay --snapshot-tag "om-20260505-200455" --target-timestamp 1778030500
"""

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import typer

from .. import config
from . import window


# ------------------------------------------------------------------------------------------------
# Console colors not exported by config.py.
# ------------------------------------------------------------------------------------------------
WHITE = "bright_white"
DARK_CYAN = "cyan"
GRAY = "white"


# Enforce the ^om-\d{8}-\d{6}$ pattern on the snapshot tag.
def _validate_snapshot_tag(value: str) -> str:
    if not re.match(r"^om-\d{8}-\d{6}$", value):
        raise typer.BadParameter("SnapshotTag must match pattern ^om-\\d{8}-\\d{6}$")
    return value


# ------------------------------------------------------------------------------------------------
# Tee stdout + stderr to the log file while still writing to console, so console output
# (config.write_host / typer.secho) and command output are captured in the appended log.
# ------------------------------------------------------------------------------------------------
class _Tee:
    def __init__(self, stream, log_handle):
        self._stream = stream
        self._log = log_handle

    def write(self, data):
        self._stream.write(data)
        self._log.write(data)
        return len(data)

    def flush(self):
        self._stream.flush()
        self._log.flush()

    def __getattr__(self, name):
        return getattr(self._stream, name)


def _run(
    snapshot_tag: str = typer.Option(
        ...,
        "--snapshot-tag",
        callback=_validate_snapshot_tag,
        help="Snapshot tag to replay (pattern: om-YYYYMMDD-HHMMSS)",
    ),
    # 0 = replay all; Unix epoch seconds otherwise
    target_timestamp: int = typer.Option(
        0,
        "--target-timestamp",
        help="0 = replay all; Unix epoch seconds otherwise",
    ),
    verify_database: str = typer.Option(
        "testdb",
        "--verify-database",
        help="database to count docs in during post-replay smoke test",
    ),
    t2_mark_path: str = typer.Option(
        "",
        "--t2-mark-path",
        help="optional: t2-mark.json with counts captured after stop-load (default: <OplogDir>/t2-mark.json)",
    ),
    skip_verification: bool = typer.Option(
        False,
        "--skip-verification",
        help="opt out of post-replay range-bound assertion (counts are still printed)",
    ),
    allow_gaps: bool = typer.Option(
        False,
        "--allow-gaps",
        help="proceed even if the replay window has gap markers or missing/short coverage "
        "(PIT coverage across a gap is unrecoverable; only data up to the first gap is trustworthy)",
    ),
    allow_floor_override: bool = typer.Option(
        False,
        "--allow-floor-override",
        help="UNSAFE: replay to a target below the snapshot's recorded on-disk floor (mongo:floor). "
        "The result will contain MORE data than the target implies (an undone drop can come back).",
    ),
    replay_timeout_sec: int = typer.Option(
        3600,
        "--replay-timeout-sec",
        help="Per-segment timeout for the remote mongorestore --oplogReplay. Under heavy write load a "
        "segment can take minutes; raise for very large recovery windows.",
    ),
    deployment: str = typer.Option(
        None,
        "--deployment",
        help="Deployment name to replay (selects '<NAME>__' keys in .env). Omit to use the flat keys.",
    ),
):
    # Load config FIRST (throws without .env).
    config.load_config(deployment=deployment)

    # SIGTERM must behave like Ctrl-C so an interrupted replay reports honestly instead of dying cold.
    config.install_sigterm_handler()

    # Log dir + log file appended to during this run.
    log_dir = Path(os.path.expanduser("~")) / "mongo-oplogreplay-logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"oplogreplay-{snapshot_tag}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
    log_handle = open(log_path, "a", encoding="utf-8")
    orig_stdout = sys.stdout
    orig_stderr = sys.stderr
    sys.stdout = _Tee(orig_stdout, log_handle)
    sys.stderr = _Tee(orig_stderr, log_handle)

    try:

        # Oplog stream directory + existence check.
        oplog_dir = Path(os.path.expanduser("~")) / "mongo-oplog-stream" / snapshot_tag
        if not oplog_dir.exists():
            raise RuntimeError(
                f"Oplog stream directory not found at {oplog_dir}. Run start-oplog-tailer first."
            )

        # NOTE: gap markers are checked further below, once T1 (mongo:t1ts) is known — so a gap that
        # sits entirely OUTSIDE the (T1, target] replay window (already inside the snapshot, or past
        # the target) never blocks a valid restore.

        # Header + replay target description.
        config.write_host(f"\n=== Oplog Replay for snapshot {snapshot_tag} ===", fg=config.YELLOW)
        config.write_host(f"  Source : {oplog_dir}", fg=config.CYAN)
        if target_timestamp > 0:
            # Render the Unix target timestamp as an ISO-8601 UTC datetime.
            target_dt = (
                datetime.fromtimestamp(target_timestamp, tz=timezone.utc)
                .replace(tzinfo=None)
                .isoformat()
            )
            config.write_host(f"  Replay to: {target_dt} (Unix {target_timestamp})", fg=config.CYAN)
        else:
            config.write_host("  Replay to: end of captured oplog (all entries)", fg=config.CYAN)

        # Connect to FlashArray + read snapshot metadata tags.
        config.write_host(
            f"  Connecting to FlashArray gateway {config.CFG.FaEndpoint} ...", fg=config.CYAN
        )
        fa = config.connect_fa()
        ctx_names = config.resolve_fa_context_names(fa, config.CFG.ProtectionGroupName)
        snap_tags = config.get_fa_snapshot_tags(
            fa, ctx_names, f"{config.CFG.ProtectionGroupName}.{snapshot_tag}"
        )
        config.write_host("  FlashArray connected.", fg=config.GREEN)

        # Discover the replay targets (the "shards"). Sharded: listShards via mongos. Replica set
        # (TOPOLOGY=replicaset): the single RS from the OM cluster detail — no mongos. The OM replicaSet
        # id is the segment-dir name the tailer wrote under, and it equals the replSetName used in the
        # mongorestore --host connection string.
        if config.CFG.Topology == "replicaset":
            cd = config.invoke_om_api(
                path=f"group/{config.CFG.GroupId}/clusters/{config.CFG.ClusterId}"
            )
            rsets = cd.get("replicaSets") or []
            if not rsets:
                raise RuntimeError(
                    "OM cluster detail returned no replicaSets for the replica-set deployment."
                )
            rs0 = rsets[0]
            rs_id = rs0.get("id")
            members = [n.get("id") for n in (rs0.get("nodes") or []) if n.get("id")]
            if not rs_id or not members:
                raise RuntimeError(
                    f"Could not resolve replica-set id/members from OM (id={rs_id}, members={members})."
                )
            shards = [{"shardId": rs_id, "rsHosts": f"{rs_id}/{','.join(members)}", "host": members[0]}]
        else:
            shard_json = config.invoke_mongosh_js(
                ssh_target=config.CFG.MongosHost,
                uri=f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort}",
                js=config.LIST_SHARDS_JS,
                context="listShards via mongos",
            )
            shards = json.loads(shard_json)

        config.write_host("  Replay targets (shard/RS -> host):", fg=config.CYAN)
        for s in shards:
            config.write_host(f"    {s['shardId']} -> {s['host']}", fg=WHITE)

        # Warm up each shard's routing cache via mongos before per-shard replay (sharded only — a
        # replica set has no mongos router to warm).
        if config.CFG.Topology != "replicaset":
            config.write_host("\n  Warming up shard routing cache via mongos...", fg=config.CYAN)
            warmup_script = (
                "var dbs=db.adminCommand({listDatabases:1}).databases; "
                "for(var i=0;i<dbs.length;i++){try{db.getSiblingDB(dbs[i].name).getCollectionNames();}catch(e){}} "
                "print('routing-cache-warmed');"
            )
            # Best-effort (non-fatal) warm-up.
            try:
                warmup_out = config.invoke_mongosh_js(
                    ssh_target=config.CFG.MongosHost,
                    uri=f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort}",
                    js=warmup_script,
                    context="routing-cache warm-up",
                )
                if re.search("routing-cache-warmed", warmup_out):
                    config.write_host("  Routing cache warm-up complete", fg=config.GREEN)
                else:
                    config.write_host(
                        f"  WARNING: routing cache warm-up returned unexpected output: {warmup_out}",
                        fg=config.YELLOW,
                    )
            except Exception as e:  # noqa: BLE001
                config.write_host(
                    f"  WARNING: routing cache warm-up failed (non-fatal): {e}", fg=config.YELLOW
                )

        errors: list[str] = []

        # Deploy the .oplogs decoder script to every replay node.
        # decode_oplogs.py lives in the same directory as this module.
        decoder_src = Path(__file__).with_name("decode_oplogs.py")
        if not decoder_src.exists():
            raise RuntimeError(
                f"decode_oplogs.py not found at {decoder_src} — it must be in the same directory as this script."
            )
        remote_decoder = "/tmp/decode_oplogs.py"

        # Load T1 atClusterTime from snapshot tag for pre-snapshot segment filtering.
        t1_at_cluster_time = 0
        if snap_tags.get("mongo:t1ts"):
            t1_at_cluster_time = int(snap_tags["mongo:t1ts"])
            config.write_host(
                f"  T1 atClusterTime (segment filter): {t1_at_cluster_time}", fg=config.CYAN
            )
        else:
            config.write_host(
                "  WARNING: mongo:t1ts tag absent — pre-T1 segment filtering disabled",
                fg=config.YELLOW,
            )

        # --- PITR floor guard: a PIT target below the snapshot's on-disk state is unreachable. ---
        # The volume snapshot restores to ~its creation instant (the per-shard mongo:floor tag, read
        # right after the FA snapshot fired) and replay only rolls FORWARD — so a target in the
        # [anchor, floor) dead zone would silently leave MORE data than requested and report success
        # (an undone drop can come back). Refuse it before touching anything.
        floors = window.floors_from_tag(snap_tags.get("mongo:floor"))
        if target_timestamp > 0:
            if floors:
                violations = window.floor_violations(floors, target_timestamp)
                if violations:
                    lines = "\n".join(
                        f"    {shard}: floor={fl[0]}:{fl[1]} "
                        f"({datetime.fromtimestamp(fl[0], tz=timezone.utc).isoformat().replace('+00:00', 'Z')})"
                        for shard, fl in sorted(violations.items())
                    )
                    msg = (
                        f"PIT target {target_timestamp} is BELOW the snapshot's on-disk floor for "
                        f"{len(violations)} shard(s):\n{lines}\n  A restore lands at the snapshot's "
                        "on-disk state and replay only rolls forward — this target cannot be reached; "
                        "the cluster would silently hold MORE data than the target implies. Use an "
                        "EARLIER snapshot whose floor is at/below the target, or raise the target."
                    )
                    if not allow_floor_override:
                        raise RuntimeError(
                            msg + "\n  (--allow-floor-override replays anyway — unsafe.)"
                        )
                    config.write_host(
                        f"  WARNING: {msg}\n  Proceeding due to --allow-floor-override.",
                        fg=config.YELLOW,
                    )
                else:
                    config.write_host(
                        f"  Floor check: target {target_timestamp} is at/above every shard's on-disk floor.",
                        fg=config.GREEN,
                    )
            else:
                config.write_host(
                    "  WARNING: mongo:floor tag absent (pre-floor snapshot) — cannot verify the target "
                    "sits at/above the snapshot's on-disk state; a below-floor target silently "
                    "over-restores.",
                    fg=config.YELLOW,
                )

        # --- Gap markers, scoped to the (T1, target] replay window. ---
        # The tailer writes gap-<ts>.json whenever oplog continuity (previousEnd) breaks. Only a gap
        # intersecting the window being replayed makes the PIT unrecoverable; one already inside the
        # snapshot (at/before T1) or beyond the target is irrelevant.
        gap_files = sorted(oplog_dir.glob("gap-*.json"))
        relevant_gaps: list[str] = []
        for g in gap_files:
            try:
                gd = json.loads(g.read_text())
            except Exception:  # noqa: BLE001 - unreadable marker: treat as relevant (conservative)
                gd = {}
            if window.gap_is_relevant(gd, t1_at_cluster_time, target_timestamp):
                relevant_gaps.append(
                    f"    {g.name}: storedLastEnd={gd.get('storedLastEnd')} omPreviousEnd={gd.get('omPreviousEnd')}"
                )
        if gap_files and not relevant_gaps:
            config.write_host(
                f"  {len(gap_files)} gap marker(s) present but none intersect the replay window — ignored.",
                fg=config.DARK_GRAY,
            )
        if relevant_gaps:
            msg = (
                f"{len(relevant_gaps)} oplog gap marker(s) intersect the replay window — PIT coverage is "
                "incomplete; replay to a point inside/after a gap is unrecoverable:\n"
                + "\n".join(relevant_gaps)
            )
            if not allow_gaps:
                raise RuntimeError(
                    msg
                    + "\n  Re-run with --allow-gaps to replay anyway (only data up to the first gap is trustworthy)."
                )
            config.write_host(f"  WARNING: {msg}\n  Proceeding due to --allow-gaps.", fg=config.YELLOW)

        # Build the per-shard work list (a flat list of work units) + per-shard segment inventories
        # for the pre-replay window validation below.
        plan: list[dict] = []
        problems: list[str] = []
        for s in shards:
            shard_id = s["shardId"]
            # Segments are written under the canonical shard id. Older streams (and the tailer's
            # best-effort fallback when mongos is unreachable) may key the dir by replica-set id instead
            # — e.g. the embedded config shard's rsId "aen-shard_0" vs its shard id "config" — so fall
            # back to the rsId dir if the shard-id dir is absent.
            rs_id = (s.get("rsHosts") or "").split("/")[0]
            seg_dir = oplog_dir / shard_id / "segments"
            if not seg_dir.exists() and rs_id and (oplog_dir / rs_id / "segments").exists():
                seg_dir = oplog_dir / rs_id / "segments"
            segments = window.list_segments(seg_dir)
            if not segments:
                # No captured stream for this shard at all. For a bounded PIT target that is a
                # coverage failure (the target provably can't be reached on this shard); for a
                # replay-all it stays a warn-and-skip (a shard with nothing to replay is a no-op).
                if target_timestamp > 0:
                    problems.append(
                        f"{shard_id}: no captured segments (looked in {shard_id}/ and {rs_id}/) — the "
                        f"target {target_timestamp} cannot be reached on this shard"
                    )
                else:
                    config.write_host(
                        f"  WARNING: no segments found for {shard_id} (looked in {shard_id}/ and {rs_id}/) - skipping shard",
                        fg=config.YELLOW,
                    )
                continue
            # Window-scoped completeness for this shard: anchor hole, interior hole, reach-the-target.
            problems.extend(
                window.validate_shard_window(shard_id, segments, t1_at_cluster_time, target_timestamp)
            )
            for start_ts, end_ts, seg_name in window.window_segments(
                segments, t1_at_cluster_time, target_timestamp
            ):
                plan.append(
                    {
                        "ShardId": shard_id,
                        "RsHosts": s["rsHosts"],
                        "Node": s["host"].split(":")[0],
                        "LocalPath": str(seg_dir / seg_name),
                        "SegLabel": seg_name[: -len(".oplogs")],
                        "StartTs": start_ts,
                        "EndTs": end_ts,
                    }
                )

        # --- All-or-nothing gate: every shard's window must validate BEFORE any shard is replayed. ---
        # Replaying shard-by-shard and discovering shard N's bad window mid-run would leave shards
        # 1..N-1 already rolled forward — a silent cross-shard inconsistency. Refuse up front instead.
        if problems:
            joined = "\n".join(f"    {p}" for p in problems)
            msg = (
                f"pre-replay window validation failed for {len(problems)} issue(s):\n{joined}\n"
                "  NO shard has been replayed; the cluster is still at the restore baseline."
            )
            if not allow_gaps:
                raise RuntimeError(
                    msg + "\n  Re-run with --allow-gaps to replay anyway (unsafe: coverage is incomplete)."
                )
            config.write_host(f"  WARNING: {msg}\n  Proceeding due to --allow-gaps.", fg=config.YELLOW)
        elif plan:
            config.write_host(
                "  Pre-replay validation: every shard's window is contiguous and reaches the target.",
                fg=config.GREEN,
            )

        # Plan summary.
        if len(plan) == 0:
            config.write_host(
                "\n  No post-T1 oplog segments to replay (all available segments pre-date the T1 snapshot). Cluster remains at T1 restore state.",
                fg=config.YELLOW,
            )
        else:
            unique_shards = len({u["ShardId"] for u in plan})
            config.write_host(
                f"\n  Replay plan: {len(plan)} segment(s) across {unique_shards} shard(s)",
                fg=config.CYAN,
            )

        # SCP the decoder to each distinct agent node once before the replay loop.
        deployed_nodes: set[str] = set()
        for unit in plan:
            if unit["Node"] not in deployed_nodes:
                deployed_nodes.add(unit["Node"])
                config.write_host(f"  Deploying decoder to {unit['Node']}...", fg=config.CYAN)
                proc = subprocess.run(
                    [
                        "scp",
                        *config.SSH_OPTS,
                        str(decoder_src),
                        f"{config.CFG.SshUser}@{unit['Node']}:{remote_decoder}",
                    ],
                    capture_output=True,
                    text=True,
                )
                if proc.returncode != 0:
                    raise RuntimeError(f"Failed to SCP decode_oplogs.py to {unit['Node']}")

        # Replay loop.
        prev_shard_id = None
        for unit in plan:
            if unit["ShardId"] != prev_shard_id:
                config.write_host(
                    f"\n  {unit['ShardId']}: replaying on {unit['RsHosts']} ...", fg=config.CYAN
                )
                prev_shard_id = unit["ShardId"]

            remote_file = (
                f"/tmp/oplog-replay-{snapshot_tag}-{unit['ShardId']}-{unit['SegLabel']}.oplogs"
            )
            num_bytes = Path(unit["LocalPath"]).stat().st_size
            # --oplogLimit applied only when file extends beyond the PIT target.
            apply_limit = target_timestamp > 0 and unit["EndTs"] > target_timestamp
            oplog_limit = f"--oplogLimit '{target_timestamp}:1'" if apply_limit else ""

            kb = num_bytes / 1024
            config.write_host(
                f"    seg {unit['SegLabel']}  {kb:,.1f} KB -> {unit['Node']}", fg=DARK_CYAN
            )
            proc = subprocess.run(
                [
                    "scp",
                    *config.SSH_OPTS,
                    unit["LocalPath"],
                    f"{config.CFG.SshUser}@{unit['Node']}:{remote_file}",
                ],
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                # Stop at the FIRST failure: continuing would apply this shard's LATER segments over a
                # missing window — a silent hole in the applied history. Stopping keeps the state
                # honest: shards/segments before this point are applied, nothing after it is.
                errors.append(f"{unit['ShardId']}/{unit['SegLabel']}: scp to {unit['Node']} failed")
                break

            # Build the remote replay command.
            replay_cmd = f"""set +e
TMPDIR=$(mktemp -d)
python3 {remote_decoder} {remote_file} > $TMPDIR/oplog.bson
if [ $? -ne 0 ]; then rm -rf $TMPDIR {remote_file}; exit 1; fi
{config.CFG.MongorestorePath} --host '{unit['RsHosts']}' --oplogReplay $TMPDIR {oplog_limit} 2>&1
EC=$?
rm -rf $TMPDIR {remote_file}
exit $EC
"""
            try:
                replay_proc = subprocess.run(
                    [
                        "ssh",
                        *config.SSH_OPTS,
                        f"{config.CFG.SshUser}@{unit['Node']}",
                        replay_cmd,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=replay_timeout_sec,
                )
            except subprocess.TimeoutExpired:
                # Honest timeout: killing the local ssh does NOT kill the remote mongorestore — it is
                # very likely STILL APPLYING on the node. Stop here (later segments must not apply over
                # an unknown state) and say exactly that.
                errors.append(
                    f"{unit['ShardId']}/{unit['SegLabel']}: mongorestore exceeded "
                    f"--replay-timeout-sec={replay_timeout_sec} on {unit['Node']}. The REMOTE "
                    "mongorestore was NOT killed and may still be applying — verify it finished "
                    "(pgrep mongorestore on the node) before re-running; later segments were NOT replayed."
                )
                break
            restore_exit = replay_proc.returncode
            # Merge stderr into the captured output stream.
            out = (replay_proc.stdout or "") + (replay_proc.stderr or "")
            config.write_host(out, fg=GRAY)
            if restore_exit == 0:
                config.write_host(f"    seg {unit['SegLabel']}: OK", fg=config.GREEN)
            else:
                errors.append(
                    f"{unit['ShardId']}/{unit['SegLabel']}: mongorestore exit {restore_exit} on {unit['Node']}"
                )
                config.write_host(
                    f"    seg {unit['SegLabel']}: mongorestore exit {restore_exit}",
                    fg=config.YELLOW,
                )
                # Stop at the FIRST failure (see the scp-failure note above): later segments must not
                # apply over a missing window.
                break

        # Fail if a segment errored (the loop stopped there; nothing after it was applied).
        if len(errors) > 0:
            joined = "\n".join(errors)
            raise RuntimeError(
                f"Oplog replay stopped at the first failing segment:\n{joined}\n"
                "  Segments before it are applied; nothing after it was touched. Fix the cause and "
                "re-run the replay (oplog application is idempotent)."
            )

        # Post-replay verification: count docs as a sanity check.
        config.write_host("\n=== Post-Replay Verification ===", fg=config.YELLOW)
        load_test_count = config.invoke_mongosh_js(
            ssh_target=config.CFG.MongosHost,
            uri=f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort}",
            js=f'db.getSiblingDB("{verify_database}").loadtest.countDocuments()',
            context=f"post-replay count of {verify_database}.loadtest",
        )
        payload_count = config.invoke_mongosh_js(
            ssh_target=config.CFG.MongosHost,
            uri=f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort}",
            js=f'db.getSiblingDB("{verify_database}").payload.countDocuments()',
            context=f"post-replay count of {verify_database}.payload",
        )

        config.write_host(f"  {verify_database}.loadtest : {load_test_count.strip()}", fg=config.CYAN)
        config.write_host(f"  {verify_database}.payload  : {payload_count.strip()}", fg=config.CYAN)

        # Range-bound assertion setup.
        counts = {
            "loadtest": int(load_test_count.strip()),
            "payload": int(payload_count.strip()),
        }
        # Default t2_mark_path to <OplogDir>/t2-mark.json when not supplied.
        if not t2_mark_path:
            t2_mark_path = str(oplog_dir / "t2-mark.json")

        # Verification branches.
        if skip_verification:
            config.write_host("  Verification skipped (-SkipVerification)", fg=config.YELLOW)
        elif not snap_tags.get("mongo:preSnap"):
            config.write_host(
                "  mongo:preSnap tag absent — skipping range check (counts printed above)",
                fg=config.YELLOW,
            )
        elif not Path(t2_mark_path).exists():
            config.write_host(
                f"  T2 mark not found at {t2_mark_path} - skipping range check (provide -T2MarkPath or write the file to enable)",
                fg=config.YELLOW,
            )
        else:
            # Parse preSnap tag JSON and t2-mark.json.
            pre_snap = json.loads(snap_tags["mongo:preSnap"])
            with open(t2_mark_path, "r", encoding="utf-8") as fh:
                t2_mark = json.loads(fh.read())
            pre_db = pre_snap.get(verify_database)
            t2_mark_counts = t2_mark.get("counts") if isinstance(t2_mark, dict) else None
            t2_db = t2_mark_counts.get(verify_database) if isinstance(t2_mark_counts, dict) else None
            # Skip if entries missing for verify_database.
            if pre_db is None or t2_db is None:
                config.write_host(
                    f"  WARNING: mongo:preSnap tag or t2-mark missing entries for '{verify_database}' - skipping range check",
                    fg=config.YELLOW,
                )
            else:
                # Per-collection range check.
                mismatches: list[str] = []
                for coll in counts.keys():
                    got = counts[coll]
                    lo = int(pre_db[coll])
                    hi = int(t2_db[coll])
                    if got < lo or got > hi:
                        mismatches.append(f"{verify_database}.{coll}: {got} NOT IN [{lo}, {hi}]")
                        config.write_host(
                            f"  Baseline FAIL : {verify_database}.{coll} = {got} NOT IN [{lo}, {hi}]",
                            fg=config.RED,
                        )
                    else:
                        tail = hi - got
                        config.write_host(
                            f"  Baseline OK   : {verify_database}.{coll} = {got} in [{lo}, {hi}] (unrecoveredTail={tail})",
                            fg=config.GREEN,
                        )
                # Raise if any mismatch.
                if len(mismatches) > 0:
                    joined = "\n".join(mismatches)
                    raise RuntimeError(f"Post-replay verification failed:\n{joined}")

        config.write_host("\n=== Oplog Replay Complete ===", fg=config.GREEN)

    finally:
        try:
            sys.stdout = orig_stdout
            sys.stderr = orig_stderr
            log_handle.close()
        except Exception:
            pass


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
