###############################################################################################################################
# Preflight - read-only readiness gate for snapshot / restore / PITR.
#
# Aggregates every check that has bitten us in the field into one non-destructive command, so an operator
# (or agent) can validate a deployment BEFORE a snapshot hangs PENDING or a restore aborts halfway:
#
#   1. Ops Manager reachable + the cluster REGISTERED for third-party backup (oplogType=thirdParty per RS)
#   2. No wedged in-flight snapshot/oplog job
#   3. Every replica set has a snapshotable member (with a TRANSIENT-vs-STRUCTURAL verdict on failure)
#   4. preferredOplogNodes entries all exist in the live topology (a stale entry - typically left by a
#      removed shard - makes the $backupCursor never open for the WHOLE cluster: snapshots hang PENDING)
#   5. No featureCompatibilityVersion skew across mongods (silent add-shard stall)
#   6. Automation agent active on every node (SSH)
#   7. No symlink under the data mount escaping to a volume outside it (a symlinked journal/oplog dir
#      would be silently missing from the snapshot -> crash-inconsistent restore)
#   8. Every tagged node volume is a member of the protection group
#
# Read-only: no OM writes, no FA writes, no node changes. Exit code 0 = no FAIL findings, 1 otherwise.
###############################################################################################################################
from __future__ import annotations

import subprocess
from datetime import datetime, timezone

import typer

from . import config

app = typer.Typer(add_completion=False)

_PASS, _WARN, _FAIL, _SKIP = "PASS", "WARN", "FAIL", "SKIP"
_COLORS = {_PASS: config.GREEN, _WARN: config.YELLOW, _FAIL: config.RED, _SKIP: config.DARK_GRAY}


def _run(
    deployment: str = typer.Option(
        None,
        "--deployment",
        help="Deployment name to check (selects '<NAME>__' keys in .env). Omit to use the flat keys.",
    ),
    skip_ssh: bool = typer.Option(
        False,
        "--skip-ssh",
        help="Skip the per-node SSH checks (agent health, symlink guard) - API-only preflight.",
    ),
) -> None:
    config.load_config(deployment=deployment)
    cfg = config.CFG
    results: list[tuple[str, str, str]] = []

    def add(status: str, check: str, detail: str = "") -> None:
        results.append((status, check, detail))
        config.write_host(f"  [{status:4}] {check}" + (f" - {detail}" if detail else ""),
                          fg=_COLORS[status])

    config.write_host(f"\n=== Preflight: {cfg.ClusterName} ({cfg.Topology}) ===", fg=config.YELLOW)

    # --- 1. OM reachable + third-party registration -------------------------------------------------
    detail = None
    try:
        detail = config.invoke_om_api(path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}")
        non_tp = [rs.get("id") for rs in (detail.get("replicaSets") or [])
                  if rs.get("oplogType") != "thirdParty"]
        if non_tp:
            add(_FAIL, "third-party registration",
                f"replica set(s) not in thirdParty oplog mode: {', '.join(map(str, non_tp))}")
        else:
            add(_PASS, "third-party registration",
                f"{len(detail.get('replicaSets') or [])} replica set(s), all thirdParty")
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if "404" in msg:
            add(_FAIL, "third-party registration",
                f"cluster {cfg.ClusterId} is NOT registered for third-party backup "
                "(enable with POST .../backup/third_party/group/{g}/clusters/{id}/manage)")
        else:
            add(_FAIL, "Ops Manager reachable", msg[:200])

    # --- 2. In-flight jobs ---------------------------------------------------------------------------
    if detail:
        for kind, key in (("snapshot", "snapshotId"), ("oplogSnapshot", "oplogSnapshotId")):
            jid = detail.get(key)
            if not jid:
                add(_PASS, f"in-flight {kind} job", "none on record")
                continue
            try:
                state = config.invoke_om_api(
                    path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/{kind}/{jid}"
                ).get("state")
            except Exception:  # noqa: BLE001
                state = "unreadable"
            if state in ("FINISHED", "FAILED"):
                add(_PASS, f"in-flight {kind} job", f"last={jid} state={state}")
            elif state == "READY" and kind == "snapshot":
                add(_WARN, f"in-flight {kind} job",
                    f"{jid} is READY (cursor open) - new-mongo-snapshot will auto-/finish it")
            else:
                add(_FAIL, f"in-flight {kind} job",
                    f"{jid} state={state} - blocks new jobs (PENDING: wait for OM's timeout reclaim; "
                    "never /fail a PENDING job)")

    # --- 3. Snapshotable members (verdict on failure) ------------------------------------------------
    if detail:
        freshness: dict = {}
        try:
            res = config.invoke_om_api(path=f"groups/{cfg.GroupId}/hosts?itemsPerPage=500",
                                       path_prefix="")
            now_utc = datetime.now(timezone.utc)
            for h in res.get("results") or []:
                if h.get("lastPing"):
                    try:
                        ts = datetime.fromisoformat(str(h["lastPing"]).replace("Z", "+00:00"))
                        freshness[f"{h.get('hostname')}:{h.get('port')}"] = (now_utc - ts).total_seconds()
                    except Exception:  # noqa: BLE001
                        pass
        except Exception:  # noqa: BLE001
            pass
        bad = 0
        for rs in detail.get("replicaSets") or []:
            nodes = rs.get("nodes") or []
            if any(n.get("snapshotable") is True for n in nodes):
                continue
            bad += 1
            fresh = [n.get("id") for n in nodes if freshness.get(n.get("id"), 1e9) < 120]
            verdict = ("TRANSIENT backup-view lag (fresh in monitoring; self-heals - or "
                       "new-mongo-snapshot --snapshotable-wait)") if fresh else (
                       "STRUCTURAL (stale/missing in monitoring too - check the agent and "
                       "backupVersions/monitoringVersions)")
            add(_FAIL, f"snapshotable: {rs.get('id')}", f"no snapshotable member - {verdict}")
        if bad == 0:
            add(_PASS, "snapshotable members", "every replica set has at least one")

    # --- 4. Stale preferredOplogNodes ----------------------------------------------------------------
    if detail:
        topo_ids = {n.get("id") for rs in (detail.get("replicaSets") or [])
                    for n in (rs.get("nodes") or [])}
        stale = [p for p in (detail.get("preferredOplogNodes") or []) if p not in topo_ids]
        if stale:
            add(_FAIL, "preferredOplogNodes",
                f"stale entr{'ies' if len(stale) > 1 else 'y'} not in the live topology: "
                f"{', '.join(stale)} - the $backupCursor will never open for the WHOLE cluster "
                "(snapshots hang PENDING). Re-register via start-oplog-tailer or POST preferredOplogNodes.")
        else:
            add(_PASS, "preferredOplogNodes",
                f"{len(detail.get('preferredOplogNodes') or [])} entr(ies), all in topology")

    # --- 5. FCV skew ----------------------------------------------------------------------------------
    try:
        ac = config.invoke_om_api(path=f"groups/{cfg.GroupId}/automationConfig", path_prefix="")
        fcvs = {p.get("featureCompatibilityVersion") for p in (ac.get("processes") or [])
                if p.get("processType") == "mongod" and p.get("featureCompatibilityVersion")}
        if len(fcvs) > 1:
            add(_FAIL, "FCV skew", f"mongods run different featureCompatibilityVersions: {sorted(fcvs)} "
                "- add-shard stalls silently on WaitFeatureCompatibilityVersionCorrect; align with "
                "setFeatureCompatibilityVersion")
        elif fcvs:
            add(_PASS, "FCV skew", f"uniform FCV {next(iter(fcvs))}")
        else:
            add(_SKIP, "FCV skew", "no mongod FCV visible in automationConfig")
    except Exception as e:  # noqa: BLE001
        add(_WARN, "FCV skew", f"automationConfig unreadable: {str(e)[:120]}")

    # --- 6 + 7. Per-node SSH checks -------------------------------------------------------------------
    if skip_ssh:
        add(_SKIP, "agent health", "--skip-ssh")
        add(_SKIP, "symlink guard", "--skip-ssh")
    else:
        try:
            nodes = config.get_cluster_nodes()
        except Exception as e:  # noqa: BLE001
            nodes = []
            add(_WARN, "node discovery", f"{str(e)[:120]} - skipping SSH checks")
        agent_bad, link_bad = [], []
        for node in nodes:
            proc = subprocess.run(
                ["ssh", *config.SSH_OPTS, f"{cfg.SshUser}@{node}",
                 "systemctl is-active mongodb-mms-automation-agent"],
                capture_output=True, text=True,
            )
            state = (proc.stdout or "").strip() or "unreachable"
            if state != "active":
                agent_bad.append(f"{node}={state}")
            # Symlink escape guard: a dir under the data mount symlinked to another volume (journal!)
            # would be silently missing from the snapshot -> crash-inconsistent restore.
            mnt = config.data_mount()
            link_proc = subprocess.run(
                ["ssh", *config.SSH_OPTS, f"{cfg.SshUser}@{node}",
                 f"find {mnt} -maxdepth 4 -type l -exec readlink -f {{}} \\; 2>/dev/null | head -20"],
                capture_output=True, text=True,
            )
            for target in (link_proc.stdout or "").splitlines():
                target = target.strip()
                if target and not target.startswith(mnt):
                    link_bad.append(f"{node}: link escapes to {target}")
        if nodes:
            add(_FAIL if agent_bad else _PASS, "agent health",
                "; ".join(agent_bad) if agent_bad else f"active on all {len(nodes)} node(s)")
            add(_FAIL if link_bad else _PASS, "symlink guard",
                "; ".join(link_bad[:5]) if link_bad
                else f"no symlink under {config.data_mount()} escapes the data volume")

    # --- 8. PG membership ------------------------------------------------------------------------------
    try:
        fa = config.connect_fa()
        ctx_names = config.resolve_fa_context_names(fa, cfg.ProtectionGroupName)
        tag_map = config.read_volume_map_tags(fa, deployment, ctx_names) or {}
        tagged = {(v["ShortName"], v["VolumeName"]) for vols in tag_map.values() for v in vols}
        members: set = set()
        for ctx in ctx_names:
            items = config._fa(fa.get_protection_groups_volumes(
                context_names=[ctx], group_names=[cfg.ProtectionGroupName]))
            for it in items or []:
                m = getattr(it, "member", None)
                name = (m.get("name") if isinstance(m, dict) else getattr(m, "name", None)) if m else None
                if name:
                    members.add((ctx, name.split("::")[-1]))
        missing = sorted(t for t in tagged if t not in members)
        if not tagged:
            add(_WARN, "PG membership",
                "no mongo: volume-map tags found - run initialize-protection-groups")
        elif missing:
            add(_FAIL, "PG membership",
                f"tagged volume(s) NOT in {cfg.ProtectionGroupName}: "
                + ", ".join(f"{a}:{v}" for a, v in missing[:5])
                + " - run initialize-protection-groups")
        else:
            add(_PASS, "PG membership",
                f"all {len(tagged)} tagged volume(s) are members of {cfg.ProtectionGroupName}")
    except Exception as e:  # noqa: BLE001
        add(_WARN, "PG membership", f"FlashArray check unavailable: {str(e)[:150]}")

    # --- Summary ---------------------------------------------------------------------------------------
    fails = [r for r in results if r[0] == _FAIL]
    warns = [r for r in results if r[0] == _WARN]
    config.write_host(
        f"\n=== Preflight complete: {len(results)} checks - "
        f"{len(fails)} FAIL, {len(warns)} WARN ===",
        fg=config.RED if fails else config.GREEN,
    )
    if fails:
        raise typer.Exit(code=1)


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
