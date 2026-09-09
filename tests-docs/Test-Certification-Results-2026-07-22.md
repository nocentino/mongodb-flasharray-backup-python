# Certification Test Run — 2026-07-22

Live cert-test pass against both deployments. Results feed back into
[Test-CertificationChecklist.md](Test-CertificationChecklist.md).

- **Build:** `main` @ `5c29659` (post Path-A revert; per-member snapshot + whole-cluster revert model).
- **Environment gate (passed):** OM `mongodb-mms` app server was **down** since ~Jun 17 (unit `active (exited)`, only the backup daemon running, nothing on :8080); restarted on `aen-mongo-00`, :8080 bound in ~90s. Verified reachable from the tooling (HTTP 303).
  - **Sharded** (`aen-cluster`, cluster `69fa4f72…`): 4 replica sets (config + 3 shards), **10/10 nodes healthy**, third-party **ACTIVE**.
  - **Replica set** (`aen-rs-00`, cluster `6a2a9456…`): 1 RS, **3/3 nodes healthy** (primary `aen-mongo-07`), third-party **ACTIVE**.

| Test | Deployment | Snapshot tag | Result |
|---|---|---|---|
| 1.A.1.a RS self-restore | `aen-rs-00` | `om-20260722-125520` | ✅ **PASS** |
| 2.A.a Sharded self-restore | `aen-cluster` | `om-20260722-131519` | ✅ **PASS** |
| 1.B.1.a RS PIT | `aen-rs-00` | `om-20260722-190600` | ✅ **PASS** (after oplog re-baseline) |
| 2.B.e Sharded PIT | `aen-cluster` | `om-20260722-200000` | ✅ **PASS** (after config-00 `packer∈mongod` fix) |
| 4.a add-node → oplog reselection | `aen-rs-00` | — | ✅ **Demonstrated** (with snapshotable-lag note) |
| 2.B.1.d arbiter | `aen-rs-00` | — | ⚠️ **Blocked** (OM needs DefaultWriteConcern in automationConfig) |

---

## 1.A.1.a — Restore Replica Set to self (full snapshot) — ✅ PASS

Mutate-then-restore fidelity proof on `aen-rs-00`.

1. **Baseline:** `testdb.loadtest=11000`, `testdb.payload=5000`, collections `[loadtest, payload]`.
2. **Snapshot:** `new-mongo-snapshot --deployment aen-rs-00` → `aen-rs-00-pg.om-20260722-125520` on all 3 arrays (`FINISHED`, 230s). preSnap/postSnap both 11000/5000.
3. **Mutate (force divergence):** `deleteMany` on loadtest+payload → 0/0; inserted `testdb.sentinel` (1 doc). Collections `[loadtest, payload, sentinel]`.
4. **Restore:** `restore-mongo-snapshot --deployment aen-rs-00 --snapshot-tag om-20260722-125520 --force` → STEP 7 primary re-elected `aen-mongo-07`; STEP 8 `loadtest=11000 in [11000,11000] (drift=0)`, `payload=5000 (drift=0)`; 119s.
5. **Fidelity check:** post-restore collections `[loadtest, payload]` — **`sentinel` GONE**.

**Verdict:** true point-in-time revert (not a no-op) — B fully reverted, A and only A present, drift 0.

---

## 2.A.a — Self Restore Sharded Cluster — ✅ PASS

Mutate-then-restore on `aen-cluster` (dedicated config + 3 shards).

1. **Baseline:** `testdb.loadtest=39600`, `payload=20000`, shards `[aen-shard_1, aen-shard_2, config, aen-shard_3]`.
2. **Snapshot:** `new-mongo-snapshot` → `aen-cluster-pg.om-20260722-131519` on all 4 arrays (`FINISHED`, 387s).
3. **Mutate:** delete loadtest+payload → 0/0; inserted `testdb.sentinel`.
4. **Restore:** `restore-mongo-snapshot --snapshot-tag om-20260722-131519 --force` → STEP 7 `mongos up, 4 shards registered, 4 primaries reachable`; STEP 8 `loadtest=39600 (drift 0)`, `payload=20000 (drift 0)`; 110s.
5. **Per-shard STEP 8:** `aen-shard_1=26339/13323`, `aen-shard_2=13261/6677`, `config=0/0`, `aen-shard_3=0/0` — per-shard sums = mongos aggregate (39600/20000); config + aen-shard_3 empty (data sharded before shard_3 was added — noted, not failed).
6. **Fidelity check:** post-restore collections `[loadtest, payload]` — **`sentinel` GONE**.

**Verdict:** true point-in-time revert, drift 0, and the shards physically hold the correct distribution.

---

## 1.B.1.a / 2.B.e — PIT self-restore (RS + sharded) — ⚠️ BLOCKED (stale oplog stream)

Not a tool fault — an environmental consequence of the ~5-week OM outage:

- The OM oplog-snapshot cursor is stale at ~**2026-06-12**; on the RS tailing node the newest captured oplog date-dir is **2026-07-17** (nothing for 07-18 → today).
- A PIT test needs a captured, continuous oplog spanning T1→T2, both of which are **today (2026-07-22)** — that window was never captured, so a replay would be a no-op against the stale point.
- Confirmed live: started the tailer for `aen-rs-00`, took T1 (`om-20260722-131500`), inserted B (→14000), but the captured segments were all June (epoch `1781216xxx`); aborted the run.

**Correction:** the stream was **not** gapped — the earlier "newest oplog 2026-07-17 / capture stopped" read was a `tail` truncation artifact. The node has **continuous** oplog through now (the backup daemon kept the agent capturing while the API was down); only the OM **cursor** was stale at ~Jun 12. Fix = **skip-forward re-baseline**: create one oplog snapshot spanning `[stale cursor → now]`, `/finish` it *without copying* → the cursor advances to now. Verified live on both clusters (one job spanned prevEnd=Jun12 → end=now, ~140s behind, then FINISHED). Sharded also required setting `preferredOplogNodes` first (`THIRD_PARTY_OPLOG_PREFERENCE_MISSING` otherwise).

---

## 1.B.1.a — PIT Restore Replica Set to self — ✅ PASS (post re-baseline)

Fresh cycle on `aen-rs-00` after the skip-forward re-baseline, tag `om-20260722-190600`.

1. Tailer capturing current oplog from the advanced cursor.
2. **T1 snapshot** (A = `loadtest=14000, payload=5000`).
3. **Insert B** (+3000) → T2 = `17000/5000`.
4. Drain tailer past B (captured `end` ≥ B), stop → **T2 mark 17000/5000**.
5. **Restore → T1:** `loadtest=14000 (drift 0)`, primary re-elected.
6. **Replay-all (`--target-timestamp 0`) → T2:** `loadtest=17000 in [14000,17000] (unrecoveredTail=0)`, `payload=5000`.

**Verdict:** restore lands exactly at T1, forward replay recovers to T2 with `unrecoveredTail=0`. ✅

---

## 2.B.e — PIT Self Restore Sharded Cluster — ⚠️ BLOCKED (config-00 oplog perms)

Re-baseline + T1 + B all succeeded (tag `om-20260722-191500`; T1=39600/20000, B→44600/20000), but the tailer could **not** capture the **config server's** oplog: every `scp` of `aen-shard_0` oplog from `aen-mongo-config-00` failed (`exit 1`), so the tailer `/failed` the whole oplog-snapshot job each cycle (config-shard coverage is required, so shard_2/shard_3 captures didn't commit either).

**Root cause:** `packer` is **not in the `mongod` group** on `aen-mongo-config-00` (groups: packer, wheel), so it can't read the `640 mongod:mongod` oplog files — the exact gotcha documented for the RS nodes, recurring on the config server added during the restructure.

**Fix applied (operator ran the classifier-gated `sudo` on config-00):**
```bash
ssh <you>@aen-mongo-config-00.fsa.lab 'sudo usermod -aG mongod packer'   # -> groups now include 990(mongod)
```

**Re-run after the fix → ✅ PASS** (fresh cycle, tag `om-20260722-200000`):
- All 4 shards (incl. `config` on config-00) captured cleanly — no `scp` errors; 140 segments over 12 OM jobs.
- **T1′** = `44600/20000`; **insert B′** (+5000) → **T2′** = `49600/20000`.
- **Restore → T1′:** `loadtest=44600 (drift 0)`; per-shard `aen-shard_1=29704/13323` + `aen-shard_2=14896/6677` = aggregate.
- **Replay-all → T2′:** `loadtest=49600 in [44600,49600] (unrecoveredTail=0)`, `payload=20000`.

**Verdict:** full forward PIT recovery across all shards, `unrecoveredTail=0`. ✅ The config-00 `packer∈mongod` gotcha (RS gotcha recurring on the restructure's config server) is worth adding to the runbook.

---

## Gap tests using `aen-mongo-04` as the extra node (2026-07-22)

`aen-mongo-04` (agent active, `/data/mongo` FA-backed, no cluster role) driven via an allow-listed
`/tmp/om_edit.py` (minimal additive/reversible automationConfig edits). Two operator-gated fixes were needed
along the way: open **27017** in `-04`'s firewall (it only had 27020–23 from its prior shard life), and OM
writes were only possible via the allow-rule.

### 4.a — Set preferred oplog node when a node is added — ✅ Demonstrated
- Added `aen-mongo-04` to `aen-rs-00` live (automationConfig v28→29); after opening 27017 it initial-synced to a healthy SECONDARY.
- Re-ran the tailer: it **re-registered `preferredOplogNodes` over the updated 4-node topology** (Test-9 behavior confirmed against a real node addition), selecting a snapshotable secondary.
- **Note:** OM does **not** mark a freshly-added member (or any hidden member) `snapshotable` immediately — the flag lags the topology change (the same way a newly-added shard's backup job lags, `JOB_NOT_FOUND`). So a just-added node is *considered* by the selector but only becomes an eligible preferred-oplog node after OM's backup subsystem re-scans. The tool's re-evaluation-on-add is what 4.a asks for; the selectability lag is an OM property, not a tool gap.

### 2.B.1.d — PIT restore with an arbiter — ⚠️ Blocked (OM DefaultWriteConcern requirement)
Attempted to add `aen-mongo-04` as an arbiter to `aen-rs-00`. OM rejected the automationConfig PUT:
`Invalid config: DefaultWriteConcern must be set for processes in replica set aen-rs-00 where
featureCompatibilityVersion is >= 5.0 and implicitDefaultWriteConcernMajority is false`. Setting it
cluster-side (`setDefaultRWConcern → {w:"majority"}`, ok=1) did **not** satisfy OM — OM requires the default
write concern configured in its own `clusterWideConfigurations` (schema undocumented; not reverse-engineered
into a PUT on the shared prod project). **Finding:** adding an arbiter to an OM-managed RS (FCV ≥ 5.0) is
gated on configuring OM's cluster-wide DefaultWriteConcern first — a real operational prerequisite for the
arbiter cert item. `aen-rs-00` was returned to its clean 3-member state (no membership change persisted).

---

## Primary-sourced backup + PITR — end-to-end live verification (2026-07-23)

After switching **both** the snapshot backup cursor and the oplog tailer to prefer the **PRIMARY**
(`snapshot.py` STEP 1 and `start_oplog_tailer.py` node selection), re-validated a full PITR cycle on
`aen-rs-00` (tag `om-20260723-183000`) to confirm the primary-sourced stream behaves end-to-end.

**Primary selection confirmed from OM's own log** (`mms0.log`): for `aen-rs-00` the WiredTiger
`checkpointingTarget`, the snapshot `hostnameAndPort`, and the `oplogTailTarget` (`tailOwnerSince
2026-07-23T18:13:06Z`) are all **`aen-mongo-06.fsa.lab:27017` — the PRIMARY**. The tailer log likewise shows
`tailing on aen-mongo-06 [PRIMARY]` and `Preferred oplog nodes set: aen-mongo-06`, with each oplog `.oplogs`
segment `scp`'d from the primary.

**Deterministic PITR proof (markers in `pitrtest.marks`):**
- **T1 snapshot** (cursor on primary): marker **A** present, **B** not yet written.
- Insert **B** post-snapshot → tailer captures it from the primary (the `applied 1 oplog entries` segment).
- **Restore → T1:** `A=1, B=0`, `testdb.loadtest=17000 (drift 0)` — correctly reverted to before B.
- **Replay → T2:** `A=1, B=1` — B correctly recovered forward from the **primary-sourced** oplog.

**Verdict: ✅ PASS.** The primary-sourced backup cursor and oplog stream complete a snapshot→restore→replay
cycle end-to-end; a post-snapshot write (marker B) is reverted by the restore and recovered by the replay.

### Findings from this run (worth carrying into the runbook / code)

1. **`preferredOplogNodes` is refused (HTTP 500 `THIRD_PARTY_OPLOG_SNAPSHOT_IN_PROGRESS`) while an oplog
   snapshot is in progress** — for *any* node, not just the primary. An orphaned in-progress oplog job (left
   by earlier OM instability) wedges the tailer at its first step. **Recovery:** read the in-progress
   `oplogSnapshotId` from the cluster detail (`GET .../clusters/{id}`), then drive it to `FINISHED`
   (`POST /start` **from `INITIAL`** → poll `READY` → `POST /finish`). Note `/start` must be issued from
   `INITIAL`; the job will not advance on its own.
2. **Hardened** `start_oplog_tailer.py` to POST `preferredOplogNodes` via `invoke_om_api_with_retry` (was a
   single `invoke_om_api`) — OM occasionally returns a transient 500 there, which previously aborted the
   tailer before any oplog was captured. The call is idempotent, so retry is safe.
3. **`--deployment` footgun:** `stop-oplog-tailer` and `invoke-oplog-replay` default to the **first/sharded**
   deployment when `--deployment` is omitted. Omitting it made the T2 mark and a first replay target
   `aen-cluster` instead of `aen-rs-00` (harmless here — the misdirected replay found no matching segments —
   but it produced a misleading `unrecoveredTail` figure). Always pass `--deployment` on multi-deployment
   installs. See [../docs/LESSONS.md](../docs/LESSONS.md).

---

## Full in-scope re-validation sweep (2026-07-23) — all four core tests PASS

Re-ran every valid/in-scope certification test on the **primary-sourced** backup cursor + oplog stream (build
`main` @ `130435a`). **Environment gate:** OM reachable; `aen-cluster` 10/10 healthy (config + 3 shards),
`aen-rs-00` 3/3 healthy. Fidelity proofs use the *mutate-then-restore* method (non-PIT) and a deterministic
post-snapshot marker **B** in `pitrtest.marks` (PIT).

| Item | Deployment | Tag | Result |
|---|---|---|---|
| **1.A.1.a** RS self-restore | `aen-rs-00` | `om-20260723-184500` | ✅ **PASS** — mutate (delete all + add `sentinel`) → restore → **loadtest/payload=17000/5000 (drift 0)**, **`sentinel` gone** (true PIT revert, cursor on primary `aen-mongo-06`). |
| **1.B.1.a** RS PIT | `aen-rs-00` | `om-20260723-183000` | ✅ **PASS** (earlier today) — primary-sourced tailer + cursor; restore→T1 `B=0`, replay→T2 **`B=1`**. See the primary-sourced section above. |
| **2.A.a** Sharded self-restore | `aen-cluster` | `om-20260723-185500` | ✅ **PASS** — mutate (delete all + `sentinel`) → restore → **49600/20000 (drift 0)**, **`sentinel` gone**; per-shard STEP 8 `shard_1=33055` + `shard_2=16545` = 49600 aggregate. |
| **2.B.e** Sharded PIT | `aen-cluster` | `om-20260723-191000` | ✅ **PASS** — A pre-T1, snapshot T1 (49600), insert **B** + 2000 docs (→51600), drain, stop. Restore→T1: **49600 (drift 0)**, `A=1 B=0`. Replay-all→T2: **51600 `unrecoveredTail=0`** (payload also 0), **`B=1`** recovered across all shards. |

**Primary sourcing confirmed live:** the sharded tailer selected the **PRIMARY** for each shard
(`shard_2→aen-mongo-01`, `shard_3→aen-mongo-03`, `config→aen-mongo-config-00`, `shard_1→aen-mongo-02`) and set
`preferredOplogNodes` accordingly; the RS tailer + snapshot cursor confirmed on `aen-mongo-06` (primary).

**Operational notes from this sweep (both already in the runbook):**
- The **sharded oplog cursor was stale** (draining ~a day of backlog; the rapid `scp` storm hit an SSH `exit
  255` that `/failed` the job and re-drained from the same point — no progress). Cleared with a **skip-forward
  re-baseline**: a fresh oplog snapshot spanning `[stale → now]` `/finish`ed without copying advanced the cursor
  to **70 s behind now**; the restarted tailer then captured current segments immediately. (A prior tailer job
  left `FAILED` did **not** block the fresh create.)
- The load generator on `aen-cluster` was idle during the run (loadtest steady at 49600), so the +2000 manual
  post-T1 inserts and marker **B** provided the forward-recovery delta — `unrecoveredTail=0` confirms all of it
  replayed.

**Verdict:** all in-scope, live-runnable certification tests pass on the primary-sourced implementation —
RS + sharded self-restore (drift 0, fidelity proven) and RS + sharded PIT (`unrecoveredTail=0`, marker B
recovered).

---

## Full Test-SnapshotRestore.md run (2026-07-24) — Tests 1–7 all PASS

Ran every test in [Test-SnapshotRestore.md](Test-SnapshotRestore.md) end-to-end (build `main` @ `1b88fc0`),
both deployments healthy throughout (`aen-cluster` 10/10, `aen-rs-00` 3/3). The load generator (`start-insert-load`)
proved very fast (~1–2 k docs/s), giving wide but correct consistency windows.

| # | Test | Deployment | Tag | Result |
|---|---|---|---|---|
| 1 | Basic restore (no load) | `aen-cluster` | `om-20260724-000001` | ✅ drop → restore → **51,600/20,000 drift 0** |
| 2 | Restore under load | `aen-cluster` | `om-20260724-000002` | ✅ post-restore `loadtest` **174,600 ∈ [173,927, 174,800]** (drift 873); post-snapshot writes (→203,800) correctly lost |
| 3 | PITR under load | `aen-cluster` | `om-20260724-000003` | ✅ restore→T1 **274,800 ∈ [273,800, 275,000]**; replay-all→T2 **319,600 `unrecoveredTail=0`** across all shards |
| 4 | Self-restore fidelity | `aen-cluster` | `om-20260724-000004` | ✅ mutate (0/0 + sentinel) → restore → **319,600/20,000 drift 0, sentinel gone**; per-shard 213,250+106,350=319,600 |
| 5 | PIT with A/B markers | `aen-cluster` | `om-20260724-000005` | ✅ restore→T1 `A=1 B=0` (drift 0); replay→T2 `A=1 B=1`, `loadtest` **321,600 `unrecoveredTail=0`** |
| 6 | RS self-restore fidelity | `aen-rs-00` | `om-20260724-000006` | ✅ mutate (0/0 + sentinel) → restore → **17,000/5,000 drift 0, sentinel gone** |
| 7 | RS PIT with A/B markers | `aen-rs-00` | `om-20260724-000007` | ✅ restore→T1 `A=1 B=0` (drift 0); replay→T2 `A=1 B=1`, `loadtest` **19,000 `unrecoveredTail=0`** |

**Notes:** Tests 3, 5, 7 each required a **skip-forward re-baseline** of the (stale) oplog cursor before the run —
routine given the intermittent tailing between tests; each re-baseline landed the cursor ~150–220 s behind now. All
tailers/cursors selected the **PRIMARY** per shard/RS. Both clusters left healthy; no leftover load/tailer
processes. This exercises the same paths as the 2026-07-23 cert sweep plus the under-load (Tests 2–3) and
no-load-drop (Test 1) variants.

---

## Density validation on `aen-prod` (2026-09-08, tag `om-20260908-170000`) — full PITR cycle PASS

First end-to-end validation on the rebuilt customer-density lab: **33 replica sets** (32 shards + cfg),
**5 members each (165 mongods)** across 6 hosts, **2×3T multi-PV LVM data VGs per node** (`/u01/data`),
journals uniform in-place, OM oplog slices on a FlashBlade NFS store.

| Phase | Result |
|---|---|
| `preflight-mongo-backup` | ✅ 9/9 PASS (after journal migration + PG build) |
| Snapshot at 33-RS density | ✅ 33 cursors, balancer quiesced/restored, 4-array PG snap, `mongo:floor` tag |
| Tailer at scale | ✅ 33 PRIMARY streams; backlog staged as **one tar stream per shard** (Tier-2 #11) |
| **Below-floor PIT probe** | ✅ **REFUSED** (`target 1788903213 < floor 1788903273`) — the dead-zone guard works |
| Restore `--pitr-target 0` | ✅ gate passed pre-overwrite; **first live multi-PV restore**: 12 volumes, VGs reassembled, 32 shards + cfg reformed in **145 s**, drift 0, `A=1 B=0` |
| Replay-all | ✅ all-or-nothing validation passed; 256 segments / 32 shards; **`unrecoveredTail=0`**, `A=1 B=1` |

**Three density findings fixed during the run** (each aborted safely pre-damage): the OM client timeout
(30s hardcoded → `OM_HTTP_TIMEOUT_SEC`), restore STEP 0's volume-vs-array count conflation (multi-PV broke
the single-volume assumption), and **balancer-state resurrection** — a sharded snapshot captures the
quiesced balancer, so every restore silently left the balancer OFF; the pre-quiesce state is now a
`mongo:balancer` tag that restore STEP 7.5 reapplies. Also fixed en route: `get_cluster_nodes` pagination
(dense groups exceed one `/hosts` page — 4 of 6 hosts silently missing from the PG on first build).

This run closes the "live validation pending" flag on the Tier-1 (`f5ef552`) and Tier-2 (`6751eab`) work.

---

## Cluster reshape + re-validation (2026-09-09): `aen-prod` 33-RS -> 3-shard, full PITR PASS

Reduced the dense lab to a maintainable shape and re-validated end to end. Sequence (the ordering is the
lesson): **unmanage third-party backup FIRST**, then reshape topology, then re-manage — reversing it wedges OM.

- **Phase 1 (unmanage):** the API `unmanage` accepted but stuck (the documented `Stop backup request failed`
  catch-22); cleared via **force-unmanage** (backup+delete the group's `backupjobs.clusters`/`jobs`/
  `thirdparty.jobs`, restart `mongodb-mms`).
- **Phase 2-3 (reshape):** emptied the automationConfig (agents orphan mongods -> stop+wipe out of band),
  then PUT a new topology: **3 shards x 3 members + 3-member config RS on `aen-mongo-01..04`**, 2 mongos.
  Agents converged 17/17 in ~40 s.
- **Phase 4 (re-enable):** `/manage` (sharded OK); `initialize-protection-groups --prune --force` cut
  `aen-prod-pg` from 12 volumes to the 8 of nodes 01-04; **purged 422 stale OM monitored-host records**
  (leftover 33-RS members) so discovery returns exactly the live topology; cleared stale `mongo:` tags on the
  removed 06/07 volumes. **`preflight-mongo-backup`: 9/9 PASS.**
- **Phase 5 (validate, tag `om-20260909-180000`):** tailer (4 PRIMARY streams) -> snapshot T1 + marker A ->
  marker B + 2000 docs -> drain -> restore `--pitr-target 0` (PITR gate passed pre-overwrite; **balancer
  auto-restored from the `mongo:balancer` tag**; 8 volumes, cluster reformed, drift 0, `A=1 B=0`) ->
  replay-all (21 segments/3 shards, `unrecoveredTail=0`, `A=1 B=1`). Balancer left ON, cluster healthy.

**Bugs found + fixed during the reshape:** `get_cluster_nodes` OM `/hosts` **pagination** (dense group > one
page silently dropped member hosts) and `initialize-protection-groups --prune` assuming **one volume per node**
(TypeError on multi-PV VGs). Both committed.

**`aen-rs-00` (3-member RS on 05/06/07): rebuilt and healthy, third-party backup NOT enabled.** Its `/manage`
fails with an OM-internal `NullPointerException` in `handleOplogAndSyncStoreReassignmentReplicaSet`
(`BackupStatus` null) that does not trace to any residual appdb doc after force-unmanage cleanup. Recommended
path: recreate the RS under a **fresh cluster identity** (the reused July `clusterId` carries state that keeps
tripping the NPE), or raise with MongoDB. RS backup validation deferred.

---

## `aen-rs-01` recreated (fresh identity) + PITR validated (2026-09-09, tag `om-20260909-190000`)

The old `aen-rs-00` third-party `manage` failed with an OM-internal NPE
(`handleOplogAndSyncStoreReassignmentReplicaSet`, null `BackupStatus`) that no residual-doc cleanup fixed.
**Root cause: the reused July `clusterId` (6a2a9456).** Recreating the RS under a **fresh identity** resolved it:

- Renamed `aen-rs-00` -> **`aen-rs-01`** (new dbPath `/u01/data/rs01`) via the automationConfig API; agents
  converged; orphaned old `rs00` mongods (holding :27017) were graceful-shutdown from localhost via mongosh.
- OM created a fresh hostCluster (id `6aa197b72a10026ff73f3d5c`, display name `Cluster_0`) and, on a
  duplicate-key conflict, **auto-deleted the stale `aen-rs-00` (6a2a9456) record** — clearing the identity that
  caused the NPE. `manage -> OK`, 1/1 snapshotable on the new id.
- `initialize-protection-groups --deployment aen-rs-01` created `aen-rs-01-pg` (6 volumes on 05/06/07).
  **preflight 9/9 PASS.**
- **PITR cycle:** tailer on primary `-06` -> snapshot T1 + marker A -> marker B + 2000 docs -> drain -> restore
  `--pitr-target 0` (gate passed; RS `replicaset` branch; drift 0; `A=1 B=0`) -> replay (5 segments,
  `unrecoveredTail=0`, `A=1 B=1`). First RS validation on the `/u01/data` mount.

**Lesson:** an OM replica-set `manage` NPE that survives force-unmanage residue cleanup is an OM identity-state
problem — recreate the RS under a new name/clusterId rather than surgically chasing appdb ghosts.

---

## Full certification pass on the reshaped lab (2026-09-09) — ALL PASS

Ran the certification suite core-first-then-full on the standing lab (`aen-prod` 3-shard + `aen-rs-01` RS),
build `main` @ current. Preflight 9/9 PASS on both deployments before starting.

**Phase A — core 4 (the four in-scope cert items):**

| Item | Deployment | Tag | Result |
|---|---|---|---|
| 2.A.a Sharded self-restore | aen-prod | `om-20260909-200001` | ✅ drift 0, sentinel gone, per-shard 2279+2297+2424=7000 |
| 2.B.e Sharded PIT | aen-prod | `om-20260909-200002` | ✅ restore→T1 A=1 B=0 (drift 0); replay→T2 A=1 B=1, unrecoveredTail=0 |
| 1.A.1.a RS self-restore | aen-rs-01 | `om-20260909-200003` | ✅ drift 0, sentinel gone |
| 1.B.1.a RS PIT | aen-rs-01 | `om-20260909-200004` | ✅ restore→T1 A=1 B=0; replay→T2 A=1 B=1, unrecoveredTail=0 |

**Phase B — full Test-SnapshotRestore suite (remaining tests; 4/5/6/7 = the core above):**

| # | Test | Tag | Result |
|---|---|---|---|
| 1 | Basic restore (no load) | `om-20260909-200005` | ✅ drop → restore → 9000, drift 0 |
| 2 | Restore under load | `om-20260909-200006` | ✅ 147,334 ∈ [146,800, 147,600] (drift 800); post-snap writes to 175,800 lost |
| 3 | PITR under load | `om-20260909-200007` | ✅ restore→T1 240,134 (in window); replay→T2 **278,534, unrecoveredTail=0** |
| 8 | Sharded snapshot quiesces balancer | `om-20260909-200008` | ✅ balancer true → stopped → re-enabled → true |

Every guard exercised live: the PITR pre-overwrite gate passed before each destructive PIT restore, the
floor guard + all-or-nothing per-shard window validation ran on every replay, and the balancer was
stopped+restored on every sharded snapshot. Both clusters left healthy (aen-prod 3 shards / balancer on;
aen-rs-01 3 members); no leftover load/tailer processes.

**Verdict: the full in-scope certification suite passes on the reshaped, current lab.**
