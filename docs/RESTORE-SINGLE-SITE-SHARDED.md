# Restoring the whole sharded dataset from a single site's snapshots (proposed runbook)

> **Status: PROPOSED / MANUAL — not certified, not automated end-to-end.** The shipped `restore-mongo-snapshot`
> does an **in-place, whole-cluster** self-restore (it overwrites *every* member's volume and lets the existing
> topology re-form). Restoring from **one site's minimal set** (one member per shard + a config-server member)
> and standing the cluster back up from just those volumes involves steps the tool does not yet automate —
> force-reconfiguration and (for new hosts) metadata rewrites. Treat this as a design/runbook to **validate in a
> lab first**, not a proven capability. Related: [TODO-restore-to-different-rs.md](TODO-restore-to-different-rs.md),
> [how-it-works.md](how-it-works.md) → "whole-cluster revert".

---

## 1. Why one site is enough (the premise)

In a three-site sharded cluster where **each shard's replica set has one member at each site** and the
**config-server replica set also has a member at each site**, a single site holds — *in aggregate* — one
complete copy of the cluster:

```
Site A = shard_1 memberA + shard_2 memberA + shard_3 memberA + config memberA
       = one replica of every data slice + the chunk map
       = a complete, self-consistent copy of the whole cluster
```

So the **minimal set to reconstitute the whole cluster** is **one member of every shard + one config-server
member** — exactly the volumes captured in that site's snapshot. Data is *partitioned across* shards and
*replicated within* each shard across sites; the single site is complete only because it has one member of every
shard **and** a config member.

---

## 2. When this applies (and when it does NOT)

**Use it to:**
- **Rewind the entire cluster to a snapshot point** while restoring the fewest volumes — restore one member per
  shard + config, isolate them into a reduced-but-complete cluster at the snapshot point, then re-grow (the other
  members initial-sync and adopt the rewound data).
- **Clone/seed a new isolated cluster** on fresh hardware from that site's snapshots (topology-matched — see §7).

**Do NOT** restore one site's snapshot *into a still-live cluster*. If the other sites' members are up and
current, replication will immediately **overwrite the restored member** (or force a rollback) — the snapshot
data is discarded. Snapshot restore of a subset only makes sense when you **isolate** that subset into its own
cluster (rewind or clone). This is the single most common misconception here.

---

## 3. Preconditions (must all hold)

- [ ] The snapshot set was taken **balancer-quiesced** and cursors **aligned to a common timestamp** (this is what
      `new-mongo-snapshot` does for sharded — stops the balancer, drains any in-flight migration). Otherwise a
      chunk mid-migration is on two shards at once and the one-member-per-shard set is inconsistent.
- [ ] The chosen site actually has **one member of every shard AND a config-server member** (verify against
      `sh.status()` / `rs.conf()` per shard; a shard missing a member at that site makes the site incomplete).
- [ ] You accept **no redundancy** until §9 — a single-member set has no HA; a further failure loses data.
- [ ] You have the **snapshot tag** and can identify each member's FlashArray volume snapshot for that tag.
- [ ] For a rewind of the same hosts: the other sites' members will be **wiped and re-synced** (they must not
      re-inject stale data — §6). For new hosts: you have topology-matched, OM-registered destination hosts (§7).

---

## 4. Inventory the minimal snapshot set

For tag `om-YYYYMMDD-HHMMSS`, identify the FA protection-group snapshot member for **each** of:

| Role | Source member (example) | Restores onto |
|---|---|---|
| Config server (`aen-shard_0`) | `aen-mongo-config-00` volume | config host |
| shard_1 | this site's `aen-shard_1` member | shard_1 host |
| shard_2 | this site's `aen-shard_2` member | shard_2 host |
| shard_3 | this site's `aen-shard_3` member | shard_3 host |

That's **N shards + 1 config = the whole dataset**. (The shipped tool restores *all* members; scoping to one
site's four volumes is currently a **manual FA volume overwrite** per volume, or a future `--nodes` flag.)

---

## 5. Ordering principle

Always: **config server first → shards → `mongos` last.** The config server holds the chunk map every shard and
router depends on; nothing routes until it's up and reconfigured. Within each replica set, restore the volume,
start `mongod`, then **force-reconfigure** the RS down to the surviving member so it can elect a primary
(a 1-of-3 set has no majority on its own).

---

## 6. Procedure — rewind on the same hosts (surviving/same site)

1. **Freeze automation.** Stop the OM automation agent on the target hosts (or put the project in a state that
   matches the reduced topology) so OM doesn't fight the manual reconfig. Stop `mongod`/`mongos` on the targets.
2. **Restore the config-server volume** from its snapshot (CoW overwrite), remount `/data/mongo`.
3. **Bring up the config server**, then **force-reconfigure its RS to the surviving member(s)**:
   ```js
   // on the config-server member
   cfg = rs.conf();
   cfg.members = cfg.members.filter(m => m.host === "<this-config-host>:27019");
   rs.reconfig(cfg, {force: true});   // single-member config RS elects itself primary
   ```
4. **For each shard**, restore its member's volume, remount, start `mongod`, and force-reconfigure that shard's
   RS to the surviving member (same `rs.reconfig(..., {force:true})` pattern on each shard member).
5. **Start `mongos`** pointing at the (reconfigured) config server.
6. **Wipe + re-sync the other sites' members** *before* they can re-join with stale data: on each non-surviving
   member, wipe its dbPath so it performs a clean **initial sync** from the restored member when re-added (§9).
   Do not let a stale member rejoin a shard RS and win — that would undo the rewind.

Result: a **complete but single-copy** cluster at the snapshot point — every shard + the config server, one
member each, routing through `mongos`.

## 7. Procedure — clone onto new hosts (different hostnames)

Everything in §6, plus you must **rewrite the identity metadata** the snapshot carries (it references the source
hostnames), because a sharded clone has *N+1 interlocked identities*:

- On each restored member: rewrite `local.system.replset` to the destination RS name/host (offline standalone
  rewrite, as `restore-mongo-snapshot-to-target` does for a single RS).
- On the **config server**: update `config.shards.host` for every shard to the **new** shard RS connection
  strings, and confirm `config.chunks` still references the same **shard names** (keep names identical — §8).
- On **each shard**: update its `admin.system.version` **`shardIdentity`** (`configsvrConnectionString` → the new
  config server; `shardName` unchanged).

This cross-cutting rewrite across the config server and every shard is why sharded-to-different is **out of scope**
in the tool. It is doable by hand but error-prone — validate in a lab, and script it before trusting it.

---

## 8. The topology-matched requirement (clone case)

The destination must reproduce the **logical shape and names**: same **number of shards**, same **shard names**
(`config.chunks` points chunks at shard *names* — they must exist), a **config server**, and each shard as its own
RS. Only the **hostnames** may differ, and only if you do the §7 rewrites. Anything less (a single node, fewer
shards, renamed shards) cannot be reconstituted from snapshots — you'd have to un-shard via a logical
`mongodump`/`mongorestore` through `mongos`.

---

## 9. Re-establish redundancy and the balancer

Once the reduced cluster verifies healthy:
1. **Re-add members** at the other sites to each shard RS and to the config RS (`rs.add(...)`); each new member
   **initial-syncs** from the restored member and adopts the restored data.
2. Wait until every RS is fully healthy (all members `SECONDARY`/`PRIMARY`).
3. **Re-enable the balancer** (`sh.startBalancer()`) — it was quiesced for the snapshot and should stay off until
   the cluster is whole, or it will migrate chunks against a partially-rebuilt topology.

---

## 10. Verification

- `mongos` up; `sh.status()` lists the config server + **all** shards; `db.adminCommand({listShards:1})` returns
  the full set.
- Per-shard counts sum to the mongos aggregate for the verify collections (the same check
  `restore-mongo-snapshot` STEP 8 does for sharded — connect to each shard directly, count, assert
  `sum ≥ routed total`).
- Spot-check known documents / a sentinel to confirm it's the intended point in time.
- Expected noise: config-server `NotWritablePrimary` against `config.*` namespaces is normal; user data is what
  must be correct.

---

## 11. Risks and what to validate first

- **OM reconciliation is the top risk.** A reduced/rewritten topology fights OM's automationConfig (which still
  expects all members/hosts). Plan how OM adopts the reconfigured cluster (align automationConfig to the reduced
  set, or do the restore out-of-band and reconcile after). This is the same unresolved risk noted for
  `restore-mongo-snapshot-to-target`.
- **Force-reconfig is destructive to quorum** — double-check you're reconfiguring the *intended* member set.
- **Consistency depends entirely on the snapshot being balancer-quiesced + cursor-aligned** (§3).
- **Not certified.** Prove the whole flow — rewind and (if needed) clone — in a lab against a throwaway cluster
  before relying on it for production DR. Capture the run in `tests-docs/` like the other certification results.

---

## 12. What's automated vs. manual today

| Step | Tool support |
|---|---|
| Per-volume snapshot / CoW overwrite / mount / agent stop-start | ✅ `new-mongo-snapshot` / `restore-mongo-snapshot` (whole cluster) |
| Restoring **only one site's** four volumes | ⚠️ manual FA overwrite per volume (or a future `--nodes` scope flag) |
| Force-reconfigure each RS to the surviving member | ❌ manual (`rs.reconfig(..., {force:true})`) |
| Offline `local.system.replset` rewrite (new hosts) | ◑ pattern exists in `restore-mongo-snapshot-to-target` (RS only) |
| Rewrite `config.shards` / `shardIdentity` (new hosts) | ❌ manual — the out-of-scope sharded-clone piece |
| Re-grow + re-enable balancer | ❌ manual (`rs.add`, `sh.startBalancer`) |

**Recommendation:** if single-site sharded restore becomes a real requirement, the highest-value tool work is a
`--nodes`/`--site` scope flag on `restore-mongo-snapshot` plus an optional post-restore force-reconfig, which turns
§6 into one command for the same-hosts rewind case.
