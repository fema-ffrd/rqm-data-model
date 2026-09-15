# ADR — Inventory Versioning, Branching, Bitemporality

**ID:** ADR-versioning
**Status:** Proposed
**Date:** 2026-09-03
**Resolves:** ISSUE-Q03, ISSUE-R02
**Serves user stories:** RQM-02, RQM-03, RQM-11, RQM-12, RQM-16
**Locks:** stable asset identity survives geometry / attribute / location changes; valid time ≠ record time; run snapshots are immutable.

## Context

The current `versioning` table is single-time: one `created_at` column, a `semver`, a lineage pointer, and an author string. That is enough to answer "what version is this?" but not "who changed what, when did it become true in the real world, and on which branch?" The reviewer's driving use case (ISSUE-R02) makes the gap explicit:

> GIS Reviewer 1 moves 50 buildings; GIS Reviewer 2 edits 30 attributes for calibration. We must be able to see who did what, when, and why — and roll back either edit without disturbing the other.

At the same time, ISSUE-Q03 asks whether inventory versioning requires full snapshots, branches, valid-time history, audit history, or a defined subset. The pragmatic answer is: some combination of all four, but only where change actually occurs. The locked invariant "bitemporality only where changes actually occur" means we do not push bitemporal columns onto every reference table — we route them through a single registry.

Two shape problems in the current schema drive the design:

1. **Identity is conflated with revision.** `buildings.building_id` is both "which building is this?" and "which snapshot?" A branch or a revision would need a new `building_id`, breaking every downstream FK (`hazard_links.building_id`, `loss_realizations.building_id`).
2. **Change history is not first-class.** There is no editor identity, no reason, no diff, no branch.

## Decision

Introduce four new tables and extend `versioning` with a bitemporal quadruple. Do not renumber the existing `building_id` FK graph — the stable-identity split is added *underneath* `buildings`, not on top of it.

### 1. Identity vs revision split

- `asset` — surrogate stable identity for any inventory asset (a building today; a bridge, a levee, a component tomorrow via Prompt 8A). `asset.asset_id` is the FK target that downstream tables treat as "which real-world thing is this," independent of any given revision.
- `asset_revision` — one row per (asset, branch, versioning) revision. Carries the valid-time interval during which the revision represents the asset's real-world state on that branch.
- `buildings` gains an `asset_id` FK. The `buildings` row is treated as the *current head projection* of the main-branch revision of that asset. Downstream FKs to `building_id` remain valid — they logically resolve through `buildings.asset_id → asset.asset_id`. Older revisions are materialized in `asset_revision`; a compatibility view (Prompt 11) exposes any-point-in-time projections.

This satisfies the locked invariant "stable asset identity survives geometry / attribute / location changes" and unblocks Prompt 4 (geometry-as-its-own-class): geometry revisions attach to `asset_id`, not `building_id`.

### 2. Branches

- `inventory_branch` — named branch of the inventory. Every branch has a parent (except `main`), a purpose (calibration, scenario, hypothesis, cleanup), a creating agent, and a status (`open | merged | abandoned`). Merging a branch back into `main` sets `merged_into_branch_id`.
- Every `asset_revision` row carries a `branch_id`. Two reviewers editing the same asset on different branches produce two revisions that co-exist without conflict; conflict resolution happens at merge time.

Branches don't fork identity — the `asset_id` is shared across all branches. This is what makes "roll back either edit without disturbing the other" tractable.

### 3. Bitemporal registry

Extend `versioning` with a bitemporal quadruple:

- `valid_from`, `valid_to` — real-world time interval during which the versioned entity is asserted to be true (valid-time).
- `record_from`, `record_to` — system time interval during which our knowledge base held this assertion (record-time / transaction-time).

`versioning` becomes the central bitemporal registry. Any entity that needs bitemporality points to a `version_id` and inherits the interval. Entities that don't need bitemporality (reference lookups, controlled vocabularies) leave the columns alone. This preserves the locked invariant "bitemporality applies only where changes actually occur" — we don't push a temporal quadruple onto every table.

Semantics:
- Correcting a wrong assertion: close the old `versioning` row's `record_to`, insert a new row with a new record interval covering the same or an overlapping `valid_from/valid_to`. The record intervals never overlap for the same entity_ref; valid intervals may.
- Amending a real-world change: insert a new `versioning` row with a new `valid_from`, close the previous row's `valid_to`. Both remain in the record.

### 4. Revision log

- `revision_log` — one row per structured change to a versioned entity: editor agent, reason, structured `change_kind` (attribute_edit / geometry_move / add / delete / metadata), and a `diff` jsonb payload with before/after snapshots keyed by column path. Points at the `version_id` that resulted.

This is the "who did what, when, why" register that ISSUE-R02 requires. It is complementary to `adopted_value_link` from ADR-provenance: `adopted_value_link` records *sources of a value*; `revision_log` records *edits to a value*.

### 5. Run immutability

`run_catalog` rows continue to reference a specific `manifest_id` and (via manifest) a specific set of versions. A run resolves versions at execution time and never re-resolves. This is the "run snapshots are immutable" locked invariant. Bitemporal edits to inventory *after* a run completes do not retroactively change the run's inputs.

## What is chosen vs deferred

**Chosen (in-scope):**
- Identity vs revision split for `buildings` (and, by extension via `asset`, future asset types).
- Bitemporal registry via `versioning` quadruple.
- Named branches with parent lineage and merge status.
- Structured revision log with editor, reason, change kind, and diff payload.

**Deferred:**
- **Full historical projections of `buildings`.** Today: only the current-head projection exists in `buildings`; historical revisions live in `asset_revision` and are reconstructable by view (Prompt 11 candidate). A dedicated `buildings_history` snapshot table is not needed.
- **Automatic three-way merge for branches.** Merges today are recorded (`merged_into_branch_id`) but resolution semantics are analyst-driven. A merge policy engine is out of scope.
- **Bitemporality on `loss_realizations` / `mv_loss_summary`.** Runs are immutable; a superseded run gets `status='superseded'` and a new run replaces it. No temporal bounds are added to the Iceberg tier.
- **Bitemporality on reference tables** (`generics`, `events`, enum vocabularies). Reference tables version via `versioning.version_id` but do not typically carry valid intervals.
- **Diff schema formalization.** `revision_log.diff` is jsonb with a recommended shape (`{path: [column], before: value, after: value}`) but is not fully schematized. Formal diff schema is Prompt 11 candidate.

## Validation rules (Prompt 11 will enforce in `preview_dict.py`)

1. Every `buildings.asset_id` resolves to an existing `asset`.
2. Every `asset_revision.(asset_id, branch_id, valid_from)` is unique — one revision per asset per branch per valid-time start.
3. Every `asset_revision.version_id` resolves to a `versioning` row; the `versioning` row's `record_from` is not null.
4. Every `inventory_branch.parent_branch_id` (when non-null) resolves to an existing branch; there is no cycle in the parent chain.
5. `versioning.record_from` is non-null on any row referenced by an `asset_revision`; `record_to` is null iff the row is current.
6. `revision_log.version_id` resolves to a `versioning` row; `revision_log.editor_agent_id` resolves to a `prov_agent`; `revision_log.change_kind` matches the enum.
7. For any `asset_id` on a single branch, the `[record_from, record_to)` intervals of the referenced `versioning` rows never overlap.

## Worked examples

### Example 1 — Reviewer 1 moves 50 buildings on a calibration branch

```
inventory_branch  {branch_id=5, name='calibration-2026Q3', parent_branch_id=1,
                   purpose='calibration', created_by_agent_id=101, status='open'}

For each of the 50 buildings (illustrated for asset_id=42):

versioning        {version_id=9001, entity_type='buildings', entity_ref='asset:42',
                   semver='1.3.0', parent_version_id=9000,
                   valid_from=2026-01-01, valid_to=null,
                   record_from=2026-09-03T14:22, record_to=null}

asset_revision    {revision_id=7001, asset_id=42, branch_id=5, version_id=9001,
                   valid_from=2026-01-01, valid_to=null}

revision_log      {log_id=15001, version_id=9001, editor_agent_id=101,
                   change_kind='geometry_move', reason='Reviewer 1 relocation pass',
                   diff={"path":["geom"],"before":"POINT(-80.11 40.44)","after":"POINT(-80.10 40.45)"}}
```

The `buildings` row for `asset_id=42` is unchanged on main; a projection of asset_revision.revision_id=7001 shows the moved location on branch 5.

### Example 2 — Reviewer 2 edits 30 attributes on the same branch — no conflict

Different attribute, same asset, same branch → a new `versioning` row that supersedes 9001, new `asset_revision` closing the previous record_to. Both edits are separately logged in `revision_log` (two rows, one per change). Rollback of either is a change-of-intent that inserts a new `versioning` row reverting the diff, not a delete.

### Example 3 — Correcting a wrong record (bitemporal amend)

A reviewer discovers Reviewer 1's move was based on a mis-georeferenced field photo. Real-world truth was the original location.

```
Close 9001:  record_to = 2026-09-04T09:00

versioning   {version_id=9002, entity_type='buildings', entity_ref='asset:42',
              semver='1.3.1', parent_version_id=9001,
              valid_from=2026-01-01, valid_to=null,     -- unchanged real-world validity
              record_from=2026-09-04T09:00, record_to=null}

asset_revision {revision_id=7002, asset_id=42, branch_id=5, version_id=9002, ...}

revision_log   {log_id=15002, version_id=9002, editor_agent_id=101,
                change_kind='geometry_move', reason='Reverting mis-georef; original coord retained',
                diff={"path":["geom"],"before":"POINT(-80.10 40.45)","after":"POINT(-80.11 40.44)"}}
```

The record log preserves the fact that we *thought* the building had moved between 09-03 14:22 and 09-04 09:00, even though we no longer assert that. Runs executed in that window remain reproducible.

## Claim discipline

- Identity-vs-revision split is *research-recommends* (standard temporal-modeling practice) + *project-requires* (RQM-11, RQM-12).
- Bitemporal quadruple is *specification-allows* (SQL:2011 bitemporal pattern) + *project-requires* (locked invariant "valid time ≠ record time").
- Branches are *research-recommends* + *project-requires* to the extent that ISSUE-R02's use case is two reviewers working in parallel.
- The `asset` table is deliberately generic (asset_type discriminator) so Prompt 8A's generics generalization can extend it without another refactor.

## Alternatives considered

- **Add versioning columns to each domain table.** Rejected — duplicates the temporal quadruple across 15+ tables and drifts.
- **A separate `buildings_history` snapshot table.** Rejected — doubles storage and creates two sources of truth for "the current building." The `asset_revision` + view pattern is cleaner.
- **Git-backed inventory (branch/merge in an external VCS).** Rejected — data volume and query patterns (per-asset lookup at time T) do not map to a VCS well; keeping branches inside the schema keeps queries in SQL.
- **Full temporal tables (SQL:2011 `SYSTEM VERSIONING`).** Rejected for now — Postgres doesn't support it natively without extensions, and we would still need `asset_revision` and `inventory_branch` on top.

## Impact on other ADRs

- **ADR-provenance:** `revision_log` and `adopted_value_link` are complementary — the former records *edits*, the latter records *sources*. Cross-linkable by `version_id`.
- **ADR-uncertainty:** `uncertainty_spec` continues to version via `versioning.version_id` and now inherits the bitemporal quadruple. Class-level specs typically have open valid intervals; building-specific specs may have bounded valid intervals when the underlying attribute changes.
- **ADR-geometry (Prompt 4):** the `geometries` table introduced there will attach to `asset_id`, not `building_id`, so a building's geometry can revise independently of its non-geometric attributes.
- **ADR-generics-generalization (Prompt 8A):** `asset.asset_type` is the hook — the enum will grow to include bridge, levee, component, and virtual assets. No further refactor of the identity model is needed.
- **ADR-export-contract (Prompt 5):** the compiler pins an `(asset_id, version_id)` pair per exported record; the resulting export is reproducible even if the inventory changes after export.
