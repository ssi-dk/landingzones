# Landing Zones App

The Landing Zones app provides operator-facing commands for building, validating, and activating transfer runtimes.

## Language

**Landing Zone Runtime**:
A deployed transfer environment with its own generated artifacts and operational identity.
_Avoid_: User deployment, server deployment

**Runtime ID**:
The canonical identity of a **Landing Zone Runtime**.
_Avoid_: System user, cron user, deploy user

**Runtime-Scoped Cron Deployment**:
A cron activation that is guided by selected **Runtime IDs** while accounting for the full **Cron Fragment Set** staged in the current **Execution Context**.
_Avoid_: User-scoped cron deployment, system-scoped cron deployment

**Cron Fragment Set**:
The Landing Zone cron files staged for one **Execution Context**.
_Avoid_: Runtime, selected runtime

**Runtime Selection**:
The set of **Runtime IDs** chosen as the direct subject of cron activation.
_Avoid_: Runtime prefix, artifact prefix, user selection

**Cron Activation Plan**:
The operator-visible classification of every staged `.cron` file before active cron is replaced.
_Avoid_: Crontab write list, deploy output

**Cron Fragment Exclusion**:
An exact staged cron filename that the operator intends to omit from active cron.
_Avoid_: Runtime exclusion, glob exclusion

**Execution-Context Cron Scope**:
The default cron activation scope that keeps active cron aligned with the current **Execution Context**.
_Avoid_: Selected scope, user scope

**Staged Cron Scope**:
The explicit cron activation scope that activates every staged `.cron` file in the **Cron Fragment Set**.
_Avoid_: Expected scope, execution-context scope

**Generated Runtime Metadata**:
The build-written list of **Runtime IDs** represented by generated runtime artifacts.
_Avoid_: Transfer inventory, runtime scan

**Transfer Catalog**:
The owner of transfer loading invariants for normalized transfer rows.
_Avoid_: Per-command transfer parsing, report-only parser

**Build/Runtime Catalog Loading**:
The transfer catalog mode used by build and runtime validation commands. It
requires runnable-script fields such as `log_file` and `flock_file`.
_Avoid_: Generator parser, strict report loading

**Reporting Catalog Loading**:
The transfer catalog mode used by reporting analysis. It reads normalized
transfer facts while allowing reporting-only inventories to omit runtime-only
file columns.
_Avoid_: Dashboard parser, loose runtime loading

**Unidentified Cron Fragment**:
A staged `.cron` file that does not carry a **Runtime ID** in its filename and is outside Landing Zones runtime ownership.
_Avoid_: Unknown runtime, invalid cron

**Preserved Runtime Cron Fragment**:
An identified runtime cron file that is outside the direct **Runtime Selection** but remains active in the **Execution Context**.
_Avoid_: Excluded cron, unknown cron

**Foreign Runtime Cron Fragment**:
An identified runtime cron file that belongs to a different **Execution Context** from the current cron activation.
_Avoid_: Preserved runtime, selected runtime

**Unresolved Runtime Cron Fragment**:
An identified runtime cron file whose **Runtime ID** is not found in the current **Transfer Catalog**.
_Avoid_: Unidentified cron, preserved runtime

**Excluded Runtime Cron Fragment**:
An identified runtime cron file that is staged but intentionally omitted from active cron.
_Avoid_: Deleted cron, stale cron

**Execution Context**:
The system and Unix account under which a runtime command or cron job runs.
_Avoid_: Runtime identity, deploy boundary

## Relationships

- A **Landing Zone Runtime** has exactly one **Runtime ID**.
- A **Runtime-Scoped Cron Deployment** has one **Execution Context** and one **Cron Activation Plan**.
- An **Execution Context** can host multiple **Landing Zone Runtimes**.
- An **Execution Context** is not a sufficient deployment boundary when multiple **Runtime IDs** share it.
- A **Cron Fragment Set** can intentionally contain cron files for multiple **Runtime IDs** on the same **Execution Context**.
- A **Runtime Selection** names the directly managed **Runtime IDs**, not every runtime that may remain active.
- A **Cron Activation Plan** distinguishes activated, preserved, and excluded runtime cron fragments.
- A **Cron Activation Plan** includes unidentified staged cron files so the operator can see whether they remain active.
- A **Cron Activation Plan** uses the **Transfer Catalog** to decide whether identified runtime cron fragments belong to the current **Execution Context**.
- A **Cron Fragment Exclusion** is matched by exact staged filename.
- A **Cron Fragment Exclusion** that matches no staged file should be visible to the operator without blocking activation.
- Multiple **Cron Fragment Exclusions** combine into one effective exclusion set.
- The **Execution-Context Cron Scope** is the safe default for shared managed hosts.
- **Generated Runtime Metadata** can broaden expected runtime activation but does not override the current **Execution Context** boundary.
- The **Staged Cron Scope** can activate foreign or unresolved runtime cron fragments because it treats the staged directory as the complete intended active crontab.
- A **Foreign Runtime Cron Fragment** should not remain active merely because it is present in the same staged directory.
- An **Unresolved Runtime Cron Fragment** requires operator attention because it may be an old, moved, or missing runtime.
- **Generated Runtime Metadata** describes the **Runtime IDs** represented by generated runtime artifacts.
- The **Transfer Catalog** owns transfer loading invariants before command code consumes rows.
- **Build/Runtime Catalog Loading** validates runnable transfer artifacts for `build`, deployment validation, and integration validation.
- **Reporting Catalog Loading** preserves normalized transfer facts for dashboard analysis without requiring runtime-only file columns.
- An **Unidentified Cron Fragment** can be activated during default execution-context activation without being treated as a **Landing Zone Runtime**.
- A **Preserved Runtime Cron Fragment** remains active because the **Execution Context** owns the active crontab.
- A **Foreign Runtime Cron Fragment** can be excluded from active cron while remaining staged.
- An **Unresolved Runtime Cron Fragment** can be excluded from active cron while remaining staged.
- An **Excluded Runtime Cron Fragment** may remain staged while being omitted from the active crontab.

## Example dialogue

> **Dev:** "Should cron deployment install every Landing Zone cron file for this Unix user?"
> **Domain expert:** "It should plan activation for the **Execution Context** and make selected, preserved, and excluded runtime fragments visible."

> **Dev:** "Can one system/account activate cron for two sequencing nodes?"
> **Domain expert:** "Yes. The same **Execution Context** can intentionally activate a **Cron Fragment Set** containing multiple **Runtime IDs**."

## Flagged ambiguities

- "user" was used as both a runtime boundary and an execution account. Resolved: use **Runtime ID** for the runtime boundary and **Execution Context** for the system/account pair.
- "prefix" was used for the selected runtime identity, but this conflicts with artifact filename prefixes. Resolved: use **Runtime Selection** and exact **Runtime ID** matching.
