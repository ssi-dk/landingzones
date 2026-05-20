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
A cron activation that is bounded by selected **Runtime IDs**.
_Avoid_: User-scoped cron deployment, system-scoped cron deployment

**Cron Fragment Set**:
The Landing Zone cron files staged for one **Execution Context**.
_Avoid_: Runtime, selected runtime

**Runtime Selection**:
The set of **Runtime IDs** chosen for cron activation.
_Avoid_: Runtime prefix, artifact prefix, user selection

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
A staged `.cron` file that does not carry a **Runtime ID** in its filename.
_Avoid_: Unknown runtime, invalid cron

**Excluded Runtime Cron Fragment**:
An identified runtime cron file that is staged but not selected for activation.
_Avoid_: Deleted cron, stale cron

**Execution Context**:
The system and Unix account under which a runtime command or cron job runs.
_Avoid_: Runtime identity, deploy boundary

## Relationships

- A **Landing Zone Runtime** has exactly one **Runtime ID**.
- A **Runtime-Scoped Cron Deployment** activates cron entries for one or more selected **Runtime IDs**.
- An **Execution Context** can host multiple **Landing Zone Runtimes**.
- An **Execution Context** is not a sufficient deployment boundary when multiple **Runtime IDs** share it.
- A **Cron Fragment Set** can intentionally contain cron files for multiple **Runtime IDs** on the same **Execution Context**.
- A **Runtime Selection** defaults to the selected **Runtime ID** exactly.
- **Generated Runtime Metadata** describes the **Runtime IDs** represented by generated runtime artifacts.
- The **Transfer Catalog** owns transfer loading invariants before command code consumes rows.
- **Build/Runtime Catalog Loading** validates runnable transfer artifacts for `build`, deployment validation, and integration validation.
- **Reporting Catalog Loading** preserves normalized transfer facts for dashboard analysis without requiring runtime-only file columns.
- An **Unidentified Cron Fragment** can be preserved during activation without being treated as a **Landing Zone Runtime**.
- An **Excluded Runtime Cron Fragment** may remain staged while being omitted from the active crontab.

## Example dialogue

> **Dev:** "Should cron deployment install every Landing Zone cron file for this Unix user?"
> **Domain expert:** "No. It should deploy the selected **Runtime ID**. The Unix account is only the **Execution Context**."

> **Dev:** "Can one system/account activate cron for two sequencing nodes?"
> **Domain expert:** "Yes. The same **Execution Context** can intentionally activate a **Cron Fragment Set** containing multiple **Runtime IDs**."

## Flagged ambiguities

- "user" was used as both a runtime boundary and an execution account. Resolved: use **Runtime ID** for the runtime boundary and **Execution Context** for the system/account pair.
- "prefix" was used for the selected runtime identity, but this conflicts with artifact filename prefixes. Resolved: use **Runtime Selection** and exact **Runtime ID** matching.
