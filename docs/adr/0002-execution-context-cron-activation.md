# Execution-Context Cron Activation

Runtime-scoped cron activation originally defaulted to activating only the selected runtime fragments, but shared managed hosts can stage multiple Landing Zone runtimes plus non-Landing-Zones cron fragments under one Unix account. The default cron activation scope is now the **Execution-Context Cron Scope**: it activates the selected runtime fragments, preserves staged runtime fragments that the **Transfer Catalog** resolves to the same **Execution Context**, preserves unidentified staged `.cron` files, and excludes foreign or unresolved runtime fragments unless the operator chooses a broader explicit scope.

**Consequences**

- This supersedes the default `selected` behavior described in ADR 0001.
- The **Transfer Catalog** is the source for deciding whether an identified runtime cron fragment belongs to the current **Execution Context**.
- The default scope is named `execution-context` so operators do not confuse it with exact selected-runtime replacement.
- Exact filename **Cron Fragment Exclusions** from deploy config and CLI arguments combine into one effective exclusion set.
- Missing exclusion targets warn without blocking activation.
- The `staged` scope remains the explicit complete-directory mode and can activate foreign or unresolved runtime fragments after preview.
