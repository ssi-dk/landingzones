# Runtime-Scoped Cron Activation

Cron activation is scoped by selected runtime IDs, not by the system/user execution context, because one Unix account can intentionally stage cron fragments for multiple Landing Zone runtimes. The deployment command should present the operator with the runtime cron fragments that will be activated, unidentified `.cron` fragments that will be preserved, and identified runtime cron fragments that will be excluded before replacing the active crontab.

**Consequences**

- Generated runtime metadata beside the cron output is the preferred source for expected runtime IDs; generated cron filenames are a warned compatibility fallback when metadata is missing.
- Unidentified `.cron` files are preserved by default because `crontab -` replaces the whole user crontab.
- Cron activation scope is selected through an explicit named CLI option, with `selected` as the default and `expected` and `staged` as broader activation scopes.
- `expected` activation prompts the operator to copy missing expected runtime cron fragments from the generated cron output into the staged cron directory before activation.
- `expected` activation fails when an expected runtime cron fragment is missing from both the staged cron directory and the generated cron output.
- Non-interactive cron activation fails closed unless the command includes an explicit confirmation option.
