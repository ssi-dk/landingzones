# Validate runtime credentials before activation

The Python runtime can check the credentials and read-only endpoint access
required by its configured connections before accepting a transfer. Run the
check as the account that will execute transfers, using the installed runtime's
Python environment and configuration:

```sh
landingzones --config /path/to/runtime/config/execution.yaml transfer validate-credentials
```

This command contacts the configured endpoints. The checks use the runtime's
actual hostname/account, private-key paths, pinned host keys and environment.
Run it in the intended deployment environment; offline tests use synthetic
clients instead.

The default checks every enabled Python connection selected by the config's
`runtime_ids`. It checks both remote sources and remote destinations, even when
the destination adapter is `local`. Disabled connections, legacy executor rows,
other runtimes and unused credential entries are excluded. No request, receipt,
payload label, transfer event or state directory is created.

To investigate one connection:

```sh
landingzones --config /path/to/runtime/config/execution.yaml transfer validate-credentials --connection partner
```

## Checks by transport

| Configured access | Checks |
| --- | --- |
| SFTP destination or remote source (`sftp://` or `ssh://`) | Selected credential files, pinned host verification, authentication, SFTP subsystem, configured root and directory listing. |
| Rsync over SSH destination | Required SFTP access plus the local rsync executable and a bounded `rsync --version` command through OpenSSH using the transfer's key/host options. |
| ENA | Selected Webin credentials, TLS-protected FTP login and upload-root listing. |
| Local source/destination | Directory availability and read/traverse access; local rsync connections also require a working rsync executable. There is no remote credential to authenticate. |

The explicit adapter and endpoint URL determine which checks run. Credential
name prefixes are for readability and do not select a transport. Each connection
is reported independently, including connections that share a key file.

ENA metadata submission is not performed. A successful ENA upload-area login
does not establish metadata API authentication, metadata validity or accession
issuance. Supply the configured password environment variable to the execution
account; an enabled ENA route with missing credentials fails. Leave ENA routes
disabled while their live validation is deferred.

## Results and limits

The command prints JSON with an overall status, execution context and
per-connection checks. Checks identify the direction, transport, credential
reference, phase and result. Endpoint failures do not prevent checks of other
connections. Error categories are sanitized: passwords, private-key contents,
raw server replies, subprocess output and directory contents are not included.

Exit codes:

- `0`: all selected checks passed.
- `1`: at least one required check failed, or no configured connection was selected.
- `2`: configuration, execution-context or connection-selection error.

Validation does not upload, rename or delete files and does not submit metadata.
It cannot establish payload-file readability, write/delete permissions, available
space, ownership after transfer, atomic publication or later service availability.
The report records untested operations. Use the separate
[transport and processing tests](testing-transfers.md) for those acceptance checks.

## Deployment integration

A deployment should prepare a candidate environment and configuration, run this
command with that candidate environment as the runtime account, save its report,
and activate the candidate only when the check passes. A failed candidate must
leave the existing runtime configuration and environment selected. Do not start
new schedules or transfers following a failed check.

The corresponding Python deploy role implements that gate. It stores the latest
attempt's report at `log/credential-validation.json` with mode `0600`.
An application revision without this command fails the gate; use a compatible
reviewed application revision with the updated deploy role. Ansible syntax or
check mode does not provide live credential evidence.
