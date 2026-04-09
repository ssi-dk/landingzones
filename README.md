# Landing Zones

Automated data transfer system using rsync with cron job generation.

## Quick Start

```bash
# Install
pip install -e .

# Generate cron files
lz-generate-cron --help
lz-generate-cron

# Check deployment readiness
lz-check-deployment

# Run toy data through the configured flows
lz-check-deployment --test-with-data
```

## Project Structure

```
landingzones/
├── src/landingzones/           # Main package
│   ├── generate_cron_files.py  # Cron generation tool
│   ├── check_deployment_readiness.py
│   └── config/transfers.tsv    # Default config
├── input/                      # Default input directory
├── output/                     # Default output directory
│   └── crontab.d/             # Generated cron files
├── log/                        # Default log directory
├── tests/                      # Test suite
├── setup.py                    # Package config
└── requirements.txt            # Dependencies
```

## Configuration

The system is configured via a tab-separated `transfers.tsv` file:

| Column | Description | Example |
|--------|-------------|---------|
| `identifiers` | Unique transfer ID used for generated shell script names | `transfer_001`, `gridion_to_calc` |
| `system` | Source system identifier | `server1`, `localhost` |
| `users` | System user for transfer | `user1`, `local` |
| `source` | Source directory path | `/srv/data/src/` |
| `source_port` | SSH port for remote sources (optional) | `2222` |
| `destination` | Destination (local or remote) | `user@host:/dest/` |
| `destination_port` | SSH port (optional) | `225` |
| `rsync_options` | Additional rsync flags | `--chown=:group` |
| `io_nice` | Optional `ionice` settings for `rsync` | `-c2 -n7` |
| `log_file` | Log file name resolved under the system log folder | `transfers.log` |
| `flock_file` | Lock file name resolved under the system flock folder | `transfer.lock` |

### Example

```tsv
identifiers	system	users	source	source_port	destination	destination_port	rsync_options	io_nice	log_file	flock_file
local_copy	localhost	testuser	input/*		output/				transfers.log	landingzones.lock
```

## CLI Commands

```bash
# Generate cron files with defaults
lz-generate-cron

# Check deployment readiness
lz-check-deployment

# Seed toy data and run the real scripts/logs/locks
lz-check-deployment --test-with-data
```

### Generated Cron Format

```bash
*/15 * * * * /bin/sh output/scripts/local_copy.sh
```

## Installation

```bash
# Development mode
pip install -e .

# With test dependencies
pip install -e ".[test]"

# Production
pip install .
```

## Testing

```bash
# Run all tests
pytest

# Verbose
pytest -v

# With coverage
pytest --cov=landingzones --cov-report=html

# Specific test
pytest tests/test_generate_cron_files.py::TestClassName::test_method
```

### Test With Data

`--test-with-data` is the integration-style test mode. It copies toy data into the configured starting locations, generates the real shell scripts, and runs the transfers using the normal log and flock paths. After a successful run the data should be visible in the terminal destinations unless you choose cleanup at the prompt.

Required config in your deployment `config.yaml`:

```yaml
transfers_file: input/transfers.tsv
test_data: tests/toy_data/
rit_managed_locations:
  test_local: tests/test_local
flock_paths:
  test_local: /opt/homebrew/bin/flock
rit_managed_folder_structure:
  log: output/log/
  flock: output/flock/
  sh_output: output/scripts/
  crontabs: output/crontab.d/
```

Typical local fixture layout:

```text
deploy/local/
├── config/config.yaml
├── input/transfers.tsv
├── tests/toy_data/
└── tests/test_local/
```

How to run it:

```bash
# Run from the deployment root that owns config/, input/, and tests/
cd deploy/local

# Generate and execute the transfer scripts with toy data
lz-check-deployment --config config/config.yaml --test-with-data
```

What it does:

- Filters `transfers.tsv` to the current `system` and `user`
- Seeds each initial source root from `test_data`
- Generates scripts into the configured `sh_output` directory
- Uses the configured `log` and `flock` directories
- Executes the scripts in transfer order
- Validates that the seeded top-level directories reached the terminal destinations

After a successful run it asks whether you want cleanup. Answer `y` to remove the propagated test directories plus generated log and lock artifacts so the next run starts from the initial state. Answer `n` to inspect the final tree and logs.

## Deployment

1. Configure `transfers.tsv` with your routes
2. Generate cron files: `lz-generate-cron`
3. Deploy:
   ```bash
   cp output/crontab.d/*.cron ~/crontab.d/
   cat ~/crontab.d/*.cron | crontab -
   ```

Or use automated deployment:
```bash
lz-check-deployment
```

## Development

```bash
# Make changes in src/landingzones/
# Run tests
pytest

# Test CLI
lz-generate-cron --help
```

## Requirements

- Python >= 3.8
- pandas >= 1.0.0
- System: rsync, ssh, flock
