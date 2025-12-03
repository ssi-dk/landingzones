# Landing Zones

Automated data transfer system using rsync with cron job generation.

## Quick Start

```bash
# Install
pip install -e .

# Generate cron files
lz-generate-cron --help
lz-generate-cron --transfers config/transfers.tsv --output-dir output/crontab.d --log-dir log

# Check deployment readiness
lz-check-deployment
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
| `system` | Source system identifier | `server1`, `localhost` |
| `users` | System user for transfer | `user1`, `local` |
| `source` | Source directory path | `/srv/data/src/` |
| `destination` | Destination (local or remote) | `user@host:/dest/` |
| `destination_port` | SSH port (optional) | `225` |
| `rsync_options` | Additional rsync flags | `--chown=:group` |
| `log_file` | Log file path (optional) | `log/transfers.log` |
| `flock_file` | Lock file path | `/tmp/transfer.lock` |

### Example

```tsv
system	users	source	destination	destination_port	rsync_options	log_file	flock_file
localhost	testuser	input/*	output/			log/transfers.log	/tmp/landingzones.lock
```

## CLI Commands

```bash
# Generate cron files with defaults
lz-generate-cron

# Custom paths
lz-generate-cron -t config/transfers.tsv -o output/crontab.d -l log

# Check deployment
lz-check-deployment
```

### Generated Cron Format

```bash
*/15 * * * * /usr/bin/flock -n /tmp/landingzones.lock -c '\
    rsync -av --remove-source-files /source/ /dest/ >> log/transfers.log 2>&1 && \
    find /source/ -mindepth 1 -type d -empty -delete >> log/transfers.log 2>&1'
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
