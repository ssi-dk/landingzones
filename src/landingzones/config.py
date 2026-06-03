#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Configuration management for Landing Zone.

Provides centralized configuration with sensible defaults.

Configuration priority (highest to lowest):
1. Runtime arguments passed to load_config()
2. Environment variables (LZ_*)
3. config.yaml file in current working directory
4. Default values (relative to current working directory)

Example config.yaml:
    transfers_file: config/transfers.tsv
    report_transfer_log_file: output/log/Landing_Zone_test_local.transfers.tsv
    log_dir: log
    output_dir: output
    crontab_dir: output/crontab.d
    cron_fragment_exclusions:
      - old-runtime.Landing_Zone.cron
    rit_managed_locations:
      localhost: /srv/rit_managed/
    flock_paths:
      localhost: /usr/bin/flock
    rit_managed_folder_structure:
      sh_output: runtimes/server1_prod.user1/landingzone/output/scripts/
      crontabs: runtimes/server1_prod.user1/landingzone/output/crontab.d/
      log: log/
      flock: flock/
    input_dir: input
    default_lock_file: /tmp/landingzones.lock
    default_cron_frequency: "*/15 * * * *"
    validation_scripts_dir: output/validation_scripts
    runtime_ids:
      - server1_prod.user1
    notifications:
      endpoint: https://example.org/landingzones/events
      token_env: LANDINGZONES_NOTIFY_TOKEN
      title: Landing Zone transfer event
      body: A Landing Zone transfer emitted a notifiable event.
      timeout_seconds: 5
      status_file: Landing_Zone_notifications.tsv
      status_lock_file: Landing_Zone_notifications.lock
"""

import os
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


# Default config file names to search for
CONFIG_FILE_NAMES = ['config.yaml', 'config.yml', 'landingzones.yaml', 'landingzones.yml']

# Subdirectories to search for config files
CONFIG_SEARCH_DIRS = ['.', 'config']
DEFAULT_RIT_MANAGED_FOLDER_STRUCTURE = {
    'sh_output': 'scripts',
    'crontabs': 'crontab.d',
    'log': 'log',
    'flock': 'flock',
}
DEFAULT_NOTIFICATIONS = {
    'endpoint': '',
    'token_env': '',
    'title': 'Landing Zone transfer event',
    'body': 'A Landing Zone transfer emitted a notifiable event.',
    'timeout_seconds': '5',
    'status_file': '',
    'status_lock_file': '',
}


def _expand_path(path):
    """Expand environment variables and user home in path"""
    if path:
        return os.path.expandvars(os.path.expanduser(path))
    return path


def _expand_path_mapping(values):
    """Return a copy of a mapping with shell-style paths expanded."""
    return {
        key: _expand_path(value)
        for key, value in dict(values).items()
    }


def _normalize_runtime_ids(value):
    """Normalize runtime_id config values to a de-duplicated list."""
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = value.split(',')
    elif isinstance(value, (list, tuple, set)):
        raw_values = value
    else:
        raw_values = [value]

    normalized = []
    for raw_value in raw_values:
        text = str(raw_value).strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _normalize_string_list(value):
    """Normalize list-like config values to de-duplicated strings."""
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = value.split(',')
    elif isinstance(value, (list, tuple, set)):
        raw_values = value
    else:
        raw_values = [value]

    normalized = []
    for raw_value in raw_values:
        text = str(raw_value).strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _load_yaml_config(config_file=None):
    """Load configuration from a YAML file.
    
    Args:
        config_file: Path to config file. If None, searches for default names in CWD.
    
    Returns:
        dict: Configuration values from YAML, or empty dict if not found.
    """
    if not YAML_AVAILABLE:
        return {}
    
    if config_file:
        # Explicit config file specified
        config_path = _expand_path(config_file)
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        return {}
    
    # Search for default config file names in CWD and config/ subdirectory
    cwd = os.getcwd()
    for search_dir in CONFIG_SEARCH_DIRS:
        for name in CONFIG_FILE_NAMES:
            config_path = os.path.join(cwd, search_dir, name)
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f) or {}
    
    return {}


class Config:
    """Configuration settings for Landing Zone.
    
    All paths default to being relative to the current working directory.
    
    Configuration priority (highest to lowest):
    1. Values set via load_config() or direct attribute assignment
    2. Environment variables (LZ_*)
    3. config.yaml file
    4. Default values
    
    Environment variables:
        - LZ_CONFIG_FILE: Path to config.yaml file
        - LZ_TRANSFERS_FILE: Path to transfers.tsv
        - LZ_TEST_DATA: Path to toy test data for --test-with-data
        - LZ_ARTIFACT_OWNER_ID: Owner marker for generated artifacts
        - LZ_ARTIFACT_PREFIX: Filename prefix for generated artifacts
        - LZ_REPORT_TRANSFER_LOG_FILE: Path to the default transfer TSV used for reporting
        - LZ_LOCK_FILE: Path to default lock file
        - LZ_LOG_DIR: Default log directory
        - LZ_OUTPUT_DIR: Default output directory
        - LZ_INPUT_DIR: Default input directory
        - LZ_CRONTAB_DIR: Default crontab output directory
        - LZ_VALIDATION_SCRIPTS_DIR: Default validation wrapper output directory
        - LZ_RUNTIME_IDS: Comma-separated runtime_id filter values
        - LZ_RUNTIME_ID: Single runtime_id filter value
        - LZ_CRON_FRAGMENT_EXCLUSIONS: Comma-separated staged cron filenames to exclude
        - LZ_CRON_FREQUENCY: Default cron schedule expression
        - LZ_NOTIFICATION_ENDPOINT: Optional notification API endpoint
        - LZ_NOTIFICATION_TOKEN_ENV: Optional env var name containing bearer token
    """
    
    def __init__(self):
        self._yaml_config = {}
        self._runtime_config = {}
        self._config_file = None
        # Auto-load config.yaml if present
        self._load_yaml()
    
    def _load_yaml(self, config_file=None):
        """Load YAML configuration file."""
        # Check for config file from environment first
        if config_file is None:
            config_file = os.environ.get('LZ_CONFIG_FILE')
        
        self._config_file = config_file
        self._yaml_config = _load_yaml_config(config_file)
    
    def load_config(self, config_file=None, **kwargs):
        """Load configuration from file and/or runtime arguments.
        
        Args:
            config_file: Path to YAML config file (optional)
            **kwargs: Runtime configuration overrides:
                - transfers_file
                - test_data
                - log_dir
                - output_dir
                - crontab_dir
                - validation_scripts_dir
                - rit_managed_locations
                - flock_paths
                - rit_managed_folder_structure
                - input_dir
                - default_lock_file
                - default_cron_frequency
        
        Example:
            config.load_config(
                config_file='my_config.yaml',
                log_dir='/var/log/landingzones',
                default_cron_frequency='*/5 * * * *'
            )
        """
        if config_file:
            self._load_yaml(config_file)
            self._runtime_config = {}
        
        # Store runtime overrides
        for key, value in kwargs.items():
            if value is not None:
                self._runtime_config[key] = value

    def snapshot_state(self):
        """Capture internal config state for temporary override workflows."""
        return {
            'yaml_config': dict(self._yaml_config),
            'runtime_config': dict(self._runtime_config),
            'config_file': self._config_file,
        }

    def restore_state(self, snapshot):
        """Restore a snapshot created by snapshot_state()."""
        self._yaml_config = dict(snapshot['yaml_config'])
        self._runtime_config = dict(snapshot['runtime_config'])
        self._config_file = snapshot['config_file']
    
    def _get_value(self, key, env_var, default):
        """Get configuration value with priority: runtime > env > yaml > default."""
        # 1. Runtime config (highest priority)
        if key in self._runtime_config:
            return _expand_path(self._runtime_config[key])
        
        # 2. Environment variable
        env_value = os.environ.get(env_var)
        if env_value:
            return _expand_path(env_value)
        
        # 3. YAML config
        if key in self._yaml_config:
            return _expand_path(self._yaml_config[key])
        
        # 4. Default value
        return _expand_path(default)
    
    @property
    def config_file(self):
        """Path to the loaded config file, if any"""
        return self._config_file
    
    @property
    def transfers_file(self):
        """Path to the transfers.tsv configuration file"""
        return self._get_value('transfers_file', 'LZ_TRANSFERS_FILE', 'config/transfers.tsv')

    @property
    def report_transfer_log_file(self):
        """Path to the default transfer TSV input used for reporting commands."""
        value = self._get_value(
            'report_transfer_log_file',
            'LZ_REPORT_TRANSFER_LOG_FILE',
            '',
        )
        if value:
            return value

        # Backward compatibility for pre-rename config and automation setups.
        legacy_env_value = os.environ.get('LZ_TRANSFER_LOG_FILE')
        if legacy_env_value:
            return _expand_path(legacy_env_value)

        legacy_yaml_value = self._yaml_config.get('transfer_log_file')
        if legacy_yaml_value:
            return _expand_path(legacy_yaml_value)

        legacy_runtime_value = self._runtime_config.get('transfer_log_file')
        if legacy_runtime_value:
            return _expand_path(legacy_runtime_value)

        return ''

    @property
    def transfer_log_file(self):
        """Backward-compatible alias for older callers."""
        return self.report_transfer_log_file
    
    @property
    def default_lock_file(self):
        """Default lock file path for flock"""
        return self._get_value('default_lock_file', 'LZ_LOCK_FILE', '/tmp/landingzones.lock')

    @property
    def test_data(self):
        """Path to toy data used by --test-with-data."""
        return self._get_value('test_data', 'LZ_TEST_DATA', 'tests/data')
    
    @property
    def log_dir(self):
        """Default log directory"""
        return self._get_value('log_dir', 'LZ_LOG_DIR', 'log')
    
    @property
    def output_dir(self):
        """Default output directory"""
        return self._get_value('output_dir', 'LZ_OUTPUT_DIR', 'output')
    
    @property
    def input_dir(self):
        """Default input directory"""
        return self._get_value('input_dir', 'LZ_INPUT_DIR', 'input')
    
    @property
    def crontab_dir(self):
        """Default crontab output directory"""
        # Special case: crontab_dir defaults to output_dir/crontab.d
        default = os.path.join(self.output_dir, 'crontab.d')
        return self._get_value('crontab_dir', 'LZ_CRONTAB_DIR', default)

    @property
    def validation_scripts_dir(self):
        """Default output directory for generated validation wrapper scripts."""
        default = os.path.join(self.output_dir, 'validation_scripts')
        return self._get_value(
            'validation_scripts_dir',
            'LZ_VALIDATION_SCRIPTS_DIR',
            default,
        )

    @property
    def artifact_owner_id(self):
        """Owner marker for generated files in shared artifact directories."""
        return self._get_value('artifact_owner_id', 'LZ_ARTIFACT_OWNER_ID', '')

    @property
    def artifact_prefix(self):
        """Optional filename prefix for generated scripts, crons, and wrappers."""
        return self._get_value('artifact_prefix', 'LZ_ARTIFACT_PREFIX', '')

    @property
    def runtime_ids(self):
        """Runtime IDs used to scope transfer loading and generated artifacts."""
        for key in ('runtime_ids', 'runtime_id'):
            if key in self._runtime_config:
                return _normalize_runtime_ids(self._runtime_config[key])

        env_value = os.environ.get('LZ_RUNTIME_IDS')
        if env_value:
            return _normalize_runtime_ids(env_value)
        env_value = os.environ.get('LZ_RUNTIME_ID')
        if env_value:
            return _normalize_runtime_ids(env_value)

        for key in ('runtime_ids', 'runtime_id'):
            if key in self._yaml_config:
                return _normalize_runtime_ids(self._yaml_config[key])

        return []

    @property
    def cron_fragment_exclusions(self):
        """Exact staged cron filenames to exclude from activation."""
        key = 'cron_fragment_exclusions'
        if key in self._runtime_config:
            return _normalize_string_list(self._runtime_config[key])

        env_value = os.environ.get('LZ_CRON_FRAGMENT_EXCLUSIONS')
        if env_value:
            return _normalize_string_list(env_value)

        if key in self._yaml_config:
            return _normalize_string_list(self._yaml_config[key])

        return []

    @property
    def rit_managed_locations(self):
        """Configured rit_managed base locations for each system."""
        runtime_value = self._runtime_config.get('rit_managed_locations')
        if runtime_value is not None:
            return _expand_path_mapping(runtime_value)

        yaml_value = self._yaml_config.get('rit_managed_locations')
        if yaml_value is not None:
            return _expand_path_mapping(yaml_value)

        return {}

    @property
    def rit_managed_folder_structure(self):
        """Configured folder suffixes under each rit_managed base location."""
        runtime_value = self._runtime_config.get('rit_managed_folder_structure')
        if runtime_value is not None:
            return _expand_path_mapping(runtime_value)

        yaml_value = self._yaml_config.get('rit_managed_folder_structure')
        if yaml_value is not None:
            return _expand_path_mapping(yaml_value)

        return dict(DEFAULT_RIT_MANAGED_FOLDER_STRUCTURE)

    @property
    def flock_paths(self):
        """Configured flock binary paths by system."""
        runtime_value = self._runtime_config.get('flock_paths')
        if runtime_value is not None:
            return _expand_path_mapping(runtime_value)

        yaml_value = self._yaml_config.get('flock_paths')
        if yaml_value is not None:
            return _expand_path_mapping(yaml_value)

        return {}

    @property
    def notifications(self):
        """Configured notification API and delivery-log settings."""
        values = dict(DEFAULT_NOTIFICATIONS)

        yaml_value = self._yaml_config.get('notifications')
        if yaml_value is not None:
            values.update(dict(yaml_value))

        runtime_value = self._runtime_config.get('notifications')
        if runtime_value is not None:
            values.update(dict(runtime_value))

        env_endpoint = os.environ.get('LZ_NOTIFICATION_ENDPOINT')
        if env_endpoint:
            values['endpoint'] = env_endpoint

        env_token_env = os.environ.get('LZ_NOTIFICATION_TOKEN_ENV')
        if env_token_env:
            values['token_env'] = env_token_env

        for key in ('endpoint', 'token_env', 'title', 'body', 'status_file', 'status_lock_file'):
            values[key] = _expand_path(str(values.get(key, '') or ''))
        values['timeout_seconds'] = str(values.get('timeout_seconds', '') or '5')
        return values

    @property
    def path_variables(self):
        """Configured ${VAR} placeholder values used in transfers.tsv paths."""
        values = dict(os.environ)

        yaml_value = self._yaml_config.get('path_variables')
        if yaml_value is not None:
            for key, value in yaml_value.items():
                values[str(key)] = _expand_path(value)

        runtime_value = self._runtime_config.get('path_variables')
        if runtime_value is not None:
            for key, value in runtime_value.items():
                values[str(key)] = _expand_path(value)

        return values

    def get_rit_managed_location(self, system):
        """Return the rit_managed base location for a system."""
        if system in self.rit_managed_locations:
            return self.rit_managed_locations[system]
        return self.output_dir

    def get_flock_path(self, system):
        """Return the flock binary path for a system."""
        return self.flock_paths.get(system, '/usr/bin/flock')

    def get_rit_managed_path(self, system, structure_key):
        """Return a system-specific path from base location + configured suffix."""
        base_path = self.get_rit_managed_location(system)
        suffix = self.rit_managed_folder_structure.get(structure_key)
        if suffix is None:
            raise KeyError(
                "rit_managed_folder_structure missing key: {0}".format(
                    structure_key
                )
            )
        return os.path.join(base_path, suffix)

    def resolve_managed_file_path(self, system, filename, structure_key):
        """Resolve a managed file name to a system-specific full path."""
        value = str(filename).strip() if filename is not None else ''
        if not value or value == 'nan':
            return ''
        if '/' in value:
            return value
        directory = self.get_rit_managed_path(system, structure_key)
        return os.path.join(directory, value)
    
    @property
    def default_cron_frequency(self):
        """Default cron frequency schedule.
        
        Format: standard cron expression (minute hour day month weekday)
        Examples:
            - */15 * * * *  (every 15 minutes)
            - */5 * * * *   (every 5 minutes)
            - 0 * * * *     (every hour)
            - 0 0 * * *     (daily at midnight)
        """
        return self._get_value('default_cron_frequency', 'LZ_CRON_FREQUENCY', '*/15 * * * *')
    
    def to_dict(self):
        """Return all configuration values as a dictionary."""
        return {
            'config_file': self.config_file,
            'transfers_file': self.transfers_file,
            'report_transfer_log_file': self.report_transfer_log_file,
            'test_data': self.test_data,
            'default_lock_file': self.default_lock_file,
            'log_dir': self.log_dir,
            'output_dir': self.output_dir,
            'input_dir': self.input_dir,
            'crontab_dir': self.crontab_dir,
            'validation_scripts_dir': self.validation_scripts_dir,
            'artifact_owner_id': self.artifact_owner_id,
            'artifact_prefix': self.artifact_prefix,
            'runtime_ids': self.runtime_ids,
            'cron_fragment_exclusions': self.cron_fragment_exclusions,
            'rit_managed_locations': self.rit_managed_locations,
            'flock_paths': self.flock_paths,
            'notifications': self.notifications,
            'path_variables': self.path_variables,
            'rit_managed_folder_structure': self.rit_managed_folder_structure,
            'default_cron_frequency': self.default_cron_frequency,
        }
    
    def __repr__(self):
        return "Config({})".format(self.to_dict())


# Global config instance
config = Config()
