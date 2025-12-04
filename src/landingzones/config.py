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
    log_dir: log
    output_dir: output
    crontab_dir: output/crontab.d
    input_dir: input
    default_lock_file: /tmp/landingzones.lock
    default_cron_frequency: "*/15 * * * *"
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


def _expand_path(path):
    """Expand environment variables and user home in path"""
    if path:
        return os.path.expandvars(os.path.expanduser(path))
    return path


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
            if search_dir == '.':
                config_path = os.path.join(cwd, name)
            else:
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
        - LZ_LOCK_FILE: Path to default lock file
        - LZ_LOG_DIR: Default log directory
        - LZ_OUTPUT_DIR: Default output directory
        - LZ_INPUT_DIR: Default input directory
        - LZ_CRONTAB_DIR: Default crontab output directory
        - LZ_CRON_FREQUENCY: Default cron schedule expression
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
                - log_dir
                - output_dir
                - crontab_dir
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
        
        # Store runtime overrides
        for key, value in kwargs.items():
            if value is not None:
                self._runtime_config[key] = value
    
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
    def default_lock_file(self):
        """Default lock file path for flock"""
        return self._get_value('default_lock_file', 'LZ_LOCK_FILE', '/tmp/landingzones.lock')
    
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
            'default_lock_file': self.default_lock_file,
            'log_dir': self.log_dir,
            'output_dir': self.output_dir,
            'input_dir': self.input_dir,
            'crontab_dir': self.crontab_dir,
            'default_cron_frequency': self.default_cron_frequency,
        }
    
    def __repr__(self):
        return "Config({})".format(self.to_dict())


# Global config instance
config = Config()
