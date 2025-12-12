#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test suite for config.py"""

import os
import tempfile
import pytest

from landingzones import config


class TestLoadYamlConfig:
    """Test the _load_yaml_config function"""
    
    def test_explicit_config_file(self, tmp_path):
        """Test loading an explicitly specified config file"""
        config_file = tmp_path / "custom_config.yaml"
        config_file.write_text("transfers_file: custom/transfers.tsv\nlog_dir: custom_log\n")
        
        result = config._load_yaml_config(str(config_file))
        
        assert result['transfers_file'] == 'custom/transfers.tsv'
        assert result['log_dir'] == 'custom_log'
    
    def test_explicit_config_file_not_found(self, tmp_path):
        """Test loading a non-existent explicit config file returns empty dict"""
        result = config._load_yaml_config(str(tmp_path / "nonexistent.yaml"))
        
        assert result == {}
    
    def test_search_config_in_cwd(self, tmp_path, monkeypatch):
        """Test finding config.yaml in current working directory"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("log_dir: found_in_cwd\n")
        
        monkeypatch.chdir(tmp_path)
        result = config._load_yaml_config()
        
        assert result['log_dir'] == 'found_in_cwd'
    
    def test_search_config_in_config_subdir(self, tmp_path, monkeypatch):
        """Test finding config.yaml in config/ subdirectory"""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_file = config_dir / "config.yaml"
        config_file.write_text("log_dir: found_in_config_subdir\n")
        
        monkeypatch.chdir(tmp_path)
        result = config._load_yaml_config()
        
        assert result['log_dir'] == 'found_in_config_subdir'
    
    def test_cwd_takes_priority_over_config_subdir(self, tmp_path, monkeypatch):
        """Test that config in CWD takes priority over config/ subdirectory"""
        # Create config in config/ subdir
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        subdir_config = config_dir / "config.yaml"
        subdir_config.write_text("log_dir: from_subdir\n")
        
        # Create config in CWD
        cwd_config = tmp_path / "config.yaml"
        cwd_config.write_text("log_dir: from_cwd\n")
        
        monkeypatch.chdir(tmp_path)
        result = config._load_yaml_config()
        
        # CWD should take priority
        assert result['log_dir'] == 'from_cwd'
    
    def test_search_alternative_config_names(self, tmp_path, monkeypatch):
        """Test finding config files with alternative names"""
        config_file = tmp_path / "landingzones.yaml"
        config_file.write_text("log_dir: found_landingzones_yaml\n")
        
        monkeypatch.chdir(tmp_path)
        result = config._load_yaml_config()
        
        assert result['log_dir'] == 'found_landingzones_yaml'
    
    def test_config_yml_extension(self, tmp_path, monkeypatch):
        """Test finding config files with .yml extension"""
        config_file = tmp_path / "config.yml"
        config_file.write_text("log_dir: found_yml\n")
        
        monkeypatch.chdir(tmp_path)
        result = config._load_yaml_config()
        
        assert result['log_dir'] == 'found_yml'
    
    def test_no_config_found(self, tmp_path, monkeypatch):
        """Test returns empty dict when no config file found"""
        monkeypatch.chdir(tmp_path)
        result = config._load_yaml_config()
        
        assert result == {}
    
    def test_empty_config_file(self, tmp_path, monkeypatch):
        """Test loading an empty config file returns empty dict"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("")
        
        monkeypatch.chdir(tmp_path)
        result = config._load_yaml_config()
        
        assert result == {}
    
    def test_config_with_only_comments(self, tmp_path, monkeypatch):
        """Test loading a config file with only comments returns empty dict"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("# This is a comment\n# Another comment\n")
        
        monkeypatch.chdir(tmp_path)
        result = config._load_yaml_config()
        
        assert result == {}


class TestExpandPath:
    """Test the _expand_path function"""
    
    def test_expand_user_home(self):
        """Test expanding ~ to user home directory"""
        result = config._expand_path("~/test/path")
        
        assert result.startswith(os.path.expanduser("~"))
        assert "~" not in result
    
    def test_expand_env_variable(self, monkeypatch):
        """Test expanding environment variables in path"""
        monkeypatch.setenv("TEST_VAR", "/test/value")
        
        result = config._expand_path("$TEST_VAR/subdir")
        
        assert result == "/test/value/subdir"
    
    def test_none_path(self):
        """Test that None path returns None"""
        result = config._expand_path(None)
        
        assert result is None
    
    def test_empty_path(self):
        """Test that empty path returns empty string"""
        result = config._expand_path("")
        
        assert result == ""


class TestConfigClass:
    """Test the Config class"""
    
    def test_default_values(self, tmp_path, monkeypatch):
        """Test that Config uses sensible defaults"""
        monkeypatch.chdir(tmp_path)
        # Clear any LZ_ environment variables
        for key in list(os.environ.keys()):
            if key.startswith('LZ_'):
                monkeypatch.delenv(key, raising=False)
        
        cfg = config.Config()
        
        assert cfg.log_dir == 'log'
        assert cfg.output_dir == 'output'
        assert cfg.input_dir == 'input'
    
    def test_config_from_yaml(self, tmp_path, monkeypatch):
        """Test that Config loads values from config.yaml"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("log_dir: custom_log\noutput_dir: custom_output\n")
        
        monkeypatch.chdir(tmp_path)
        cfg = config.Config()
        
        assert cfg.log_dir == 'custom_log'
        assert cfg.output_dir == 'custom_output'
    
    def test_config_from_config_subdir(self, tmp_path, monkeypatch):
        """Test that Config loads values from config/config.yaml"""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_file = config_dir / "config.yaml"
        config_file.write_text("log_dir: log_from_subdir\n")
        
        monkeypatch.chdir(tmp_path)
        cfg = config.Config()
        
        assert cfg.log_dir == 'log_from_subdir'
    
    def test_env_variable_override(self, tmp_path, monkeypatch):
        """Test that environment variables override config file values"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("log_dir: from_yaml\n")
        
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("LZ_LOG_DIR", "from_env")
        
        cfg = config.Config()
        
        assert cfg.log_dir == 'from_env'


class TestLoadConfig:
    """Test the Config.load_config method"""
    
    def test_load_config_applies_values(self, tmp_path, monkeypatch):
        """Test that load_config applies values from config file"""
        config_file = tmp_path / "custom.yaml"
        config_file.write_text("log_dir: custom_log_dir\n")
        
        monkeypatch.chdir(tmp_path)
        
        cfg = config.Config()
        cfg.load_config(config_file=str(config_file))
        
        assert cfg.log_dir == 'custom_log_dir'
    
    def test_load_config_with_overrides(self, tmp_path, monkeypatch):
        """Test that load_config accepts override values"""
        monkeypatch.chdir(tmp_path)
        
        cfg = config.Config()
        cfg.load_config(log_dir='override_log')
        
        assert cfg.log_dir == 'override_log'
