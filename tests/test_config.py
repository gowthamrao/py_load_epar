import os
from unittest.mock import patch

import pytest
import yaml
from pydantic import SecretStr

from py_load_epar.config import (
    DatabaseSettings,
    EtlSettings,
    Settings,
    SporApiSettings,
    StorageSettings,
    get_settings,
)


def test_default_settings():
    """Test that settings are loaded with default values."""
    settings = Settings()
    assert settings.db.user == "user"
    assert settings.etl.load_strategy == "DELTA"
    assert settings.spor_api.base_url == "https://sporify.eu"
    assert settings.storage.backend == "local"


@patch.dict(
    os.environ,
    {
        "DB__USER": "test_user",
        "ETL__LOAD_STRATEGY": "FULL",
        "STORAGE__BACKEND": "s3",
    },
)
def test_load_from_env_vars():
    """Test that settings are correctly loaded from environment variables."""
    settings = Settings()
    assert settings.db.user == "test_user"
    assert settings.etl.load_strategy == "FULL"
    assert settings.storage.backend == "s3"


def test_load_from_yaml(tmp_path):
    """Test that settings are correctly loaded from a YAML file."""
    config_content = {
        "db": {"host": "db.example.com", "port": 5433},
        "spor_api": {"username": "api_user"},
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    settings = Settings(config_path=str(config_file))

    assert settings.db.host == "db.example.com"
    assert settings.db.port == 5433
    assert settings.spor_api.username == "api_user"
    # Test that unset values remain default
    assert settings.db.user == "user"


@patch.dict(os.environ, {"DB__HOST": "env_host"})
def test_env_vars_override_yaml(tmp_path):
    """Test that environment variables take precedence over YAML file settings."""
    config_content = {"db": {"host": "yaml_host"}}
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    settings = Settings(config_path=str(config_file))

    assert settings.db.host == "env_host"


def test_database_dsn_property():
    """Test the DSN property of the DatabaseSettings."""
    db_settings = DatabaseSettings(
        user="test_user",
        password="test_password",
        host="test_host",
        port=1234,
        dbname="test_db",
    )
    expected_dsn = "postgresql://test_user:test_password@test_host:1234/test_db"
    assert db_settings.dsn == expected_dsn


def test_get_settings_factory(tmp_path):
    """Test the get_settings factory function."""
    config_file = tmp_path / "config.yaml"
    config_file.touch()

    # Test with a direct path
    settings = get_settings(config_path=str(config_file))
    assert isinstance(settings, Settings)
    assert settings.config_path == str(config_file)

    # Test with an environment variable
    with patch.dict(os.environ, {"PY_LOAD_EPAR_CONFIG_PATH": str(config_file)}):
        settings_from_env = get_settings()
        assert settings_from_env.config_path == str(config_file)
