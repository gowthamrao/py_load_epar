import os
from typing import Any, Optional

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Models database connection settings."""

    type: str = "postgresql"
    user: str = "user"
    password: str = "password"
    host: str = "localhost"
    port: int = 5432
    dbname: str = "epar_db"

    # Pydantic-settings will automatically look for environment variables
    # with this prefix, e.g., PY_LOAD_EPAR_DB_PASSWORD
    model_config = SettingsConfigDict(env_prefix="PY_LOAD_EPAR_DB_")

    @property
    def dsn(self) -> str:
        """Data Source Name for connecting to the database."""
        return f"{self.type}://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"


class ApiSettings(BaseSettings):
    """Models settings for external APIs."""

    ema_base_url: str = "https://www.ema.europa.eu"
    spor_api_url: str = "https://spor.ema.europa.eu/rmswi/api"
    ema_file_url: str = Field(
        default="https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx",
        description="URL to the main EMA EPAR index file (Excel/CSV).",
    )

    model_config = SettingsConfigDict(env_prefix="PY_LOAD_EPAR_API_")


class EtlSettings(BaseSettings):
    """Models settings for the ETL process."""

    load_strategy: str = "DELTA"  # or "FULL"
    batch_size: int = 1000
    max_retries: int = 5
    document_storage_path: str = "epar_documents"

    model_config = SettingsConfigDict(env_prefix="PY_LOAD_EPAR_ETL_")


class Settings(BaseSettings):
    """Main settings container."""

    db: DatabaseSettings = DatabaseSettings()
    api: ApiSettings = ApiSettings()
    etl: EtlSettings = EtlSettings()

    # Optional path to a YAML config file
    config_path: Optional[str] = None

    def __init__(self, config_path: Optional[str] = None, **values: Any):
        super().__init__(**values)
        if config_path:
            self.config_path = config_path
        elif os.environ.get("PY_LOAD_EPAR_CONFIG_PATH"):
            self.config_path = os.environ.get("PY_LOAD_EPAR_CONFIG_PATH")

        if self.config_path and os.path.exists(self.config_path):
            self._load_from_yaml()

    def _load_from_yaml(self) -> None:
        """Loads and merges settings from a YAML file."""
        if not self.config_path:
            return

        with open(self.config_path, "r") as f:
            yaml_config = yaml.safe_load(f)

        if not yaml_config:
            return

        # Update nested settings models
        if "db" in yaml_config:
            self.db = self.db.model_copy(update=yaml_config["db"])
        if "api" in yaml_config:
            self.api = self.api.model_copy(update=yaml_config["api"])
        if "etl" in yaml_config:
            self.etl = self.etl.model_copy(update=yaml_config["etl"])

    model_config = SettingsConfigDict(env_nested_delimiter="__")


def get_settings(config_path: Optional[str] = None) -> Settings:
    """Factory function to get the settings."""
    return Settings(config_path=config_path)


# Example usage:
# settings = get_settings("config.yaml")
# print(settings.db.dsn)
