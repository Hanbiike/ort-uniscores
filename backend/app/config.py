"""Application settings loaded from environment variables."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for API and ETL."""

    database_url: str = (
        "mysql+pymysql://user:password@127.0.0.1:3306/"
        "ort_uniscores?charset=utf8mb4"
    )
    site_root: Path = Path("..")
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
