"""Project configuration.

This module reads settings from environment variables. For local development, it also
loads a `.env` file from the repo root (if present).

IMPORTANT:
- Do NOT commit `.env` to git.
- In Hugging Face Spaces, set these values as Secrets.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


class SettingsError(RuntimeError):
    """Raised when required settings are missing or invalid."""


def _as_float(value: str, *, name: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise SettingsError(f"Environment variable {name} must be a number. Got: {value!r}") from exc


def _get_env(name: str, *, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    value = os.getenv(name, default)
    if required and (value is None or value.strip() == ""):
        raise SettingsError(
            f"Missing required environment variable: {name}. " 
            f"Add it to your .env file (local) or as a Secret (Hugging Face)."
        )
    return value


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the project."""

    tomtom_api_key: str
    tfl_app_key: str
    tfl_app_id: Optional[str]
    http_timeout_seconds: float
    user_agent: str


_SETTINGS: Optional[Settings] = None


def get_settings(*, reload: bool = False) -> Settings:
    """Load and return Settings (cached by default)."""

    global _SETTINGS
    if _SETTINGS is not None and not reload:
        return _SETTINGS

    # Load .env if available (does nothing if missing)
    load_dotenv()

    tomtom_api_key = _get_env("TOMTOM_API_KEY", required=True)  # type: ignore[assignment]
    tfl_app_key = _get_env("TFL_APP_KEY", required=True)  # type: ignore[assignment]
    tfl_app_id = _get_env("TFL_APP_ID", default=None, required=False)

    http_timeout_seconds_raw = _get_env("HTTP_TIMEOUT_SECONDS", default="30", required=False)
    http_timeout_seconds = _as_float(http_timeout_seconds_raw or "30", name="HTTP_TIMEOUT_SECONDS")

    user_agent = _get_env("USER_AGENT", default="london-traffic-forecast/0.1", required=False) or "london-traffic-forecast/0.1"

    _SETTINGS = Settings(
        tomtom_api_key=tomtom_api_key,  # type: ignore[arg-type]
        tfl_app_key=tfl_app_key,        # type: ignore[arg-type]
        tfl_app_id=tfl_app_id,
        http_timeout_seconds=http_timeout_seconds,
        user_agent=user_agent,
    )
    return _SETTINGS
