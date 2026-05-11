"""Runtime configuration for the RO-ANIME backend.

The defaults here intentionally match the legacy constants from ``main.py``.
That lets us start moving toward a real application layout without changing
production behavior in the same step.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _first_present(*values: str | None) -> str:
    for value in values:
        cleaned = _clean(value)
        if cleaned:
            return cleaned
    return ""


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_database_url(url: str) -> str:
    cleaned = _clean(url)
    if cleaned.startswith("postgres://"):
        return "postgresql://" + cleaned[len("postgres://") :]
    return cleaned


@dataclass(frozen=True)
class Settings:
    mal_client_id: str
    mal_base: str
    anilist_gql: str
    megaplay_base: str
    vidwish_base: str
    cloudflare_proxy_base: str
    database_url: str
    sqlite_path: str
    sqlite_dir: str
    render_disk_dir: Path
    environment: str
    debug: bool

    @property
    def is_postgres(self) -> bool:
        return self.database_url.startswith("postgresql")


def load_settings() -> Settings:
    hardcoded_proxy = _clean(
        os.getenv(
            "HARDCODED_CLOUDFLARE_PROXY_BASE",
            "https://anime-tv-stream-proxy.kamuri-anime.workers.dev",
        )
    )

    return Settings(
        mal_client_id=_first_present(
            os.getenv("MAL_CLIENT_ID"),
            "b7718fb862ae30f4ba64bc9a4f90d2ea",
        ),
        mal_base=_first_present(os.getenv("MAL_BASE"), "https://api.myanimelist.net/v2"),
        anilist_gql=_first_present(os.getenv("ANILIST_GQL"), "https://graphql.anilist.co"),
        megaplay_base=_first_present(os.getenv("MEGAPLAY_BASE"), "https://megaplay.buzz"),
        vidwish_base=_first_present(os.getenv("VIDWISH_BASE"), "https://vidwish.live"),
        cloudflare_proxy_base=_first_present(
            hardcoded_proxy,
            os.getenv("CLOUDFLARE_PROXY_BASE"),
            os.getenv("RO_PROXY_BASE"),
        ).rstrip("/"),
        database_url=_normalize_database_url(os.getenv("DATABASE_URL", "")),
        sqlite_path=_clean(os.getenv("SQLITE_PATH")),
        sqlite_dir=_clean(os.getenv("SQLITE_DIR")),
        render_disk_dir=Path(_first_present(os.getenv("RENDER_DISK_DIR"), "/var/data")),
        environment=_first_present(os.getenv("ENVIRONMENT"), "development"),
        debug=_bool_env("DEBUG", False),
    )


settings = load_settings()
