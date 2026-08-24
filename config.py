from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def _env_csv(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    scout_interval_minutes: int
    scout_on_startup: bool
    scout_startup_delay_seconds: int
    topic_ttl_hours: int
    max_topics_per_run: int
    candidates_per_source: int
    scout_sources: list[str]
    scout_categories: list[str]
    scout_categories_per_run: int

    google_news_lang: str
    google_news_country: str
    google_news_edition: str
    github_token: str | None
    user_agent: str

    llm_filter_enabled: bool
    openai_api_key: str | None
    filter_model: str

    mcp_auth_token: str | None
    mcp_allowed_hosts: list[str]
    mcp_allowed_origins: list[str]
    disable_dns_rebinding_protection: bool

    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = Path(os.getenv("DATA_DIR", "/data/topic-pool")).expanduser()
        db_path = Path(os.getenv("DB_PATH", str(data_dir / "topic_pool.sqlite3"))).expanduser()
        api_key = os.getenv("OPENAI_API_KEY") or None
        return cls(
            data_dir=data_dir,
            db_path=db_path,
            scout_interval_minutes=max(10, _env_int("SCOUT_INTERVAL_MINUTES", 360)),
            scout_on_startup=_env_bool("SCOUT_ON_STARTUP", True),
            scout_startup_delay_seconds=max(0, _env_int("SCOUT_STARTUP_DELAY_SECONDS", 20)),
            topic_ttl_hours=max(1, _env_int("TOPIC_TTL_HOURS", 36)),
            max_topics_per_run=max(1, min(10, _env_int("MAX_TOPICS_PER_RUN", 3))),
            candidates_per_source=max(3, min(30, _env_int("CANDIDATES_PER_SOURCE", 8))),
            scout_sources=_env_csv("SCOUT_SOURCES", "google_news,hacker_news,arxiv,github"),
            scout_categories=_env_csv(
                "SCOUT_CATEGORIES",
                "ai,technology,science,law,world,culture,weird",
            ),
            scout_categories_per_run=max(1, min(7, _env_int("SCOUT_CATEGORIES_PER_RUN", 3))),
            google_news_lang=os.getenv("GOOGLE_NEWS_LANG", "zh-CN"),
            google_news_country=os.getenv("GOOGLE_NEWS_COUNTRY", "CN"),
            google_news_edition=os.getenv("GOOGLE_NEWS_EDITION", "CN:zh-Hans"),
            github_token=os.getenv("GITHUB_TOKEN") or None,
            user_agent=os.getenv(
                "SCOUT_USER_AGENT",
                "TopicPoolMCP/1.0 (+personal-news-buffer)",
            ),
            llm_filter_enabled=_env_bool("LLM_FILTER_ENABLED", bool(api_key)),
            openai_api_key=api_key,
            filter_model=os.getenv("FILTER_MODEL", "gpt-5-mini"),
            mcp_auth_token=os.getenv("MCP_AUTH_TOKEN") or None,
            mcp_allowed_hosts=_env_csv("MCP_ALLOWED_HOSTS", ""),
            mcp_allowed_origins=_env_csv("MCP_ALLOWED_ORIGINS", ""),
            disable_dns_rebinding_protection=_env_bool(
                "MCP_DISABLE_DNS_REBINDING_PROTECTION", True
            ),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
