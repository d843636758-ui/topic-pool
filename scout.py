from __future__ import annotations

import asyncio
import logging
import random
import re
import uuid
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote_plus

import feedparser
import httpx

from config import Settings
from filter import TopicFilter
from topic_pool import TopicPool, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

CATEGORY_QUERIES = {
    "ai": "人工智能 OR AI OR 大模型 OR 机器人",
    "technology": "科技 OR 软件 OR 芯片 OR 开源",
    "science": "科学 OR 研究 OR 太空 OR 生物",
    "law": "法律 OR 法院 OR 立法 OR 司法 OR 数据合规",
    "world": "国际 OR 全球 OR 地缘 OR 外交",
    "culture": "文化 OR 设计 OR 媒体 OR 社会",
    "weird": "奇闻 OR 反常识 OR 有趣研究 OR 怪现象",
}

ARXIV_QUERIES = {
    "ai": "cat:cs.AI OR cat:cs.LG OR cat:cs.CL",
    "technology": "cat:cs.SE OR cat:cs.HC OR cat:cs.RO",
    "science": "cat:physics.pop-ph OR cat:q-bio.NC OR cat:astro-ph.GA",
}


def _iso_or_none(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        try:
            return parsedate_to_datetime(value).astimezone(timezone.utc).isoformat()
        except Exception:
            return None


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", text).strip()


class TopicScout:
    def __init__(
        self,
        settings: Settings,
        pool: TopicPool,
        http: httpx.AsyncClient,
        topic_filter: TopicFilter,
    ) -> None:
        self.settings = settings
        self.pool = pool
        self.http = http
        self.topic_filter = topic_filter
        self._run_lock = asyncio.Lock()

    async def run_once(self, categories: list[str] | None = None) -> dict[str, Any]:
        if self._run_lock.locked():
            return {"ok": False, "reason": "scout_already_running"}

        async with self._run_lock:
            run_id = await self.pool.begin_scout_run()
            candidates: list[dict[str, Any]] = []
            selected: list[dict[str, Any]] = []
            inserted = 0
            error: str | None = None
            try:
                chosen = self._choose_categories(categories)
                tasks = []
                sources = set(self.settings.scout_sources)
                if "google_news" in sources:
                    tasks.extend(self._fetch_google_news(category) for category in chosen)
                if "hacker_news" in sources:
                    tasks.append(self._fetch_hacker_news())
                if "arxiv" in sources:
                    for category in chosen:
                        if category in ARXIV_QUERIES:
                            tasks.append(self._fetch_arxiv(category))
                if "github" in sources:
                    tasks.append(self._fetch_github())

                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logger.warning("Scout source failed: %s", result)
                    else:
                        candidates.extend(result)

                fresh: list[dict[str, Any]] = []
                for c in candidates:
                    content_hash = stable_hash(c.get("title", ""), c.get("summary", ""))
                    if not await self.pool.has_source(c["source_id"], content_hash):
                        c["content_hash"] = content_hash
                        fresh.append(c)

                selected = await self.topic_filter.select(
                    fresh,
                    self.settings.max_topics_per_run,
                )
                now = datetime.now(timezone.utc)
                expires = (now + timedelta(hours=self.settings.topic_ttl_hours)).isoformat()
                for item in selected:
                    topic = {
                        "id": f"topic_{uuid.uuid4().hex[:16]}",
                        "source_id": item["source_id"],
                        "source_type": item["source_type"],
                        "source_title": item["title"],
                        "source_url": item["url"],
                        "source_name": item.get("source_name"),
                        "published_at": item.get("published_at"),
                        "observed_at": utc_now_iso(),
                        "expires_at": expires,
                        "category": item.get("category", "other"),
                        "summary": item.get("summary", ""),
                        "hook": item.get("hook", ""),
                        "score": item.get("score", 0.5),
                        "content_hash": item.get("content_hash")
                        or stable_hash(item.get("title", ""), item.get("summary", "")),
                        "metadata": item.get("metadata", {}),
                    }
                    if await self.pool.insert_topic(topic):
                        inserted += 1
                await self.pool.cleanup_expired()
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                logger.exception("Scout run failed")
            finally:
                await self.pool.finish_scout_run(
                    run_id,
                    candidate_count=len(candidates),
                    selected_count=len(selected),
                    inserted_count=inserted,
                    error=error,
                )

            return {
                "ok": error is None,
                "run_id": run_id,
                "categories": chosen,
                "candidate_count": len(candidates),
                "selected_count": len(selected),
                "inserted_count": inserted,
                "error": error,
            }

    def _choose_categories(self, categories: list[str] | None) -> list[str]:
        allowed = [c for c in self.settings.scout_categories if c]
        if categories:
            requested = [c.strip().lower() for c in categories if c.strip().lower() in allowed]
            return requested[: self.settings.scout_categories_per_run] or allowed[:1]
        k = min(len(allowed), self.settings.scout_categories_per_run)
        return random.sample(allowed, k=k) if allowed else ["ai"]

    async def _fetch_google_news(self, category: str) -> list[dict[str, Any]]:
        query = CATEGORY_QUERIES.get(category, category)
        url = (
            "https://news.google.com/rss/search?q="
            f"{quote_plus(query)}&hl={quote_plus(self.settings.google_news_lang)}"
            f"&gl={quote_plus(self.settings.google_news_country)}"
            f"&ceid={quote_plus(self.settings.google_news_edition)}"
        )
        response = await self.http.get(url, timeout=20)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        items: list[dict[str, Any]] = []
        for entry in feed.entries[: self.settings.candidates_per_source]:
            link = getattr(entry, "link", "")
            title = _strip_html(getattr(entry, "title", ""))
            summary = _strip_html(getattr(entry, "summary", ""))
            published = _iso_or_none(getattr(entry, "published", None))
            guid = str(getattr(entry, "id", "") or link)
            if not link or not title:
                continue
            items.append(
                {
                    "source_id": f"google_news:{stable_hash(guid)[:24]}",
                    "source_type": "google_news",
                    "source_name": "Google News RSS",
                    "title": title,
                    "url": link,
                    "summary": summary,
                    "published_at": published,
                    "category": category,
                    "metadata": {},
                }
            )
        return items

    async def _fetch_hacker_news(self) -> list[dict[str, Any]]:
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=72)).timestamp())
        url = "https://hn.algolia.com/api/v1/search_by_date"
        params = {
            "tags": "story",
            "hitsPerPage": self.settings.candidates_per_source,
            "numericFilters": f"created_at_i>{cutoff}",
        }
        response = await self.http.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        items: list[dict[str, Any]] = []
        for hit in data.get("hits", []):
            title = (hit.get("title") or "").strip()
            object_id = str(hit.get("objectID") or "")
            link = hit.get("url") or (f"https://news.ycombinator.com/item?id={object_id}" if object_id else "")
            if not title or not link:
                continue
            points = hit.get("points") or 0
            comments = hit.get("num_comments") or 0
            items.append(
                {
                    "source_id": f"hacker_news:{object_id or stable_hash(link)[:24]}",
                    "source_type": "hacker_news",
                    "source_name": "Hacker News",
                    "title": title,
                    "url": link,
                    "summary": f"Hacker News: {points} points, {comments} comments.",
                    "published_at": _iso_or_none(hit.get("created_at")),
                    "category": "technology",
                    "metadata": {"hn_points": points, "hn_comments": comments, "hn_id": object_id},
                }
            )
        return items

    async def _fetch_arxiv(self, category: str) -> list[dict[str, Any]]:
        query = ARXIV_QUERIES[category]
        params = {
            "search_query": query,
            "start": 0,
            "max_results": self.settings.candidates_per_source,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
        response = await self.http.get("https://export.arxiv.org/api/query", params=params, timeout=30)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        items: list[dict[str, Any]] = []
        for entry in feed.entries[: self.settings.candidates_per_source]:
            title = _strip_html(getattr(entry, "title", ""))
            link = str(getattr(entry, "link", "") or getattr(entry, "id", ""))
            arxiv_id = str(getattr(entry, "id", "") or link).rstrip("/").split("/")[-1]
            if not title or not link:
                continue
            items.append(
                {
                    "source_id": f"arxiv:{arxiv_id}",
                    "source_type": "arxiv",
                    "source_name": "arXiv",
                    "title": title,
                    "url": link,
                    "summary": _strip_html(getattr(entry, "summary", ""))[:2400],
                    "published_at": _iso_or_none(getattr(entry, "published", None)),
                    "category": category,
                    "metadata": {"arxiv_id": arxiv_id},
                }
            )
        return items

    async def _fetch_github(self) -> list[dict[str, Any]]:
        since = (datetime.now(timezone.utc) - timedelta(days=10)).date().isoformat()
        params = {
            "q": f"created:>{since} stars:>20",
            "sort": "stars",
            "order": "desc",
            "per_page": self.settings.candidates_per_source,
        }
        headers = {"Accept": "application/vnd.github+json"}
        if self.settings.github_token:
            headers["Authorization"] = f"Bearer {self.settings.github_token}"
        response = await self.http.get("https://api.github.com/search/repositories", params=params, headers=headers, timeout=20)
        if response.status_code == 403:
            logger.warning("GitHub source rate limited; set GITHUB_TOKEN for a higher limit")
            return []
        response.raise_for_status()
        data = response.json()
        items: list[dict[str, Any]] = []
        for repo in data.get("items", [])[: self.settings.candidates_per_source]:
            name = repo.get("full_name") or repo.get("name")
            link = repo.get("html_url")
            if not name or not link:
                continue
            desc = (repo.get("description") or "").strip()
            stars = repo.get("stargazers_count") or 0
            lang = repo.get("language") or "unknown"
            items.append(
                {
                    "source_id": f"github:{repo.get('id') or stable_hash(link)[:24]}",
                    "source_type": "github",
                    "source_name": "GitHub",
                    "title": f"{name} — new repository gaining attention",
                    "url": link,
                    "summary": f"{desc} Stars: {stars}. Primary language: {lang}.",
                    "published_at": _iso_or_none(repo.get("created_at")),
                    "category": "technology",
                    "metadata": {"stars": stars, "language": lang, "repo": name},
                }
            )
        return items
