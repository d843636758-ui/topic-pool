from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Awaitable
from urllib.parse import quote_plus

import feedparser
import httpx

from filter import TopicFilter
from settings import Settings
from topic_pool import TopicPool, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

# Broad Chinese-language queries. Hotlists supply the fast-moving "trending" signal;
# Google News supplies category-specific material and source links.
CATEGORY_QUERIES = {
    "trending": "热搜 OR 热议 OR 热点 OR 话题",
    "society": "社会 OR 民生 OR 通报 OR 公共事件 OR 城市新闻",
    "entertainment": "娱乐 OR 影视 OR 明星 OR 综艺 OR 音乐",
    "finance": "财经 OR 经济 OR 金融 OR 股市 OR 公司",
    "sports": "体育 OR 赛事 OR 比赛 OR 运动员",
    "education": "教育 OR 高校 OR 学校 OR 考试 OR 学生",
    "health": "医疗 OR 健康 OR 医院 OR 疾病 OR 公共卫生",
    "consumer": "消费者 OR 维权 OR 食品安全 OR 产品召回 OR 品牌",
    "ai": "人工智能 OR AI OR 大模型 OR 机器人",
    "technology": "科技 OR 软件 OR 芯片 OR 开源 OR 互联网",
    "science": "科学 OR 研究 OR 太空 OR 生物 OR 物理",
    "law": "法律 OR 法院 OR 立法 OR 司法 OR 检察 OR 数据合规",
    "domestic": "中国 OR 国内 OR 政策 OR 地方新闻",
    "world": "国际 OR 全球 OR 外交 OR 海外 OR 地缘",
    "culture": "文化 OR 设计 OR 文学 OR 媒体 OR 展览",
    "weird": "奇闻 OR 反常识 OR 有趣研究 OR 怪现象",
}

ARXIV_QUERIES = {
    "ai": "cat:cs.AI OR cat:cs.LG OR cat:cs.CL",
    "technology": "cat:cs.SE OR cat:cs.HC OR cat:cs.RO",
    "science": "cat:physics.pop-ph OR cat:q-bio.NC OR cat:astro-ph.GA",
}

TECH_CATEGORIES = {"ai", "technology", "science"}


def _iso_or_none(value: str | None) -> str | None:
    if not value:
        return None

    try:
        return (
            datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
            .astimezone(timezone.utc)
            .isoformat()
        )
    except Exception:
        try:
            return (
                parsedate_to_datetime(value)
                .astimezone(timezone.utc)
                .isoformat()
            )
        except Exception:
            return None


def _strip_html(text: str) -> str:
    text = re.sub(
        r"<[^>]+>",
        " ",
        text or "",
    )

    return re.sub(
        r"\s+",
        " ",
        text,
    ).strip()


def _intish(value: Any) -> int | None:
    if value is None:
        return None

    match = re.search(
        r"\d+",
        str(value).replace(",", ""),
    )

    if not match:
        return None

    try:
        return int(match.group(0))
    except ValueError:
        return None


def _hot_summary(
    source: str,
    rank: int,
    hot_value: Any = None,
    desc: str = "",
) -> str:
    parts = [
        f"{source}热榜第 {rank} 位"
    ]

    hot = _intish(hot_value)

    if hot is not None and hot > 0:
        parts.append(
            f"热度 {hot}"
        )

    if desc:
        parts.append(
            _strip_html(desc)[:500]
        )

    return "；".join(parts) + "。"


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
        self._rotation_cursor = 0

    async def run_once(
        self,
        categories: list[str] | None = None,
    ) -> dict[str, Any]:

        if self._run_lock.locked():
            return {
                "ok": False,
                "reason": "scout_already_running",
            }

        async with self._run_lock:
            run_id = await self.pool.begin_scout_run()

            candidates: list[dict[str, Any]] = []
            selected: list[dict[str, Any]] = []

            inserted = 0
            error: str | None = None

            source_errors: list[
                dict[str, str]
            ] = []

            chosen: list[str] = []

            try:
                chosen = self._choose_categories(
                    categories
                )

                sources = set(
                    self.settings.scout_sources
                )

                tasks: list[
                    tuple[
                        str,
                        Awaitable[
                            list[
                                dict[str, Any]
                            ]
                        ],
                    ]
                ] = []

                # 国内热榜只在 trending 分类出现时抓取。
                # 自动巡逻因为固定包含 trending，所以会自动抓。
                if "trending" in chosen:
                    if "baidu_hot" in sources:
                        tasks.append(
                            (
                                "baidu_hot",
                                self._fetch_baidu_hot(),
                            )
                        )

                    if "weibo_hot" in sources:
                        tasks.append(
                            (
                                "weibo_hot",
                                self._fetch_weibo_hot(),
                            )
                        )

                    if "toutiao_hot" in sources:
                        tasks.append(
                            (
                                "toutiao_hot",
                                self._fetch_toutiao_hot(),
                            )
                        )

                # Google News 按当前选择的每个分类分别抓取。
                if "google_news" in sources:
                    tasks.extend(
                        (
                            f"google_news:{category}",
                            self._fetch_google_news(
                                category
                            ),
                        )
                        for category in chosen
                    )

                # HN / arXiv / GitHub 只在科技类轮次加入。
                # 避免每轮社会新闻和吃瓜局都被技术新闻淹没。
                if TECH_CATEGORIES.intersection(
                    chosen
                ):
                    if "hacker_news" in sources:
                        tasks.append(
                            (
                                "hacker_news",
                                self._fetch_hacker_news(),
                            )
                        )

                    if "arxiv" in sources:
                        for category in chosen:
                            if (
                                category
                                in ARXIV_QUERIES
                            ):
                                tasks.append(
                                    (
                                        f"arxiv:{category}",
                                        self._fetch_arxiv(
                                            category
                                        ),
                                    )
                                )

                    if "github" in sources:
                        tasks.append(
                            (
                                "github",
                                self._fetch_github(),
                            )
                        )

                if tasks:
                    results = await asyncio.gather(
                        *(
                            coro
                            for _, coro
                            in tasks
                        ),
                        return_exceptions=True,
                    )

                    for (
                        label,
                        _,
                    ), result in zip(
                        tasks,
                        results,
                    ):
                        if isinstance(
                            result,
                            Exception,
                        ):
                            msg = (
                                f"{type(result).__name__}: "
                                f"{result}"
                            )

                            logger.warning(
                                "Scout source failed [%s]: %s",
                                label,
                                msg,
                            )

                            source_errors.append(
                                {
                                    "source": label,
                                    "error": msg,
                                }
                            )

                        else:
                            candidates.extend(
                                result
                            )

                # 数据库级去重。
                fresh: list[
                    dict[str, Any]
                ] = []

                for candidate in candidates:
                    content_hash = stable_hash(
                        candidate.get(
                            "title",
                            "",
                        ),
                        candidate.get(
                            "summary",
                            "",
                        ),
                    )

                    exists = (
                        await self.pool.has_source(
                            candidate[
                                "source_id"
                            ],
                            content_hash,
                        )
                    )

                    if not exists:
                        item = dict(
                            candidate
                        )

                        item[
                            "content_hash"
                        ] = content_hash

                        fresh.append(
                            item
                        )

                selected = (
                    await self.topic_filter.select(
                        fresh,
                        self.settings
                        .max_topics_per_run,
                    )
                )

                now = datetime.now(
                    timezone.utc
                )

                expires = (
                    now
                    + timedelta(
                        hours=self.settings
                        .topic_ttl_hours
                    )
                ).isoformat()

                for item in selected:
                    topic = {
                        "id": (
                            "topic_"
                            + uuid.uuid4().hex[
                                :16
                            ]
                        ),
                        "source_id": item[
                            "source_id"
                        ],
                        "source_type": item[
                            "source_type"
                        ],
                        "source_title": item[
                            "title"
                        ],
                        "source_url": item[
                            "url"
                        ],
                        "source_name": item.get(
                            "source_name"
                        ),
                        "published_at": item.get(
                            "published_at"
                        ),
                        "observed_at": (
                            utc_now_iso()
                        ),
                        "expires_at": expires,
                        "category": item.get(
                            "category",
                            "other",
                        ),
                        "summary": item.get(
                            "summary",
                            "",
                        ),
                        "hook": item.get(
                            "hook",
                            "",
                        ),
                        "score": item.get(
                            "score",
                            0.5,
                        ),
                        "content_hash": (
                            item.get(
                                "content_hash"
                            )
                            or stable_hash(
                                item.get(
                                    "title",
                                    "",
                                ),
                                item.get(
                                    "summary",
                                    "",
                                ),
                            )
                        ),
                        "metadata": item.get(
                            "metadata",
                            {},
                        ),
                    }

                    if await self.pool.insert_topic(
                        topic
                    ):
                        inserted += 1

                await self.pool.cleanup_expired()

                # 单个公开热榜挂掉是正常情况。
                # 只有所有本轮来源都挂了，才把整轮标记失败。
                if (
                    tasks
                    and not candidates
                    and len(source_errors)
                    == len(tasks)
                ):
                    error = (
                        "all_sources_failed"
                    )

            except Exception as exc:
                error = (
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )

                logger.exception(
                    "Scout run failed"
                )

            finally:
                await self.pool.finish_scout_run(
                    run_id,
                    candidate_count=len(
                        candidates
                    ),
                    selected_count=len(
                        selected
                    ),
                    inserted_count=inserted,
                    error=error,
                )

            return {
                "ok": error is None,
                "run_id": run_id,
                "categories": chosen,
                "candidate_count": len(
                    candidates
                ),
                "selected_count": len(
                    selected
                ),
                "inserted_count": inserted,
                "source_errors": (
                    source_errors
                ),
                "error": error,
            }

    def _choose_categories(
        self,
        categories: list[str] | None,
    ) -> list[str]:

        allowed: list[str] = []

        for category in (
            self.settings.scout_categories
        ):
            c = (
                category
                .strip()
                .lower()
            )

            if (
                c
                and c not in allowed
            ):
                allowed.append(
                    c
                )

        if not allowed:
            return [
                "trending"
            ]

        limit = min(
            len(allowed),
            self.settings
            .scout_categories_per_run,
        )

        # 手动调用：
        # scout_now(categories=[...])
        # 就严格尊重调用方指定分类。
        # pinned 只用于自动巡逻。
        if categories:
            requested: list[str] = []

            for category in categories:
                c = (
                    category
                    .strip()
                    .lower()
                )

                if (
                    c in allowed
                    and c not in requested
                ):
                    requested.append(
                        c
                    )

            return (
                requested[:limit]
                or allowed[:1]
            )

        # 自动巡逻固定分类。
        pinned: list[str] = []

        for category in (
            self.settings
            .scout_pinned_categories
        ):
            c = (
                category
                .strip()
                .lower()
            )

            if (
                c in allowed
                and c not in pinned
            ):
                pinned.append(
                    c
                )

        pinned = pinned[:limit]

        slots = max(
            0,
            limit - len(pinned),
        )

        rotating = [
            c
            for c in allowed
            if c not in pinned
        ]

        chosen = list(
            pinned
        )

        if (
            slots
            and rotating
        ):
            for offset in range(
                slots
            ):
                chosen.append(
                    rotating[
                        (
                            self._rotation_cursor
                            + offset
                        )
                        % len(rotating)
                    ]
                )

            self._rotation_cursor = (
                self._rotation_cursor
                + slots
            ) % len(rotating)

        return (
            chosen
            or allowed[:1]
        )

    async def _fetch_baidu_hot(
        self,
    ) -> list[dict[str, Any]]:

        url = (
            "https://top.baidu.com/"
            "board?tab=realtime"
        )

        response = await self.http.get(
            url,
            headers={
                "Referer": (
                    "https://top.baidu.com/"
                )
            },
            timeout=20,
        )

        response.raise_for_status()

        match = re.search(
            r"<!--s-data:(.*?)-->",
            response.text,
            flags=re.S,
        )

        if not match:
            raise ValueError(
                "Baidu hot board "
                "payload not found"
            )

        payload = json.loads(
            match.group(1)
        )

        cards = (
            (
                payload.get(
                    "data"
                )
                or {}
            ).get(
                "cards"
            )
            or payload.get(
                "cards"
            )
            or []
        )

        if not cards:
            return []

        content = (
            cards[0].get(
                "content"
            )
            or []
        )

        if (
            content
            and isinstance(
                content[0],
                dict,
            )
            and isinstance(
                content[0].get(
                    "content"
                ),
                list,
            )
        ):
            content = content[0][
                "content"
            ]

        items: list[
            dict[str, Any]
        ] = []

        for index, row in enumerate(
            content[
                : self.settings
                .candidates_per_source
            ],
            start=1,
        ):
            if not isinstance(
                row,
                dict,
            ):
                continue

            title = str(
                row.get(
                    "word"
                )
                or row.get(
                    "title"
                )
                or ""
            ).strip()

            if not title:
                continue

            query = str(
                row.get(
                    "query"
                )
                or title
            ).strip()

            link = str(
                row.get(
                    "rawUrl"
                )
                or row.get(
                    "url"
                )
                or ""
            ).strip()

            if not link.startswith(
                "http"
            ):
                link = (
                    "https://www.baidu.com/"
                    "s?wd="
                    + quote_plus(
                        query
                    )
                )

            hot = (
                row.get(
                    "hotScore"
                )
                or row.get(
                    "hotTag"
                )
            )

            desc = str(
                row.get(
                    "desc"
                )
                or ""
            ).strip()

            rank = (
                _intish(
                    row.get(
                        "index"
                    )
                )
                or index
            )

            items.append(
                {
                    "source_id": (
                        "baidu_hot:"
                        + stable_hash(
                            query
                        )[:24]
                    ),
                    "source_type": (
                        "baidu_hot"
                    ),
                    "source_name": (
                        "百度热搜"
                    ),
                    "title": title,
                    "url": link,
                    "summary": (
                        _hot_summary(
                            "百度",
                            rank,
                            hot,
                            desc,
                        )
                    ),
                    "published_at": None,
                    "category": (
                        "trending"
                    ),
                    "metadata": {
                        "rank": rank,
                        "hot_value": (
                            _intish(
                                hot
                            )
                        ),
                        "author": (
                            row.get(
                                "show"
                            )
                        ),
                    },
                }
            )

        return items

    async def _fetch_weibo_hot(
        self,
    ) -> list[dict[str, Any]]:

        url = (
            "https://weibo.com/"
            "ajax/side/hotSearch"
        )

        response = await self.http.get(
            url,
            headers={
                "Accept": (
                    "application/json, "
                    "text/plain, */*"
                ),
                "Referer": (
                    "https://s.weibo.com/"
                    "top/summary"
                ),
            },
            timeout=20,
        )

        response.raise_for_status()

        payload = response.json()

        realtime = (
            (
                payload.get(
                    "data"
                )
                or {}
            ).get(
                "realtime"
            )
            or []
        )

        if not isinstance(
            realtime,
            list,
        ):
            raise ValueError(
                "Weibo hot search "
                "payload missing "
                "realtime list"
            )

        items: list[
            dict[str, Any]
        ] = []

        for index, row in enumerate(
            realtime[
                : self.settings
                .candidates_per_source
            ],
            start=1,
        ):
            if not isinstance(
                row,
                dict,
            ):
                continue

            title = str(
                row.get(
                    "note"
                )
                or row.get(
                    "word"
                )
                or ""
            ).strip()

            if (
                not title
                or row.get(
                    "is_ad"
                )
            ):
                continue

            scheme = str(
                row.get(
                    "word_scheme"
                )
                or title
            ).strip()

            link = (
                "https://s.weibo.com/"
                "weibo?q="
                + quote_plus(
                    scheme
                )
            )

            hot = (
                row.get(
                    "num"
                )
                or row.get(
                    "raw_hot"
                )
            )

            rank = (
                _intish(
                    row.get(
                        "rank"
                    )
                )
                or index
            )

            items.append(
                {
                    "source_id": (
                        "weibo_hot:"
                        + stable_hash(
                            title
                        )[:24]
                    ),
                    "source_type": (
                        "weibo_hot"
                    ),
                    "source_name": (
                        "微博热搜"
                    ),
                    "title": title,
                    "url": link,
                    "summary": (
                        _hot_summary(
                            "微博",
                            rank,
                            hot,
                        )
                    ),
                    "published_at": None,
                    "category": (
                        "trending"
                    ),
                    "metadata": {
                        "rank": rank,
                        "hot_value": (
                            _intish(
                                hot
                            )
                        ),
                        "label": (
                            row.get(
                                "label_name"
                            )
                        ),
                        "flag": (
                            row.get(
                                "flag_desc"
                            )
                        ),
                    },
                }
            )

        return items

    async def _fetch_toutiao_hot(
        self,
    ) -> list[dict[str, Any]]:

        url = (
            "https://www.toutiao.com/"
            "hot-event/hot-board/"
            "?origin=toutiao_pc"
        )

        response = await self.http.get(
            url,
            headers={
                "Referer": (
                    "https://www.toutiao.com/"
                )
            },
            timeout=20,
        )

        response.raise_for_status()

        payload = response.json()

        rows = (
            payload.get(
                "data"
            )
            or []
        )

        if not isinstance(
            rows,
            list,
        ):
            raise ValueError(
                "Toutiao hot board "
                "payload missing "
                "data list"
            )

        items: list[
            dict[str, Any]
        ] = []

        for index, row in enumerate(
            rows[
                : self.settings
                .candidates_per_source
            ],
            start=1,
        ):
            if not isinstance(
                row,
                dict,
            ):
                continue

            title = str(
                row.get(
                    "Title"
                )
                or row.get(
                    "title"
                )
                or ""
            ).strip()

            if not title:
                continue

            cluster_id = str(
                row.get(
                    "ClusterIdStr"
                )
                or row.get(
                    "ClusterId"
                )
                or row.get(
                    "cluster_id"
                )
                or ""
            ).strip()

            link = str(
                row.get(
                    "Url"
                )
                or row.get(
                    "url"
                )
                or ""
            ).strip()

            if (
                not link.startswith(
                    "http"
                )
                and cluster_id
            ):
                link = (
                    "https://www.toutiao.com/"
                    f"trending/{cluster_id}/"
                )

            if not link.startswith(
                "http"
            ):
                link = (
                    "https://www.toutiao.com/"
                    "search/?keyword="
                    + quote_plus(
                        title
                    )
                )

            hot = (
                row.get(
                    "HotValue"
                )
                or row.get(
                    "hot_value"
                )
            )

            items.append(
                {
                    "source_id": (
                        "toutiao_hot:"
                        + (
                            cluster_id
                            or stable_hash(
                                title
                            )[:24]
                        )
                    ),
                    "source_type": (
                        "toutiao_hot"
                    ),
                    "source_name": (
                        "今日头条热榜"
                    ),
                    "title": title,
                    "url": link,
                    "summary": (
                        _hot_summary(
                            "今日头条",
                            index,
                            hot,
                        )
                    ),
                    "published_at": None,
                    "category": (
                        "trending"
                    ),
                    "metadata": {
                        "rank": index,
                        "hot_value": (
                            _intish(
                                hot
                            )
                        ),
                        "cluster_id": (
                            cluster_id
                            or None
                        ),
                    },
                }
            )

        return items

    async def _fetch_google_news(
        self,
        category: str,
    ) -> list[dict[str, Any]]:

        query = CATEGORY_QUERIES.get(
            category,
            category,
        )

        url = (
            "https://news.google.com/"
            "rss/search?q="
            f"{quote_plus(query)}"
            f"&hl={quote_plus(self.settings.google_news_lang)}"
            f"&gl={quote_plus(self.settings.google_news_country)}"
            f"&ceid={quote_plus(self.settings.google_news_edition)}"
        )

        response = await self.http.get(
            url,
            timeout=20,
        )

        response.raise_for_status()

        feed = feedparser.parse(
            response.content
        )

        items: list[
            dict[str, Any]
        ] = []

        for entry in feed.entries[
            : self.settings
            .candidates_per_source
        ]:
            link = getattr(
                entry,
                "link",
                "",
            )

            title = _strip_html(
                getattr(
                    entry,
                    "title",
                    "",
                )
            )

            summary = _strip_html(
                getattr(
                    entry,
                    "summary",
                    "",
                )
            )

            published = (
                _iso_or_none(
                    getattr(
                        entry,
                        "published",
                        None,
                    )
                )
            )

            guid = str(
                getattr(
                    entry,
                    "id",
                    "",
                )
                or link
            )

            if (
                not link
                or not title
            ):
                continue

            items.append(
                {
                    "source_id": (
                        "google_news:"
                        + stable_hash(
                            guid
                        )[:24]
                    ),
                    "source_type": (
                        "google_news"
                    ),
                    "source_name": (
                        "Google News RSS"
                    ),
                    "title": title,
                    "url": link,
                    "summary": summary,
                    "published_at": (
                        published
                    ),
                    "category": category,
                    "metadata": {},
                }
            )

        return items

    async def _fetch_hacker_news(
        self,
    ) -> list[dict[str, Any]]:

        cutoff = int(
            (
                datetime.now(
                    timezone.utc
                )
                - timedelta(
                    hours=72
                )
            ).timestamp()
        )

        url = (
            "https://hn.algolia.com/"
            "api/v1/search_by_date"
        )

        params = {
            "tags": "story",
            "hitsPerPage": (
                self.settings
                .candidates_per_source
            ),
            "numericFilters": (
                f"created_at_i>{cutoff}"
            ),
        }

        response = await self.http.get(
            url,
            params=params,
            timeout=20,
        )

        response.raise_for_status()

        data = response.json()

        items: list[
            dict[str, Any]
        ] = []

        for hit in data.get(
            "hits",
            [],
        ):
            title = (
                hit.get(
                    "title"
                )
                or ""
            ).strip()

            object_id = str(
                hit.get(
                    "objectID"
                )
                or ""
            )

            link = (
                hit.get(
                    "url"
                )
                or (
                    "https://news.ycombinator.com/"
                    f"item?id={object_id}"
                    if object_id
                    else ""
                )
            )

            if (
                not title
                or not link
            ):
                continue

            points = (
                hit.get(
                    "points"
                )
                or 0
            )

            comments = (
                hit.get(
                    "num_comments"
                )
                or 0
            )

            items.append(
                {
                    "source_id": (
                        "hacker_news:"
                        + (
                            object_id
                            or stable_hash(
                                link
                            )[:24]
                        )
                    ),
                    "source_type": (
                        "hacker_news"
                    ),
                    "source_name": (
                        "Hacker News"
                    ),
                    "title": title,
                    "url": link,
                    "summary": (
                        "Hacker News: "
                        f"{points} points, "
                        f"{comments} comments."
                    ),
                    "published_at": (
                        _iso_or_none(
                            hit.get(
                                "created_at"
                            )
                        )
                    ),
                    "category": (
                        "technology"
                    ),
                    "metadata": {
                        "hn_points": (
                            points
                        ),
                        "hn_comments": (
                            comments
                        ),
                        "hn_id": (
                            object_id
                        ),
                    },
                }
            )

        return items

    async def _fetch_arxiv(
        self,
        category: str,
    ) -> list[dict[str, Any]]:

        query = ARXIV_QUERIES[
            category
        ]

        params = {
            "search_query": query,
            "start": 0,
            "max_results": (
                self.settings
                .candidates_per_source
            ),
            "sortBy": (
                "submittedDate"
            ),
            "sortOrder": (
                "descending"
            ),
        }

        response = await self.http.get(
            "https://export.arxiv.org/api/query",
            params=params,
            timeout=30,
        )

        response.raise_for_status()

        feed = feedparser.parse(
            response.content
        )

        items: list[
            dict[str, Any]
        ] = []

        for entry in feed.entries[
            : self.settings
            .candidates_per_source
        ]:
            title = _strip_html(
                getattr(
                    entry,
                    "title",
                    "",
                )
            )

            link = str(
                getattr(
                    entry,
                    "link",
                    "",
                )
                or getattr(
                    entry,
                    "id",
                    "",
                )
            )

            arxiv_id = (
                str(
                    getattr(
                        entry,
                        "id",
                        "",
                    )
                    or link
                )
                .rstrip("/")
                .split("/")[-1]
            )

            if (
                not title
                or not link
            ):
                continue

            items.append(
                {
                    "source_id": (
                        f"arxiv:{arxiv_id}"
                    ),
                    "source_type": (
                        "arxiv"
                    ),
                    "source_name": (
                        "arXiv"
                    ),
                    "title": title,
                    "url": link,
                    "summary": (
                        _strip_html(
                            getattr(
                                entry,
                                "summary",
                                "",
                            )
                        )[:2400]
                    ),
                    "published_at": (
                        _iso_or_none(
                            getattr(
                                entry,
                                "published",
                                None,
                            )
                        )
                    ),
                    "category": category,
                    "metadata": {
                        "arxiv_id": (
                            arxiv_id
                        )
                    },
                }
            )

        return items

    async def _fetch_github(
        self,
    ) -> list[dict[str, Any]]:

        since = (
            datetime.now(
                timezone.utc
            )
            - timedelta(
                days=10
            )
        ).date().isoformat()

        params = {
            "q": (
                f"created:>{since} "
                "stars:>20"
            ),
            "sort": "stars",
            "order": "desc",
            "per_page": (
                self.settings
                .candidates_per_source
            ),
        }

        headers = {
            "Accept": (
                "application/vnd.github+json"
            )
        }

        if self.settings.github_token:
            headers[
                "Authorization"
            ] = (
                "Bearer "
                + self.settings
                .github_token
            )

        response = await self.http.get(
            "https://api.github.com/"
            "search/repositories",
            params=params,
            headers=headers,
            timeout=20,
        )

        if response.status_code == 403:
            logger.warning(
                "GitHub source rate limited; "
                "set GITHUB_TOKEN for a higher limit"
            )

            return []

        response.raise_for_status()

        data = response.json()

        items: list[
            dict[str, Any]
        ] = []

        for repo in data.get(
            "items",
            [],
        )[
            : self.settings
            .candidates_per_source
        ]:
            name = (
                repo.get(
                    "full_name"
                )
                or repo.get(
                    "name"
                )
            )

            link = repo.get(
                "html_url"
            )

            if (
                not name
                or not link
            ):
                continue

            desc = (
                repo.get(
                    "description"
                )
                or ""
            ).strip()

            stars = (
                repo.get(
                    "stargazers_count"
                )
                or 0
            )

            lang = (
                repo.get(
                    "language"
                )
                or "unknown"
            )

            items.append(
                {
                    "source_id": (
                        "github:"
                        + str(
                            repo.get(
                                "id"
                            )
                            or stable_hash(
                                link
                            )[:24]
                        )
                    ),
                    "source_type": (
                        "github"
                    ),
                    "source_name": (
                        "GitHub"
                    ),
                    "title": (
                        f"{name} — "
                        "new repository "
                        "gaining attention"
                    ),
                    "url": link,
                    "summary": (
                        f"{desc} "
                        f"Stars: {stars}. "
                        "Primary language: "
                        f"{lang}."
                    ),
                    "published_at": (
                        _iso_or_none(
                            repo.get(
                                "created_at"
                            )
                        )
                    ),
                    "category": (
                        "technology"
                    ),
                    "metadata": {
                        "stars": (
                            stars
                        ),
                        "language": (
                            lang
                        ),
                        "repo": name,
                    },
                }
            )

        return items
