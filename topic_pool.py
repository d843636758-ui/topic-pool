from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(*parts: str) -> str:
    payload = "\n".join((p or "").strip().lower() for p in parts)
    return hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()


class TopicPool:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self.db = await aiosqlite.connect(self.db_path)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL;")
        await self.db.execute("PRAGMA foreign_keys=ON;")
        await self._init_schema()

    async def close(self) -> None:
        if self.db is not None:
            await self.db.close()
            self.db = None

    def _conn(self) -> aiosqlite.Connection:
        if self.db is None:
            raise RuntimeError("TopicPool database is not connected")
        return self.db

    async def _init_schema(self) -> None:
        db = self._conn()
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS topics (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL UNIQUE,
                source_type TEXT NOT NULL,
                source_title TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_name TEXT,
                published_at TEXT,
                observed_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                category TEXT NOT NULL,
                summary TEXT NOT NULL,
                hook TEXT NOT NULL,
                score REAL NOT NULL DEFAULT 0,
                content_hash TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'open',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_topics_status_expiry
            ON topics(status, expires_at);

            CREATE INDEX IF NOT EXISTS idx_topics_category_score
            ON topics(category, score DESC);

            CREATE TABLE IF NOT EXISTS consumer_states (
                topic_id TEXT NOT NULL,
                consumer_id TEXT NOT NULL,
                state TEXT NOT NULL,
                note TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(topic_id, consumer_id),
                FOREIGN KEY(topic_id) REFERENCES topics(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_consumer_states_consumer
            ON consumer_states(consumer_id, state);

            CREATE TABLE IF NOT EXISTS interests (
                consumer_id TEXT PRIMARY KEY,
                categories_json TEXT NOT NULL DEFAULT '[]',
                keywords_json TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scout_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                candidate_count INTEGER NOT NULL DEFAULT 0,
                selected_count INTEGER NOT NULL DEFAULT 0,
                inserted_count INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );
            """
        )
        await db.commit()

    async def cleanup_expired(self) -> int:
        db = self._conn()
        now = utc_now_iso()
        cur = await db.execute(
            "UPDATE topics SET status='expired' WHERE status='open' AND expires_at <= ?",
            (now,),
        )
        await db.commit()
        return cur.rowcount

    async def has_source(self, source_id: str, content_hash: str) -> bool:
        db = self._conn()
        cur = await db.execute(
            "SELECT 1 FROM topics WHERE source_id=? OR content_hash=? LIMIT 1",
            (source_id, content_hash),
        )
        return await cur.fetchone() is not None

    async def insert_topic(self, topic: dict[str, Any]) -> bool:
        db = self._conn()
        try:
            await db.execute(
                """
                INSERT INTO topics (
                    id, source_id, source_type, source_title, source_url, source_name,
                    published_at, observed_at, expires_at, category, summary, hook,
                    score, content_hash, status, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
                """,
                (
                    topic["id"],
                    topic["source_id"],
                    topic["source_type"],
                    topic["source_title"],
                    topic["source_url"],
                    topic.get("source_name"),
                    topic.get("published_at"),
                    topic["observed_at"],
                    topic["expires_at"],
                    topic["category"],
                    topic.get("summary", ""),
                    topic.get("hook", ""),
                    float(topic.get("score", 0.0)),
                    topic["content_hash"],
                    json.dumps(topic.get("metadata", {}), ensure_ascii=False),
                ),
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def list_topics(
        self,
        consumer_id: str,
        limit: int = 10,
        category: str | None = None,
        include_seen: bool = True,
    ) -> list[dict[str, Any]]:
        await self.cleanup_expired()
        db = self._conn()
        clauses = ["t.status='open'", "t.expires_at > ?"]
        params: list[Any] = [consumer_id, utc_now_iso()]
        if category:
            clauses.append("t.category=?")
            params.append(category)
        if not include_seen:
            clauses.append("COALESCE(cs.state, 'unseen')='unseen'")
        clauses.append("COALESCE(cs.state, 'unseen') NOT IN ('consumed','ignored')")
        params.append(max(1, min(50, limit)))
        sql = f"""
            SELECT t.*, COALESCE(cs.state, 'unseen') AS consumer_state
            FROM topics t
            LEFT JOIN consumer_states cs
              ON cs.topic_id=t.id AND cs.consumer_id=?
            WHERE {' AND '.join(clauses)}
            ORDER BY t.score DESC, t.observed_at DESC
            LIMIT ?
        """
        cur = await db.execute(sql, params)
        rows = await cur.fetchall()
        return [self._row_to_topic(row) for row in rows]

    async def get_topic(self, topic_id: str, consumer_id: str | None = None) -> dict[str, Any] | None:
        db = self._conn()
        if consumer_id:
            cur = await db.execute(
                """
                SELECT t.*, COALESCE(cs.state, 'unseen') AS consumer_state
                FROM topics t
                LEFT JOIN consumer_states cs
                  ON cs.topic_id=t.id AND cs.consumer_id=?
                WHERE t.id=?
                """,
                (consumer_id, topic_id),
            )
        else:
            cur = await db.execute("SELECT t.*, 'unseen' AS consumer_state FROM topics t WHERE t.id=?", (topic_id,))
        row = await cur.fetchone()
        return self._row_to_topic(row) if row else None

    async def set_consumer_state(
        self,
        topic_id: str,
        consumer_id: str,
        state: str,
        note: str | None = None,
    ) -> bool:
        if state not in {"seen", "consumed", "ignored"}:
            raise ValueError("state must be seen, consumed, or ignored")
        if await self.get_topic(topic_id) is None:
            return False
        db = self._conn()
        await db.execute(
            """
            INSERT INTO consumer_states(topic_id, consumer_id, state, note, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(topic_id, consumer_id)
            DO UPDATE SET state=excluded.state, note=excluded.note, updated_at=excluded.updated_at
            """,
            (topic_id, consumer_id, state, note, utc_now_iso()),
        )
        await db.commit()
        return True

    async def mark_dead(self, topic_id: str) -> bool:
        db = self._conn()
        cur = await db.execute("UPDATE topics SET status='dead' WHERE id=?", (topic_id,))
        await db.commit()
        return cur.rowcount > 0

    async def search_topics(
        self,
        query: str,
        consumer_id: str,
        limit: int = 10,
        include_expired: bool = False,
    ) -> list[dict[str, Any]]:
        await self.cleanup_expired()
        db = self._conn()
        like = f"%{query.strip()}%"
        status_clause = "" if include_expired else "AND t.status='open' AND t.expires_at > ?"
        params: list[Any] = [consumer_id, like, like, like, like]
        if not include_expired:
            params.append(utc_now_iso())
        params.append(max(1, min(50, limit)))
        cur = await db.execute(
            f"""
            SELECT t.*, COALESCE(cs.state, 'unseen') AS consumer_state
            FROM topics t
            LEFT JOIN consumer_states cs
              ON cs.topic_id=t.id AND cs.consumer_id=?
            WHERE (
                t.source_title LIKE ? OR t.summary LIKE ? OR t.hook LIKE ? OR t.category LIKE ?
            )
            {status_clause}
            ORDER BY t.score DESC, t.observed_at DESC
            LIMIT ?
            """,
            params,
        )
        return [self._row_to_topic(row) for row in await cur.fetchall()]

    async def stats(self, consumer_id: str) -> dict[str, Any]:
        await self.cleanup_expired()
        db = self._conn()
        cur = await db.execute("SELECT status, COUNT(*) AS n FROM topics GROUP BY status")
        global_counts = {row["status"]: row["n"] for row in await cur.fetchall()}
        cur = await db.execute(
            "SELECT state, COUNT(*) AS n FROM consumer_states WHERE consumer_id=? GROUP BY state",
            (consumer_id,),
        )
        consumer_counts = {row["state"]: row["n"] for row in await cur.fetchall()}
        cur = await db.execute(
            """
            SELECT COUNT(*) AS n
            FROM topics t
            LEFT JOIN consumer_states cs
              ON cs.topic_id=t.id AND cs.consumer_id=?
            WHERE t.status='open' AND t.expires_at>? AND COALESCE(cs.state,'unseen')='unseen'
            """,
            (consumer_id, utc_now_iso()),
        )
        unseen = (await cur.fetchone())["n"]
        return {"global": global_counts, "consumer": consumer_counts, "unseen_open": unseen}

    async def get_interest(self, consumer_id: str) -> dict[str, Any]:
        db = self._conn()
        cur = await db.execute("SELECT * FROM interests WHERE consumer_id=?", (consumer_id,))
        row = await cur.fetchone()
        if not row:
            return {"consumer_id": consumer_id, "categories": [], "keywords": []}
        return {
            "consumer_id": consumer_id,
            "categories": json.loads(row["categories_json"]),
            "keywords": json.loads(row["keywords_json"]),
            "updated_at": row["updated_at"],
        }

    async def update_interest(
        self,
        consumer_id: str,
        categories: list[str] | None = None,
        keywords: list[str] | None = None,
    ) -> dict[str, Any]:
        current = await self.get_interest(consumer_id)
        new_categories = categories if categories is not None else current["categories"]
        new_keywords = keywords if keywords is not None else current["keywords"]
        new_categories = sorted({x.strip().lower() for x in new_categories if x.strip()})[:30]
        new_keywords = sorted({x.strip() for x in new_keywords if x.strip()})[:50]
        db = self._conn()
        await db.execute(
            """
            INSERT INTO interests(consumer_id, categories_json, keywords_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(consumer_id)
            DO UPDATE SET categories_json=excluded.categories_json,
                          keywords_json=excluded.keywords_json,
                          updated_at=excluded.updated_at
            """,
            (
                consumer_id,
                json.dumps(new_categories, ensure_ascii=False),
                json.dumps(new_keywords, ensure_ascii=False),
                utc_now_iso(),
            ),
        )
        await db.commit()
        return await self.get_interest(consumer_id)

    async def begin_scout_run(self) -> int:
        db = self._conn()
        cur = await db.execute("INSERT INTO scout_runs(started_at) VALUES (?)", (utc_now_iso(),))
        await db.commit()
        return int(cur.lastrowid)

    async def finish_scout_run(
        self,
        run_id: int,
        candidate_count: int,
        selected_count: int,
        inserted_count: int,
        error: str | None = None,
    ) -> None:
        db = self._conn()
        await db.execute(
            """
            UPDATE scout_runs
               SET finished_at=?, candidate_count=?, selected_count=?, inserted_count=?, error=?
             WHERE id=?
            """,
            (utc_now_iso(), candidate_count, selected_count, inserted_count, error, run_id),
        )
        await db.commit()

    async def recent_scout_runs(self, limit: int = 5) -> list[dict[str, Any]]:
        db = self._conn()
        cur = await db.execute(
            "SELECT * FROM scout_runs ORDER BY id DESC LIMIT ?",
            (max(1, min(20, limit)),),
        )
        return [dict(row) for row in await cur.fetchall()]

    @staticmethod
    def _row_to_topic(row: aiosqlite.Row) -> dict[str, Any]:
        item = dict(row)
        try:
            item["metadata"] = json.loads(item.pop("metadata_json", "{}"))
        except json.JSONDecodeError:
            item["metadata"] = {}
        return item
