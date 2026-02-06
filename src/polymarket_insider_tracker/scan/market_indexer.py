"""Market indexing into Postgres with embeddings."""

from __future__ import annotations

import json
import asyncio
import logging
import threading
from dataclasses import dataclass
from datetime import UTC, datetime

from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.scan.embeddings import SentenceTransformerEmbeddingProvider
from polymarket_insider_tracker.scan.progress import ProgressLine, default_progress_enabled
from polymarket_insider_tracker.storage.repos import MarketDTO, MarketRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketIndexerConfig:
    active_only: bool = False
    chunk_size: int = 1024
    embed_batch_size: int = 64
    commit_every_chunks: int = 10
    show_progress: bool = True


class MarketIndexer:
    def __init__(
        self,
        *,
        clob_client: ClobClient,
        embedder: SentenceTransformerEmbeddingProvider,
        config: MarketIndexerConfig | None = None,
    ) -> None:
        self._clob = clob_client
        self._embedder = embedder
        self._cfg = config or MarketIndexerConfig()

    async def run_once(self, *, session: object) -> int:
        # Import AsyncSession only when needed to keep module import light.
        from sqlalchemy.ext.asyncio import AsyncSession

        if not isinstance(session, AsyncSession):
            raise TypeError("session must be an AsyncSession")

        repo = MarketRepository(session)
        now = datetime.now(UTC)

        if self._cfg.chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        if self._cfg.embed_batch_size is not None and self._cfg.embed_batch_size < 1:
            raise ValueError("embed_batch_size must be >= 1")
        if self._cfg.commit_every_chunks < 1:
            raise ValueError("commit_every_chunks must be >= 1")

        progress = ProgressLine(
            enabled=bool(self._cfg.show_progress) and default_progress_enabled(),
        )

        loop = asyncio.get_running_loop()
        done_sentinel = object()
        q: asyncio.Queue[object] = asyncio.Queue(maxsize=4)

        def producer() -> None:
            try:
                chunk: list[object] = []
                for m in self._clob.iter_markets(active_only=self._cfg.active_only):
                    chunk.append(m)
                    if len(chunk) >= self._cfg.chunk_size:
                        asyncio.run_coroutine_threadsafe(q.put(chunk), loop).result()
                        chunk = []
                if chunk:
                    asyncio.run_coroutine_threadsafe(q.put(chunk), loop).result()
                asyncio.run_coroutine_threadsafe(q.put(done_sentinel), loop).result()
            except Exception as e:  # pragma: no cover
                asyncio.run_coroutine_threadsafe(q.put(e), loop).result()

        thread = threading.Thread(target=producer, name="market-indexer-producer", daemon=True)
        thread.start()

        fetched = 0
        indexed = 0
        chunk_i = 0
        total: int | None = None
        invalid = 0

        try:
            while True:
                msg = await q.get()
                if msg is done_sentinel:
                    total = fetched
                    break
                if isinstance(msg, Exception):
                    raise msg

                # At this point msg is a market chunk.
                markets = msg  # type: ignore[assignment]
                fetched += len(markets)
                progress.update(stage="fetch", fetched=fetched, indexed=indexed, total=total)

                # The upstream API can return duplicate condition_ids. Postgres bulk
                # upserts cannot contain duplicate conflict keys in a single INSERT.
                unique_by_id: dict[str, object] = {}
                invalid_in_chunk = 0
                for m in markets:
                    mid = getattr(m, "condition_id", None)
                    if not isinstance(mid, str) or not mid:
                        invalid_in_chunk += 1
                        continue
                    if mid in unique_by_id:
                        continue
                    unique_by_id[mid] = m
                if invalid_in_chunk:
                    invalid += invalid_in_chunk
                    logger.warning(
                        "Skipping markets with invalid condition_id (count=%d)",
                        invalid_in_chunk,
                    )
                unique_markets = list(unique_by_id.values())
                if not unique_markets:
                    # Nothing to index from this chunk.
                    continue

                texts = [f"{m.question}\n\n{m.description}".strip() for m in unique_markets]  # type: ignore[attr-defined]
                embeddings = await asyncio.to_thread(
                    self._embedder.embed_many, texts, batch_size=self._cfg.embed_batch_size
                )
                if len(embeddings) != len(unique_markets):
                    raise RuntimeError("Embedding output length mismatch")

                items: list[tuple[MarketDTO, list[float]]] = []
                for m, emb in zip(unique_markets, embeddings, strict=True):  # type: ignore[arg-type]
                    tokens_json = json.dumps(
                        [
                            {
                                "token_id": t.token_id,
                                "outcome": t.outcome,
                                "price": str(t.price) if t.price else None,
                            }
                            for t in m.tokens
                        ],
                        separators=(",", ":"),
                    )
                    items.append(
                        (
                            MarketDTO(
                                condition_id=m.condition_id,
                                question=m.question,
                                description=m.description,
                                tokens_json=tokens_json,
                                active=m.active,
                                closed=m.closed,
                                end_date=m.end_date,
                            ),
                            emb,
                        )
                    )

                await repo.upsert_many(items=items, now=now)
                indexed += len(items)
                chunk_i += 1
                progress.update(stage="index", fetched=fetched, indexed=indexed, total=total)

                if (chunk_i % self._cfg.commit_every_chunks) == 0:
                    await session.commit()

            await session.commit()
        finally:
            suffix = f" invalid={invalid:,}" if invalid else ""
            progress.close(final_line=f"index done   fetched={fetched:,} indexed={indexed:,}{suffix}")

        if fetched > 0 and indexed == 0:
            raise RuntimeError(
                "No markets were indexed (all rows invalid or filtered). "
                f"fetched={fetched} invalid={invalid}"
            )

        return indexed
