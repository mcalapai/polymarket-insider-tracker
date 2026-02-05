"""Market indexing into Postgres with embeddings."""

from __future__ import annotations

import json
import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime

from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.scan.embeddings import SentenceTransformerEmbeddingProvider
from polymarket_insider_tracker.storage.repos import MarketDTO, MarketRepository


@dataclass(frozen=True)
class MarketIndexerConfig:
    active_only: bool = False


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

        markets = await asyncio.to_thread(self._clob.get_markets, self._cfg.active_only)
        repo = MarketRepository(session)
        now = datetime.now(UTC)

        texts = [f"{m.question}\n\n{m.description}".strip() for m in markets]
        embeddings = self._embedder.embed_many(texts)
        if len(embeddings) != len(markets):
            raise RuntimeError("Embedding output length mismatch")

        for m, emb in zip(markets, embeddings, strict=True):
            tokens_json = json.dumps(
                [{"token_id": t.token_id, "outcome": t.outcome, "price": str(t.price) if t.price else None} for t in m.tokens]
            )
            await repo.upsert(
                dto=MarketDTO(
                    condition_id=m.condition_id,
                    question=m.question,
                    description=m.description,
                    tokens_json=tokens_json,
                    active=m.active,
                    closed=m.closed,
                    end_date=m.end_date,
                ),
                embedding=emb,
                now=now,
            )
        await session.flush()
        return len(markets)
