"""Semantic market search."""

from __future__ import annotations

from dataclasses import dataclass

from polymarket_insider_tracker.scan.embeddings import SentenceTransformerEmbeddingProvider
from polymarket_insider_tracker.storage.repos import MarketDTO, MarketRepository


@dataclass(frozen=True)
class MarketSearchConfig:
    top_k: int = 25
    active_only: bool = False


class MarketSearch:
    def __init__(
        self,
        *,
        embedder: SentenceTransformerEmbeddingProvider,
        config: MarketSearchConfig | None = None,
    ) -> None:
        self._embedder = embedder
        self._cfg = config or MarketSearchConfig()

    async def search(self, *, session: object, query: str) -> list[MarketDTO]:
        from sqlalchemy.ext.asyncio import AsyncSession

        if not isinstance(session, AsyncSession):
            raise TypeError("session must be an AsyncSession")
        q_emb = self._embedder.embed_one(query)
        repo = MarketRepository(session)
        return await repo.search_by_embedding(
            query_embedding=q_emb,
            top_k=self._cfg.top_k,
            active_only=self._cfg.active_only,
        )

