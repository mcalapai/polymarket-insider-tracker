"""Local embedding provider for semantic market retrieval.

This code is intentionally strict: if the required embedding backend is not
installed or the model cannot be loaded, scan/backtest must refuse to run.
"""

from __future__ import annotations

from dataclasses import dataclass


class EmbeddingProviderError(RuntimeError):
    pass


@dataclass(frozen=True)
class EmbeddingConfig:
    model_name_or_path: str
    device: str
    expected_dim: int
    normalize: bool = True


class SentenceTransformerEmbeddingProvider:
    def __init__(self, *, config: EmbeddingConfig) -> None:
        self._cfg = config
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore[import-not-found]
        except Exception as e:  # pragma: no cover
            raise EmbeddingProviderError(
                "sentence-transformers is required for SCAN_EMBEDDING_MODEL"
            ) from e

        try:
            self._model = SentenceTransformer(self._cfg.model_name_or_path, device=self._cfg.device)
        except Exception as e:
            raise EmbeddingProviderError(f"Failed to load embedding model: {e}") from e

    def embed_one(self, text: str) -> list[float]:
        vecs = self.embed_many([text])
        return vecs[0]

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        try:
            vectors = self._model.encode(
                texts,
                normalize_embeddings=self._cfg.normalize,
                show_progress_bar=False,
            )
        except Exception as e:
            raise EmbeddingProviderError(f"Embedding failed: {e}") from e

        out: list[list[float]] = []
        for v in vectors:
            row = [float(x) for x in v.tolist()]
            if len(row) != self._cfg.expected_dim:
                raise EmbeddingProviderError(
                    f"Embedding dim mismatch: got {len(row)} expected {self._cfg.expected_dim}"
                )
            out.append(row)
        return out

