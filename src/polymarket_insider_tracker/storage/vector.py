"""Minimal pgvector SQLAlchemy type without requiring the pgvector Python package.

This project standardizes on Postgres + pgvector for semantic market search.
To keep the core runtime lightweight, we implement a tiny `vector(n)` type
adapter directly.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy.types import UserDefinedType


class Vector(UserDefinedType):
    cache_ok = True

    def __init__(self, dimensions: int | None = None) -> None:
        if dimensions is None:
            self._dimensions = None
            return
        if dimensions <= 0:
            raise ValueError("dimensions must be positive")
        self._dimensions = int(dimensions)

    def get_col_spec(self, **kw: Any) -> str:  # noqa: ARG002
        if self._dimensions is None:
            return "vector"
        return f"vector({self._dimensions})"

    def bind_processor(self, dialect: Any) -> Any:  # noqa: ARG002
        def process(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, str):
                return value
            if isinstance(value, Sequence):
                return "[" + ",".join(str(float(x)) for x in value) + "]"
            raise TypeError("Vector value must be a sequence of floats or a vector literal string")

        return process
