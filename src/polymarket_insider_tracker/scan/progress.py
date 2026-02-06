"""Lightweight progress rendering utilities (no external deps).

The scan/backtest indexing path can process hundreds of thousands of markets.
We keep progress output:
- Single-line and TTY-friendly (updates in-place)
- Low-noise (rate-limited renders)
- Dependency-free (no tqdm)
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass


def _format_rate(rate_per_s: float) -> str:
    if rate_per_s <= 0:
        return "0/s"
    if rate_per_s >= 1_000_000:
        return f"{rate_per_s/1_000_000:.1f}M/s"
    if rate_per_s >= 1_000:
        return f"{rate_per_s/1_000:.1f}k/s"
    return f"{rate_per_s:.1f}/s"


def _format_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0 or seconds == float("inf"):
        return "ETA ?"
    s = int(seconds)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    if h:
        return f"ETA {h:d}:{m:02d}:{s:02d}"
    return f"ETA {m:d}:{s:02d}"


def _bar(fraction: float, width: int = 22) -> str:
    fraction = max(0.0, min(1.0, fraction))
    filled = int(round(fraction * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


@dataclass
class ProgressLine:
    enabled: bool
    min_interval_s: float = 0.20

    def __post_init__(self) -> None:
        self._start = time.monotonic()
        self._last_render = 0.0
        self._last_line_len = 0
        self._spinner_i = 0

    def update(
        self,
        *,
        stage: str,
        fetched: int,
        indexed: int,
        total: int | None,
    ) -> None:
        if not self.enabled:
            return
        now = time.monotonic()
        if (now - self._last_render) < self.min_interval_s:
            return
        self._last_render = now

        elapsed = max(1e-6, now - self._start)
        rate = indexed / elapsed

        if total:
            frac = indexed / max(1, total)
            eta = (total - indexed) / rate if rate > 0 else None
            prefix = f"{stage:<8} {_bar(frac)} {frac*100:5.1f}%"
        else:
            spinner = "|/-\\"[self._spinner_i % 4]
            self._spinner_i += 1
            eta = None
            prefix = f"{stage:<8} {spinner}"

        line = (
            f"{prefix} fetched={fetched:,} indexed={indexed:,} "
            f"rate={_format_rate(rate)} {_format_eta(eta)}"
        )
        # Clear previous line if it was longer.
        pad = " " * max(0, self._last_line_len - len(line))
        self._last_line_len = len(line)
        sys.stderr.write("\r" + line + pad)
        sys.stderr.flush()

    def close(self, *, final_line: str | None = None) -> None:
        if not self.enabled:
            return
        if final_line is not None:
            pad = " " * max(0, self._last_line_len - len(final_line))
            sys.stderr.write("\r" + final_line + pad + "\n")
        else:
            sys.stderr.write("\n")
        sys.stderr.flush()


def default_progress_enabled() -> bool:
    return bool(getattr(sys.stderr, "isatty", lambda: False)())

