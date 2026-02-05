"""Configuration management service with Pydantic Settings.

This module provides centralized configuration management for the
Polymarket Insider Tracker application, loading and validating
environment variables at startup.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = ".env"
_ENV_FILE_ENCODING = "utf-8"


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    url: str = Field(
        alias="DATABASE_URL",
        description="PostgreSQL connection string",
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate database URL format."""
        if not v.startswith(("postgresql://", "postgresql+asyncpg://")):
            raise ValueError("DATABASE_URL must be a PostgreSQL connection string")
        return v


class RedisSettings(BaseSettings):
    """Redis connection settings."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    url: str = Field(
        default="redis://localhost:6379",
        alias="REDIS_URL",
        description="Redis connection string",
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate Redis URL format."""
        if not v.startswith("redis://"):
            raise ValueError("REDIS_URL must start with redis://")
        return v


class PolygonSettings(BaseSettings):
    """Polygon blockchain RPC settings."""

    model_config = SettingsConfigDict(env_prefix="POLYGON_", extra="ignore")

    rpc_url: str = Field(
        default="https://polygon-rpc.com",
        alias="POLYGON_RPC_URL",
        description="Primary Polygon RPC endpoint",
    )
    fallback_rpc_url: str | None = Field(
        default=None,
        alias="POLYGON_FALLBACK_RPC_URL",
        description="Fallback Polygon RPC endpoint",
    )

    @field_validator("rpc_url", "fallback_rpc_url")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        """Validate RPC URL format."""
        if v is None:
            return v
        if not v.startswith(("http://", "https://")):
            raise ValueError("RPC URL must be an HTTP(S) endpoint")
        return v


class PolymarketSettings(BaseSettings):
    """Polymarket API settings."""

    model_config = SettingsConfigDict(env_prefix="POLYMARKET_", extra="ignore")

    trade_ws_url: str = Field(
        default="wss://ws-live-data.polymarket.com",
        alias="POLYMARKET_TRADE_WS_URL",
        description="WebSocket URL for trade activity feed",
    )
    clob_market_ws_url: str = Field(
        default="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        alias="POLYMARKET_CLOB_MARKET_WS_URL",
        description="CLOB market-channel WebSocket URL (book / price_change)",
    )
    clob_host: str = Field(
        default="https://clob.polymarket.com",
        alias="POLYMARKET_CLOB_HOST",
        description="CLOB HTTP API host",
    )
    clob_chain_id: int = Field(
        default=137,
        alias="POLYMARKET_CLOB_CHAIN_ID",
        description="Chain ID for signing (Polygon=137)",
    )
    clob_private_key: SecretStr | None = Field(
        default=None,
        alias="POLYMARKET_CLOB_PRIVATE_KEY",
        description="Private key used for L2 authentication (required for order/trade history endpoints)",
    )
    clob_api_key: SecretStr | None = Field(
        default=None,
        alias="POLYMARKET_CLOB_API_KEY",
        description="CLOB API key (L2 auth)",
    )
    clob_api_secret: SecretStr | None = Field(
        default=None,
        alias="POLYMARKET_CLOB_API_SECRET",
        description="CLOB API secret (L2 auth)",
    )
    clob_api_passphrase: SecretStr | None = Field(
        default=None,
        alias="POLYMARKET_CLOB_API_PASSPHRASE",
        description="CLOB API passphrase (L2 auth)",
    )
    clob_signature_type: int | None = Field(
        default=None,
        alias="POLYMARKET_CLOB_SIGNATURE_TYPE",
        description="Optional signature type override for order signing (advanced)",
    )
    clob_funder: str | None = Field(
        default=None,
        alias="POLYMARKET_CLOB_FUNDER",
        description="Optional funder address override for signing (advanced)",
    )

    @field_validator("trade_ws_url", "clob_market_ws_url")
    @classmethod
    def validate_ws_url(cls, v: str) -> str:
        """Validate WebSocket URL format."""
        if not v.startswith(("ws://", "wss://")):
            raise ValueError("WebSocket URL must start with ws:// or wss://")
        return v

    @field_validator("clob_host")
    @classmethod
    def validate_clob_host(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("POLYMARKET_CLOB_HOST must be an HTTP(S) endpoint")
        return v.rstrip("/")


class OrdersSettings(BaseSettings):
    """Order lifecycle ingestion and spoofing-detection settings."""

    model_config = SettingsConfigDict(env_prefix="ORDERS_", extra="ignore")

    enabled: bool = Field(
        default=False,
        alias="ORDERS_ENABLED",
        description="Enable order lifecycle ingestion and spoofing-related detectors",
    )
    subscribe_hot_assets_only: bool = Field(
        default=True,
        alias="ORDERS_SUBSCRIBE_HOT_ASSETS_ONLY",
        description="Only subscribe to CLOB market channel for hot assets (bounded load)",
    )
    max_subscribed_assets: int = Field(
        default=200,
        alias="ORDERS_MAX_SUBSCRIBED_ASSETS",
        ge=1,
        le=10_000,
        description="Maximum asset_ids to keep subscribed at once",
    )
    resubscribe_interval_seconds: int = Field(
        default=30,
        alias="ORDERS_RESUBSCRIBE_INTERVAL_SECONDS",
        ge=5,
        le=3600,
        description="How often to reconcile subscriptions against the hot registry",
    )
    order_details_cache_ttl_seconds: int = Field(
        default=7 * 24 * 3600,
        alias="ORDERS_ORDER_DETAILS_CACHE_TTL_SECONDS",
        ge=60,
        le=90 * 24 * 3600,
        description="Redis TTL for cached order attribution/details by order hash",
    )
    lifecycle_window_minutes: int = Field(
        default=60,
        alias="ORDERS_LIFECYCLE_WINDOW_MINUTES",
        ge=5,
        le=24 * 60,
        description="Rolling window for order/trade ratio and spoofing aggregates (minutes)",
    )
    rapid_cancel_percentile: float = Field(
        default=0.01,
        alias="ORDERS_RAPID_CANCEL_PERCENTILE",
        ge=0.0,
        le=0.5,
        description="Percentile threshold for defining 'rapid' cancels from the cancel-latency baseline",
    )
    book_impact_percentile: float = Field(
        default=0.99,
        alias="ORDERS_BOOK_IMPACT_PERCENTILE",
        ge=0.5,
        le=1.0,
        description="Percentile threshold for defining unusually large book impact (bps baseline)",
    )
    baseline_min_samples: int = Field(
        default=50,
        alias="ORDERS_BASELINE_MIN_SAMPLES",
        ge=0,
        le=1_000_000,
        description="Minimum baseline samples before computing percentile-based spoofing signals",
    )


class FreshWalletSettings(BaseSettings):
    """Fresh-wallet detector configuration."""

    model_config = SettingsConfigDict(env_prefix="FRESH_WALLET_", extra="ignore")

    min_trade_notional_usdc: Decimal = Field(
        default=Decimal("1000"),
        alias="FRESH_WALLET_MIN_TRADE_NOTIONAL_USDC",
        description="Minimum trade notional (USDC) to evaluate fresh-wallet detection",
    )
    max_nonce: int = Field(
        default=5,
        alias="FRESH_WALLET_MAX_NONCE",
        ge=0,
        le=1_000_000,
        description="Max nonce-as-of-trade to consider wallet fresh",
    )
    max_age_hours: float = Field(
        default=48.0,
        alias="FRESH_WALLET_MAX_AGE_HOURS",
        ge=0.0,
        le=3650 * 24,
        description="Max wallet age (hours) as-of-trade to consider fresh",
    )

    @field_validator("min_trade_notional_usdc")
    @classmethod
    def validate_min_trade_notional_usdc(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("FRESH_WALLET_MIN_TRADE_NOTIONAL_USDC must be > 0")
        return v


class SizeAnomalySettings(BaseSettings):
    """Liquidity-aware size anomaly detector configuration."""

    model_config = SettingsConfigDict(env_prefix="SIZE_ANOMALY_", extra="ignore")

    volume_threshold: float = Field(
        default=0.02,
        alias="SIZE_ANOMALY_VOLUME_THRESHOLD",
        ge=0.0,
        le=1.0,
        description="Volume impact threshold: trade_notional / rolling_24h_volume",
    )
    book_threshold: float = Field(
        default=0.05,
        alias="SIZE_ANOMALY_BOOK_THRESHOLD",
        ge=0.0,
        le=1.0,
        description="Book impact threshold: trade_notional / visible_book_depth",
    )


class LiquiditySettings(BaseSettings):
    """Liquidity snapshot settings for size anomaly detection."""

    model_config = SettingsConfigDict(env_prefix="LIQUIDITY_", extra="ignore")

    hot_market_ttl_seconds: int = Field(
        default=3600,
        alias="LIQUIDITY_HOT_MARKET_TTL_SECONDS",
        ge=60,
        description="How long a market remains 'hot' after a trade",
    )
    volume_window_hours: int = Field(
        default=24,
        alias="LIQUIDITY_VOLUME_WINDOW_HOURS",
        ge=1,
        le=168,
        description="Rolling volume window size (hours)",
    )
    depth_max_slippage_bps: int = Field(
        default=200,
        alias="LIQUIDITY_DEPTH_MAX_SLIPPAGE_BPS",
        ge=1,
        le=10_000,
        description="Orderbook depth band around mid (bps)",
    )


class BaselineSettings(BaseSettings):
    """Rolling statistical baselines used by anomaly detectors."""

    model_config = SettingsConfigDict(env_prefix="BASELINE_", extra="ignore")

    window_hours: int = Field(
        default=24,
        alias="BASELINE_WINDOW_HOURS",
        ge=1,
        le=168,
        description="Rolling window length for baselines (hours)",
    )
    bucket_seconds: int = Field(
        default=300,
        alias="BASELINE_BUCKET_SECONDS",
        ge=60,
        le=3600,
        description="Aggregation bucket size (seconds)",
    )
    cleanup_batch_size: int = Field(
        default=100,
        alias="BASELINE_CLEANUP_BATCH_SIZE",
        ge=1,
        le=10_000,
        description="Max expired buckets to evict per query",
    )
    persist_interval_seconds: int = Field(
        default=3600,
        alias="BASELINE_PERSIST_INTERVAL_SECONDS",
        ge=60,
        le=86_400,
        description="How often to persist daily baseline aggregates",
    )


class FundingSettings(BaseSettings):
    """Funding-chain indexing and tracing settings."""

    model_config = SettingsConfigDict(env_prefix="FUNDING_", extra="ignore")

    lookback_days: int = Field(
        default=180,
        alias="FUNDING_LOOKBACK_DAYS",
        ge=1,
        le=3650,
        description="Max lookback window for transfer indexing",
    )
    max_hops: int = Field(
        default=3,
        alias="FUNDING_MAX_HOPS",
        ge=1,
        le=10,
        description="Maximum hops to trace funding chains",
    )
    logs_chunk_size_blocks: int = Field(
        default=20_000,
        alias="FUNDING_LOGS_CHUNK_SIZE_BLOCKS",
        ge=1000,
        le=500_000,
        description="Block chunk size for eth_getLogs scans",
    )
    trace_min_score: float = Field(
        default=0.75,
        alias="FUNDING_TRACE_MIN_SCORE",
        ge=0.0,
        le=1.0,
        description="Minimum risk score to trigger tracing",
    )
    trace_high_water_notional_usdc: float = Field(
        default=10_000.0,
        alias="FUNDING_TRACE_HIGH_WATER_NOTIONAL_USDC",
        ge=0.0,
        description="Also trace when fresh-wallet trade exceeds this notional",
    )
    shares_funder_time_band_minutes: int = Field(
        default=30,
        alias="FUNDING_SHARES_FUNDER_TIME_BAND_MINUTES",
        ge=1,
        le=1440,
        description="Time band for shares_funder/funding_burst derivations",
    )
    funding_burst_min_wallets: int = Field(
        default=3,
        alias="FUNDING_BURST_MIN_WALLETS",
        ge=2,
        le=1000,
        description="Minimum funded wallets in band to mark funding_burst",
    )

    usdc_contract_addresses: tuple[str, ...] = Field(
        default=(
            "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # bridged USDC
            "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",  # native USDC
        ),
        alias="FUNDING_USDC_CONTRACT_ADDRESSES",
        description="USDC contract addresses to index (comma-separated)",
    )

    @field_validator("usdc_contract_addresses", mode="before")
    @classmethod
    def _parse_usdc_addresses(cls, v: object) -> tuple[str, ...]:
        if v is None:
            raise ValueError("FUNDING_USDC_CONTRACT_ADDRESSES must be set")
        if isinstance(v, str):
            parts = [p.strip() for p in v.split(",") if p.strip()]
            return tuple(parts)
        if isinstance(v, (list, tuple)):
            return tuple(str(x) for x in v)
        raise TypeError("Invalid FUNDING_USDC_CONTRACT_ADDRESSES type")


class SniperSettings(BaseSettings):
    """Sniper clustering settings."""

    model_config = SettingsConfigDict(env_prefix="SNIPER_", extra="ignore")

    enabled: bool = Field(
        default=True,
        alias="SNIPER_ENABLED",
        description="Enable sniper cluster detection",
    )
    run_interval_seconds: int = Field(
        default=60,
        alias="SNIPER_RUN_INTERVAL_SECONDS",
        ge=5,
        le=3600,
        description="How often to run clustering",
    )
    window_days: int = Field(
        default=7,
        alias="SNIPER_WINDOW_DAYS",
        ge=1,
        le=90,
        description="How many days of entries to retain",
    )

    entry_threshold_seconds: int = Field(
        default=300,
        alias="SNIPER_ENTRY_THRESHOLD_SECONDS",
        ge=1,
        le=3600,
        description="Max seconds after first trade considered 'early'",
    )
    min_cluster_size: int = Field(
        default=3,
        alias="SNIPER_MIN_CLUSTER_SIZE",
        ge=2,
        le=100,
        description="Minimum wallets to form a sniper cluster",
    )
    eps: float = Field(
        default=0.65,
        alias="SNIPER_EPS",
        ge=0.01,
        le=10.0,
        description="DBSCAN epsilon (distance threshold)",
    )
    min_samples: int = Field(
        default=3,
        alias="SNIPER_MIN_SAMPLES",
        ge=1,
        le=50,
        description="DBSCAN min_samples parameter",
    )
    min_entries_per_wallet: int = Field(
        default=2,
        alias="SNIPER_MIN_ENTRIES_PER_WALLET",
        ge=1,
        le=100,
        description="Minimum entries per wallet to be eligible for clustering",
    )
    min_markets_in_common: int = Field(
        default=1,
        alias="SNIPER_MIN_MARKETS_IN_COMMON",
        ge=0,
        le=50,
        description="Require cluster wallets to share at least this many markets",
    )

    coentry_top_n: int = Field(
        default=10,
        alias="SNIPER_COENTRY_TOP_N",
        ge=2,
        le=200,
        description="Top-N entrants per market to consider for co-entry",
    )
    coentry_min_markets: int = Field(
        default=3,
        alias="SNIPER_COENTRY_MIN_MARKETS",
        ge=1,
        le=100,
        description="Minimum markets a wallet must appear in top-N",
    )
    coentry_min_shared_markets: int = Field(
        default=2,
        alias="SNIPER_COENTRY_MIN_SHARED_MARKETS",
        ge=1,
        le=100,
        description="Minimum shared markets with same partner to signal",
    )


class ScanSettings(BaseSettings):
    """Historical scan/backtest settings."""

    model_config = SettingsConfigDict(env_prefix="SCAN_", extra="ignore")

    # Embeddings are required for `scan` (no brittle keyword matching).
    embedding_model: str | None = Field(
        default=None,
        alias="SCAN_EMBEDDING_MODEL",
        description="Local embedding model identifier/path",
    )
    embedding_dim: int | None = Field(
        default=None,
        alias="SCAN_EMBEDDING_DIM",
        description="Embedding vector dimensionality for pgvector (must match the model)",
        ge=1,
        le=16_000,
    )
    embedding_device: Literal["cpu", "cuda", "mps"] = Field(
        default="cpu",
        alias="SCAN_EMBEDDING_DEVICE",
        description="Device to run embeddings on",
    )
    top_k_markets: int = Field(
        default=25,
        alias="SCAN_TOP_K_MARKETS",
        ge=1,
        le=500,
        description="How many markets to retrieve for scan",
    )
    lookback_days: int = Field(
        default=30,
        alias="SCAN_LOOKBACK_DAYS",
        ge=1,
        le=3650,
        description="How many days of historical trades to consider during scan",
    )
    max_trades_per_market: int = Field(
        default=50_000,
        alias="SCAN_MAX_TRADES_PER_MARKET",
        ge=1,
        le=5_000_000,
        description="Hard cap on trades fetched per market (bounded work)",
    )
    allow_non_historical_book_depth: bool = Field(
        default=False,
        alias="SCAN_ALLOW_NON_HISTORICAL_BOOK_DEPTH",
        description="If true, uses current orderbook depth when historical depth snapshots are unavailable",
    )
    pre_move_weight: float = Field(
        default=0.10,
        alias="SCAN_PRE_MOVE_WEIGHT",
        ge=0.0,
        le=1.0,
        description="Weight for PreMoveSignal when ranking scan/backtest results",
    )

    artifacts_dir: Path = Field(
        default=Path("artifacts"),
        alias="SCAN_ARTIFACTS_DIR",
        description="Directory for scan/backtest artifacts",
    )


class ModelSettings(BaseSettings):
    """ML model training/serving settings."""

    model_config = SettingsConfigDict(env_prefix="MODEL_", extra="ignore")

    enabled: bool = Field(
        default=False,
        alias="MODEL_ENABLED",
        description="Enable model scoring in live mode (requires a trained artifact)",
    )
    artifacts_dir: Path = Field(
        default=Path("artifacts/models"),
        alias="MODEL_ARTIFACTS_DIR",
        description="Directory for model artifacts",
    )
    label_horizon_seconds: int = Field(
        default=3600,
        alias="MODEL_LABEL_HORIZON_SECONDS",
        ge=60,
        le=7 * 24 * 3600,
        description="Horizon used for self-supervised labels",
    )
    label_z_threshold: float = Field(
        default=2.0,
        alias="MODEL_LABEL_Z_THRESHOLD",
        ge=0.0,
        le=100.0,
        description="Z-threshold used to define positive labels",
    )
    auto_bless: bool = Field(
        default=True,
        alias="MODEL_AUTO_BLESS",
        description="Mark the latest trained model as blessed",
    )


class DiscordSettings(BaseSettings):
    """Discord notification settings."""

    model_config = SettingsConfigDict(env_prefix="DISCORD_", extra="ignore")

    webhook_url: SecretStr | None = Field(
        default=None,
        alias="DISCORD_WEBHOOK_URL",
        description="Discord webhook URL for alerts",
    )

    @property
    def enabled(self) -> bool:
        """Check if Discord notifications are enabled."""
        return self.webhook_url is not None


class TelegramSettings(BaseSettings):
    """Telegram notification settings."""

    model_config = SettingsConfigDict(env_prefix="TELEGRAM_", extra="ignore")

    bot_token: SecretStr | None = Field(
        default=None,
        alias="TELEGRAM_BOT_TOKEN",
        description="Telegram bot token",
    )
    chat_id: str | None = Field(
        default=None,
        alias="TELEGRAM_CHAT_ID",
        description="Telegram chat ID for alerts",
    )

    @property
    def enabled(self) -> bool:
        """Check if Telegram notifications are enabled."""
        return self.bot_token is not None and self.chat_id is not None


class Settings(BaseSettings):
    """Main application settings.

    Loads configuration from environment variables with support for
    .env files via python-dotenv.

    Example:
        ```python
        from polymarket_insider_tracker.config import get_settings

        settings = get_settings()
        print(settings.database.url)
        print(settings.log_level)
        ```
    """

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding=_ENV_FILE_ENCODING,
        extra="ignore",
    )

    # Nested configuration groups
    #
    # NOTE: Each nested BaseSettings must be given the same env_file, otherwise it
    # will only read from the process environment (and ignore `.env`).
    database: DatabaseSettings = Field(
        default_factory=lambda: DatabaseSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    redis: RedisSettings = Field(
        default_factory=lambda: RedisSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    polygon: PolygonSettings = Field(
        default_factory=lambda: PolygonSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    polymarket: PolymarketSettings = Field(
        default_factory=lambda: PolymarketSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    orders: OrdersSettings = Field(
        default_factory=lambda: OrdersSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    fresh_wallet: FreshWalletSettings = Field(
        default_factory=lambda: FreshWalletSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    size_anomaly: SizeAnomalySettings = Field(
        default_factory=lambda: SizeAnomalySettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    discord: DiscordSettings = Field(
        default_factory=lambda: DiscordSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    telegram: TelegramSettings = Field(
        default_factory=lambda: TelegramSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    liquidity: LiquiditySettings = Field(
        default_factory=lambda: LiquiditySettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    baseline: BaselineSettings = Field(
        default_factory=lambda: BaselineSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    funding: FundingSettings = Field(
        default_factory=lambda: FundingSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    sniper: SniperSettings = Field(
        default_factory=lambda: SniperSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    scan: ScanSettings = Field(
        default_factory=lambda: ScanSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )
    model: ModelSettings = Field(
        default_factory=lambda: ModelSettings(
            _env_file=_ENV_FILE,
            _env_file_encoding=_ENV_FILE_ENCODING,
        )
    )

    # Application settings
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        alias="LOG_LEVEL",
        description="Logging level",
    )
    health_port: int = Field(
        default=8080,
        alias="HEALTH_PORT",
        description="HTTP port for health check endpoints",
        ge=1,
        le=65535,
    )
    dry_run: bool = Field(
        default=False,
        alias="DRY_RUN",
        description="Run without sending actual alerts",
    )

    def get_logging_level(self) -> int:
        """Get the numeric logging level."""
        level: int = getattr(logging, self.log_level)
        return level

    def redacted_summary(self) -> dict[str, str | dict[str, str]]:
        """Get a summary of settings with secrets redacted.

        Returns:
            Dictionary of settings with sensitive values masked.
        """
        return {
            "database_url": self._redact_url(self.database.url),
            "redis_url": self._redact_url(self.redis.url),
            "polygon": {
                "rpc_url": self.polygon.rpc_url,
                "fallback_rpc_url": self.polygon.fallback_rpc_url or "(not set)",
            },
            "polymarket": {
                "trade_ws_url": self.polymarket.trade_ws_url,
                "clob_market_ws_url": self.polymarket.clob_market_ws_url,
                "clob_host": self.polymarket.clob_host,
                "clob_chain_id": str(self.polymarket.clob_chain_id),
                "clob_private_key": "(set)" if self.polymarket.clob_private_key else "(not set)",
                "clob_api_key": "(set)" if self.polymarket.clob_api_key else "(not set)",
            },
            "orders": {
                "enabled": str(self.orders.enabled),
            },
            "fresh_wallet": {
                "min_trade_notional_usdc": str(self.fresh_wallet.min_trade_notional_usdc),
                "max_nonce": str(self.fresh_wallet.max_nonce),
                "max_age_hours": str(self.fresh_wallet.max_age_hours),
            },
            "size_anomaly": {
                "volume_threshold": str(self.size_anomaly.volume_threshold),
                "book_threshold": str(self.size_anomaly.book_threshold),
            },
            "liquidity": {
                "volume_window_hours": str(self.liquidity.volume_window_hours),
                "depth_max_slippage_bps": str(self.liquidity.depth_max_slippage_bps),
            },
            "funding": {
                "lookback_days": str(self.funding.lookback_days),
                "max_hops": str(self.funding.max_hops),
            },
            "scan": {
                "embedding_model": self.scan.embedding_model or "(not set)",
                "top_k_markets": str(self.scan.top_k_markets),
                "pre_move_weight": str(self.scan.pre_move_weight),
            },
            "discord_enabled": str(self.discord.enabled),
            "telegram_enabled": str(self.telegram.enabled),
            "log_level": self.log_level,
            "health_port": str(self.health_port),
            "dry_run": str(self.dry_run),
        }

    def validate_requirements(self, *, command: Literal["run", "scan", "train-model"]) -> None:
        """Validate command-specific requirements.

        This is strict by design: if a capability is required for a command
        and not configured, the application must refuse to run.
        """
        if command in ("scan", "train-model"):
            if not self.scan.embedding_model:
                raise ValueError("SCAN_EMBEDDING_MODEL is required for historical scan/training")
            if not self.scan.embedding_dim:
                raise ValueError("SCAN_EMBEDDING_DIM is required for historical scan/training")

        # L2 auth is required for order attribution and for full historical trade fetches.
        needs_level2 = False
        if command == "run" and self.orders.enabled:
            needs_level2 = True

        if needs_level2:
            if not self.polymarket.clob_private_key:
                raise ValueError("POLYMARKET_CLOB_PRIVATE_KEY is required for Level-2 CLOB endpoints")
            if not (self.polymarket.clob_api_key and self.polymarket.clob_api_secret and self.polymarket.clob_api_passphrase):
                raise ValueError(
                    "POLYMARKET_CLOB_API_KEY/POLYMARKET_CLOB_API_SECRET/POLYMARKET_CLOB_API_PASSPHRASE are required for Level-2 CLOB endpoints"
                )

    @staticmethod
    def _redact_url(url: str) -> str:
        """Redact password from URL if present."""
        if "@" in url and "://" in url:
            # URL has credentials - redact the password
            protocol_end = url.index("://") + 3
            at_pos = url.index("@")
            creds_part = url[protocol_end:at_pos]
            if ":" in creds_part:
                username = creds_part.split(":")[0]
                return f"{url[:protocol_end]}{username}:***@{url[at_pos + 1 :]}"
        return url


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get the application settings singleton.

    Uses LRU cache to ensure settings are loaded only once and
    reused across the application.

    Returns:
        The Settings instance.

    Raises:
        ValidationError: If required environment variables are missing
            or have invalid values.
    """
    return Settings()


def clear_settings_cache() -> None:
    """Clear the settings cache.

    Useful for testing when you need to reload settings with
    different environment variables.
    """
    get_settings.cache_clear()
