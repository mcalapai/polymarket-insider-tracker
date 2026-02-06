"""Main pipeline orchestrator for Polymarket Insider Tracker.

This module provides the Pipeline class that wires together all detection
components and manages the event flow from ingestion to alerting.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

from py_clob_client.clob_types import ApiCreds
from redis.asyncio import Redis

from polymarket_insider_tracker.alerter.channels.discord import DiscordChannel
from polymarket_insider_tracker.alerter.channels.telegram import TelegramChannel
from polymarket_insider_tracker.alerter.dispatcher import AlertChannel, AlertDispatcher
from polymarket_insider_tracker.alerter.formatter import AlertFormatter
from polymarket_insider_tracker.config import Settings, get_settings
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.scorer import RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.sniper import SniperDetector
from polymarket_insider_tracker.detector.size_anomaly import (
    LiquidityInputs,
    SizeAnomalyDetector,
    SizeAnomalyError,
)
from polymarket_insider_tracker.detector.trade_size_outlier import TradeSizeOutlierDetector
from polymarket_insider_tracker.detector.digit_distribution import DigitDistributionDetector
from polymarket_insider_tracker.detector.trade_slicing import TradeSlicingDetector
from polymarket_insider_tracker.detector.order_to_trade_ratio import (
    OrderToTradeRatioConfig,
    OrderToTradeRatioDetector,
)
from polymarket_insider_tracker.detector.rapid_cancel import RapidCancelConfig, RapidCancelDetector
from polymarket_insider_tracker.detector.book_impact_without_fill import (
    BookImpactWithoutFillConfig,
    BookImpactWithoutFillDetector,
)
from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.ingestor.baselines import (
    RollingDigitConfig,
    RollingDigitDistributionCache,
    RollingHistogramCache,
    RollingHistogramConfig,
)
from polymarket_insider_tracker.ingestor.flow import RollingFlowConfig, RollingWalletMarketFlowCache
from polymarket_insider_tracker.ingestor.order_counts import RollingOrderCountCache, RollingOrderCountConfig
from polymarket_insider_tracker.ingestor.volume import RollingVolumeCache, RollingVolumeConfig
from polymarket_insider_tracker.ingestor.metadata_sync import MarketMetadataSync
from polymarket_insider_tracker.ingestor.clob_market_websocket import ClobMarketStreamHandler
from polymarket_insider_tracker.ingestor.websocket import TradeStreamHandler
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer, WalletAnalyzerError
from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.profiler.funding import FundingTracer
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.training.features import build_trade_features
from polymarket_insider_tracker.training.serving import LoadedModel, load_model_from_artifact, predict_proba
from polymarket_insider_tracker.storage.repos import (
    CancelDTO,
    CancelRepository,
    ERC20TransferRepository,
    LiquiditySnapshotDTO,
    LiquiditySnapshotRepository,
    MarketEntryRepository,
    MarketPriceBarRepository,
    MarketStateRepository,
    OrderDTO,
    OrderEventDTO,
    OrderEventRepository,
    OrderRepository,
    SniperClusterDTO,
    SniperClusterRepository,
    TradeDTO,
    TradeRepository,
    TradeSignalDTO,
    TradeSignalRepository,
    TradeFeatureRepository,
    WalletSnapshotDTO,
    WalletSnapshotRepository,
    ModelArtifactRepository,
    TradeProcessingErrorDTO,
    TradeProcessingErrorRepository,
)

if TYPE_CHECKING:
    from typing import Any

    from polymarket_insider_tracker.detector.models import (
        BookImpactWithoutFillSignal,
        CoEntryCorrelationSignal,
        FreshWalletSignal,
        FundingChainSignal,
        OrderToTradeRatioSignal,
        RapidCancelSignal,
        SizeAnomalySignal,
        SniperClusterSignal,
    )
    from polymarket_insider_tracker.ingestor.models import TradeEvent

logger = logging.getLogger(__name__)

SNIPER_WALLET_KEY_PREFIX = "polymarket:sniper:wallet:"
COENTRY_WALLET_KEY_PREFIX = "polymarket:coentry:wallet:"
FUNDING_WALLET_KEY_PREFIX = "polymarket:funding:wallet:"
BASELINE_MARKETS_SET_KEY = "polymarket:baseline:markets"
ORDERS_HOT_ASSETS_ZSET_KEY = "polymarket:orders:hot_assets"
ORDERS_WALLET_SIGNAL_KEY_PREFIX = "polymarket:orders:wallet:"


def _is_canceled_status(status: str) -> bool:
    s = status.upper()
    return s in {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}


def _parse_order_details(raw: dict[str, Any]) -> dict[str, Any] | None:
    try:
        order_hash = str(raw.get("order_id") or raw.get("order_hash") or "")
        maker = str(raw.get("maker_address") or raw.get("maker") or "").lower()
        market_id = str(raw.get("market") or raw.get("market_id") or "")
        asset_id = str(raw.get("asset_id") or "")
        side_raw = str(raw.get("side") or "").upper()
        side = "BUY" if side_raw == "BUY" else "SELL"
        price = Decimal(str(raw.get("price")))
        size = Decimal(str(raw.get("original_size") or raw.get("size")))
        size_matched = Decimal(str(raw.get("size_matched") or "0"))
        status = str(raw.get("status") or "")
        created_at_raw = raw.get("created_at")
        if created_at_raw is None:
            return None
        created_at_f = float(created_at_raw)
        if created_at_f > 1e12:
            created_at_f /= 1000.0
        created_ts = datetime.fromtimestamp(created_at_f, tz=UTC)
        if not (order_hash and maker and market_id and asset_id and status):
            return None
        return {
            "order_hash": order_hash,
            "wallet_address": maker,
            "market_id": market_id,
            "asset_id": asset_id,
            "side": side,
            "price": price,
            "size": size,
            "size_matched": size_matched,
            "status": status,
            "created_ts": created_ts,
        }
    except Exception:
        return None


def _safe_fill_ratio(*, size: Decimal, size_matched: Decimal) -> float:
    if size <= 0:
        return 0.0
    try:
        r = float(size_matched / size)
    except Exception:
        return 0.0
    return max(0.0, min(1.0, r))


def _compute_book_impact_bps(
    *,
    side: str,
    order_price: Decimal,
    prev_best_bid: Decimal | None,
    prev_best_ask: Decimal | None,
) -> tuple[bool, float | None]:
    if side == "BUY" and prev_best_bid is not None and prev_best_bid > 0:
        if order_price > prev_best_bid:
            bps = float((order_price - prev_best_bid) / prev_best_bid * Decimal("10000"))
            return True, max(0.0, bps)
        return False, None
    if side == "SELL" and prev_best_ask is not None and prev_best_ask > 0:
        if order_price < prev_best_ask:
            bps = float((prev_best_ask - order_price) / prev_best_ask * Decimal("10000"))
            return True, max(0.0, bps)
        return False, None
    return False, None


class PipelineState(str, Enum):
    """Pipeline lifecycle states."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class PipelineStats:
    """Statistics for the pipeline."""

    started_at: datetime | None = None
    trades_processed: int = 0
    signals_generated: int = 0
    alerts_sent: int = 0
    errors: int = 0
    last_trade_time: datetime | None = None
    last_error: str | None = None


class Pipeline:
    """Main pipeline orchestrator for the Polymarket Insider Tracker.

    This class wires together all detection components and manages the
    event flow from trade ingestion through profiling, detection, and alerting.

    Pipeline flow:
        WebSocket Trade Stream → Wallet Profiler → Detectors → Risk Scorer → Alerter

    Example:
        ```python
        from polymarket_insider_tracker.config import get_settings
        from polymarket_insider_tracker.pipeline import Pipeline

        settings = get_settings()
        pipeline = Pipeline(settings)

        await pipeline.start()
        # Pipeline runs until stop() is called
        await pipeline.stop()
        ```
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        dry_run: bool | None = None,
    ) -> None:
        """Initialize the pipeline.

        Args:
            settings: Application settings. If not provided, uses get_settings().
            dry_run: If True, skip sending alerts. Overrides settings.dry_run.
        """
        self._settings = settings or get_settings()
        self._dry_run = dry_run if dry_run is not None else self._settings.dry_run

        self._state = PipelineState.STOPPED
        self._stats = PipelineStats()

        # Components (initialized in start())
        self._redis: Redis | None = None
        self._db_manager: DatabaseManager | None = None
        self._polygon_client: PolygonClient | None = None
        self._clob_client: ClobClient | None = None
        self._metadata_sync: MarketMetadataSync | None = None
        self._wallet_analyzer: WalletAnalyzer | None = None
        self._funding_tracer: FundingTracer | None = None
        self._fresh_wallet_detector: FreshWalletDetector | None = None
        self._size_anomaly_detector: SizeAnomalyDetector | None = None
        self._sniper_detector: SniperDetector | None = None
        self._trade_size_outlier_detector: TradeSizeOutlierDetector | None = None
        self._digit_distribution_detector: DigitDistributionDetector | None = None
        self._trade_slicing_detector: TradeSlicingDetector | None = None
        self._risk_scorer: RiskScorer | None = None
        self._alert_formatter: AlertFormatter | None = None
        self._alert_dispatcher: AlertDispatcher | None = None
        self._trade_stream: TradeStreamHandler | None = None
        self._clob_market_stream: ClobMarketStreamHandler | None = None
        self._volume_cache: RollingVolumeCache | None = None
        self._hist_cache: RollingHistogramCache | None = None
        self._digit_cache: RollingDigitDistributionCache | None = None
        self._flow_cache: RollingWalletMarketFlowCache | None = None
        self._order_count_cache: RollingOrderCountCache | None = None
        self._order_to_trade_ratio_detector: OrderToTradeRatioDetector | None = None
        self._rapid_cancel_detector: RapidCancelDetector | None = None
        self._book_impact_detector: BookImpactWithoutFillDetector | None = None
        self._best_quotes_by_asset: dict[str, tuple[Decimal | None, Decimal | None]] = {}
        self._orders_subscribed_assets: set[str] = set()
        self._loaded_model: LoadedModel | None = None

        # Synchronization
        self._stop_event: asyncio.Event | None = None
        self._stream_task: asyncio.Task[None] | None = None
        self._sniper_task: asyncio.Task[None] | None = None
        self._baseline_task: asyncio.Task[None] | None = None
        self._orders_stream_task: asyncio.Task[None] | None = None
        self._orders_subscribe_task: asyncio.Task[None] | None = None

    @property
    def state(self) -> PipelineState:
        """Current pipeline state."""
        return self._state

    @property
    def stats(self) -> PipelineStats:
        """Current pipeline statistics."""
        return self._stats

    @property
    def is_running(self) -> bool:
        """Check if pipeline is running."""
        return self._state == PipelineState.RUNNING

    async def start(self) -> None:
        """Start the pipeline.

        Initializes all components and begins processing trades.

        Raises:
            RuntimeError: If pipeline is already running.
            Exception: If any component fails to initialize.
        """
        if self._state != PipelineState.STOPPED:
            raise RuntimeError(f"Cannot start pipeline in state {self._state}")

        self._state = PipelineState.STARTING
        self._stop_event = asyncio.Event()
        logger.info("Starting pipeline...")

        try:
            await self._initialize_components()
            await self._start_background_services()
            self._stats.started_at = datetime.now(UTC)
            self._state = PipelineState.RUNNING
            logger.info("Pipeline started successfully")
        except Exception as e:
            self._state = PipelineState.ERROR
            self._stats.last_error = str(e)
            logger.error("Failed to start pipeline: %s", e)
            await self._cleanup()
            raise

    async def stop(self) -> None:
        """Stop the pipeline gracefully.

        Stops all background services and cleans up resources.
        """
        if self._state == PipelineState.STOPPED:
            return

        self._state = PipelineState.STOPPING
        logger.info("Stopping pipeline...")

        if self._stop_event:
            self._stop_event.set()

        await self._stop_background_services()
        await self._cleanup()

        self._state = PipelineState.STOPPED
        logger.info("Pipeline stopped")

    async def _initialize_components(self) -> None:
        """Initialize all pipeline components."""
        settings = self._settings

        # Initialize Redis
        logger.debug("Initializing Redis connection...")
        self._redis = Redis.from_url(settings.redis.url)

        # Initialize Database Manager
        logger.debug("Initializing database manager...")
        self._db_manager = DatabaseManager(
            settings.database.url,
            async_mode=True,
        )

        # Initialize Polygon client
        logger.debug("Initializing Polygon client...")
        self._polygon_client = PolygonClient(
            settings.polygon.rpc_url,
            fallback_rpc_url=settings.polygon.fallback_rpc_url,
            redis=self._redis,
        )

        # Initialize CLOB client
        logger.debug("Initializing CLOB client...")
        private_key = (
            settings.polymarket.clob_private_key.get_secret_value()
            if settings.polymarket.clob_private_key
            else None
        )
        api_creds: ApiCreds | None = None
        if (
            settings.polymarket.clob_api_key
            and settings.polymarket.clob_api_secret
            and settings.polymarket.clob_api_passphrase
        ):
            api_creds = ApiCreds(
                api_key=settings.polymarket.clob_api_key.get_secret_value(),
                api_secret=settings.polymarket.clob_api_secret.get_secret_value(),
                api_passphrase=settings.polymarket.clob_api_passphrase.get_secret_value(),
            )
        self._clob_client = ClobClient(
            host=settings.polymarket.clob_host,
            chain_id=settings.polymarket.clob_chain_id,
            private_key=private_key,
            api_creds=api_creds,
            signature_type=settings.polymarket.clob_signature_type,
            funder=settings.polymarket.clob_funder,
        )

        # Initialize Market Metadata Sync
        logger.debug("Initializing market metadata sync...")
        async def on_liquidity_snapshot(snapshot: Any) -> None:
            from polymarket_insider_tracker.ingestor.liquidity import LiquiditySnapshot

            if not isinstance(snapshot, LiquiditySnapshot):
                return
            if not self._db_manager or not self._volume_cache:
                return
            rolling = await self._volume_cache.get_rolling_volume(
                snapshot.condition_id,
                as_of=snapshot.computed_at,
            )
            async with self._db_manager.get_async_session() as session:
                repo = LiquiditySnapshotRepository(session)
                await repo.upsert(
                    LiquiditySnapshotDTO(
                        condition_id=snapshot.condition_id,
                        asset_id=snapshot.asset_id,
                        computed_at=snapshot.computed_at,
                        rolling_24h_volume_usdc=rolling,
                        visible_book_depth_usdc=snapshot.visible_book_depth_usdc,
                        mid_price=snapshot.mid_price,
                    )
                )
                await session.commit()

        self._metadata_sync = MarketMetadataSync(
            redis=self._redis,
            clob_client=self._clob_client,
            hot_market_ttl_seconds=settings.liquidity.hot_market_ttl_seconds,
            depth_max_slippage_bps=settings.liquidity.depth_max_slippage_bps,
            on_liquidity_snapshot=on_liquidity_snapshot,
        )

        # Rolling volume cache (Redis)
        self._volume_cache = RollingVolumeCache(
            self._redis,
            config=RollingVolumeConfig(
                window=timedelta(hours=settings.liquidity.volume_window_hours),
            ),
        )
        self._hist_cache = RollingHistogramCache(
            self._redis,
            config=RollingHistogramConfig(
                window=timedelta(hours=settings.baseline.window_hours),
                bucket_seconds=settings.baseline.bucket_seconds,
                cleanup_batch_size=settings.baseline.cleanup_batch_size,
            ),
        )
        self._digit_cache = RollingDigitDistributionCache(
            self._redis,
            config=RollingDigitConfig(
                window=timedelta(hours=settings.baseline.window_hours),
                bucket_seconds=settings.baseline.bucket_seconds,
                cleanup_batch_size=settings.baseline.cleanup_batch_size,
            ),
        )
        self._flow_cache = RollingWalletMarketFlowCache(
            self._redis,
            config=RollingFlowConfig(
                window=timedelta(minutes=30),
                bucket_seconds=settings.baseline.bucket_seconds,
                cleanup_batch_size=settings.baseline.cleanup_batch_size,
            ),
        )

        if settings.orders.enabled:
            self._order_count_cache = RollingOrderCountCache(
                self._redis,
                config=RollingOrderCountConfig(
                    window=timedelta(minutes=settings.orders.lifecycle_window_minutes),
                    bucket_seconds=settings.baseline.bucket_seconds,
                    cleanup_batch_size=settings.baseline.cleanup_batch_size,
                ),
            )
            if not self._hist_cache:
                raise RuntimeError("Histogram cache must be initialized before order detectors")
            self._order_to_trade_ratio_detector = OrderToTradeRatioDetector(
                self._hist_cache,
                config=OrderToTradeRatioConfig(
                    window_minutes=settings.orders.lifecycle_window_minutes,
                    alert_percentile=0.99,
                    baseline_min_samples=settings.orders.baseline_min_samples,
                ),
            )
            self._rapid_cancel_detector = RapidCancelDetector(
                self._hist_cache,
                config=RapidCancelConfig(
                    rapid_percentile=settings.orders.rapid_cancel_percentile,
                    baseline_min_samples=settings.orders.baseline_min_samples,
                ),
            )
            self._book_impact_detector = BookImpactWithoutFillDetector(
                self._hist_cache,
                config=BookImpactWithoutFillConfig(
                    rapid_percentile=settings.orders.rapid_cancel_percentile,
                    impact_percentile=settings.orders.book_impact_percentile,
                    baseline_min_samples=settings.orders.baseline_min_samples,
                ),
            )

        # Initialize Wallet Analyzer
        logger.debug("Initializing wallet analyzer...")
        if not self._db_manager:
            raise RuntimeError("Database manager must be initialized before wallet analyzer")

        if not self._polygon_client:
            raise RuntimeError("Polygon client must be initialized before wallet analyzer")

        self._funding_tracer = FundingTracer(
            self._polygon_client,
            token_addresses=list(settings.funding.usdc_contract_addresses),
            lookback_days=settings.funding.lookback_days,
            logs_chunk_size_blocks=settings.funding.logs_chunk_size_blocks,
            max_hops=settings.funding.max_hops,
        )

        async def first_funding_at_provider(address: str, as_of: datetime) -> datetime:
            async with self._db_manager.get_async_session() as session:
                if not self._funding_tracer:
                    raise WalletAnalyzerError("Funding tracer is not initialized")
                first = await self._funding_tracer.ensure_first_inbound_transfer(
                    session,
                    address=address,
                    as_of=as_of,
                )
                if first.timestamp > as_of:
                    raise WalletAnalyzerError("first funding transfer is after as_of")
                return first.timestamp

        self._wallet_analyzer = WalletAnalyzer(
            self._polygon_client,
            redis=self._redis,
            usdc_addresses=tuple(settings.funding.usdc_contract_addresses),
            first_funding_at_provider=first_funding_at_provider,
        )

        # Initialize Detectors
        logger.debug("Initializing detectors...")
        self._fresh_wallet_detector = FreshWalletDetector(
            min_trade_size=settings.fresh_wallet.min_trade_notional_usdc,
            max_nonce=settings.fresh_wallet.max_nonce,
            max_age_hours=settings.fresh_wallet.max_age_hours,
        )
        self._size_anomaly_detector = SizeAnomalyDetector(
            volume_threshold=settings.size_anomaly.volume_threshold,
            book_threshold=settings.size_anomaly.book_threshold,
        )
        self._sniper_detector = (
            SniperDetector(
                entry_threshold_seconds=settings.sniper.entry_threshold_seconds,
                min_cluster_size=settings.sniper.min_cluster_size,
                eps=settings.sniper.eps,
                min_samples=settings.sniper.min_samples,
                min_entries_per_wallet=settings.sniper.min_entries_per_wallet,
                min_markets_in_common=settings.sniper.min_markets_in_common,
                coentry_top_n=settings.sniper.coentry_top_n,
                coentry_min_markets=settings.sniper.coentry_min_markets,
                coentry_min_shared_markets=settings.sniper.coentry_min_shared_markets,
            )
            if settings.sniper.enabled
            else None
        )
        if not self._hist_cache or not self._digit_cache or not self._flow_cache:
            raise RuntimeError("Baseline caches must be initialized before distribution detectors")
        self._trade_size_outlier_detector = TradeSizeOutlierDetector(self._hist_cache)
        self._digit_distribution_detector = DigitDistributionDetector(self._digit_cache)
        self._trade_slicing_detector = TradeSlicingDetector(self._flow_cache, self._hist_cache)

        # Initialize Risk Scorer
        logger.debug("Initializing risk scorer...")
        self._risk_scorer = RiskScorer(self._redis)

        # Load blessed model artifact for live scoring (strict when enabled).
        if settings.model.enabled:
            if not self._db_manager:
                raise RuntimeError("MODEL_ENABLED requires database")
            async with self._db_manager.get_async_session() as session:
                repo = ModelArtifactRepository(session)
                meta = await repo.get_latest_blessed()
                if meta is None:
                    raise RuntimeError("MODEL_ENABLED requires a blessed model in ml_models")
                self._loaded_model = load_model_from_artifact(
                    artifact_path=Path(meta.artifact_path),
                    schema_json=meta.schema_json,
                )

        # Initialize Alerting
        logger.debug("Initializing alerting components...")
        self._alert_formatter = AlertFormatter(verbosity="detailed")
        channels = self._build_alert_channels()
        self._alert_dispatcher = AlertDispatcher(channels)

        # Initialize Trade Stream
        logger.debug("Initializing trade stream handler...")
        self._trade_stream = TradeStreamHandler(
            on_trade=self._on_trade,
            host=settings.polymarket.trade_ws_url,
        )

        if settings.orders.enabled:
            logger.debug("Initializing CLOB market stream handler (orders)...")
            self._clob_market_stream = ClobMarketStreamHandler(
                host=settings.polymarket.clob_market_ws_url,
                on_book=self._on_clob_book,
                on_price_change=self._on_clob_price_change,
            )

        logger.info("All components initialized")

    def _build_alert_channels(self) -> list[AlertChannel]:
        """Build list of enabled alert channels."""
        channels: list[AlertChannel] = []
        settings = self._settings

        if settings.discord.enabled and settings.discord.webhook_url:
            webhook_url = settings.discord.webhook_url.get_secret_value()
            channels.append(DiscordChannel(webhook_url))
            logger.info("Discord channel enabled")

        if settings.telegram.enabled:
            bot_token = settings.telegram.bot_token
            chat_id = settings.telegram.chat_id
            if bot_token and chat_id:
                channels.append(
                    TelegramChannel(
                        bot_token.get_secret_value(),
                        chat_id,
                    )
                )
                logger.info("Telegram channel enabled")

        if not channels:
            logger.warning("No alert channels configured")

        return channels

    async def _start_background_services(self) -> None:
        """Start background services."""
        # Start metadata sync
        if self._metadata_sync:
            logger.debug("Starting metadata sync service...")
            await self._metadata_sync.start()

        # Start trade stream in background task
        if self._trade_stream:
            logger.debug("Starting trade stream...")
            self._stream_task = asyncio.create_task(self._run_trade_stream())

        if self._sniper_detector and self._redis:
            logger.debug("Starting sniper clustering loop...")
            self._sniper_task = asyncio.create_task(self._run_sniper_clustering_loop())

        if self._redis and self._db_manager and self._hist_cache and self._digit_cache:
            logger.debug("Starting baseline persistence loop...")
            self._baseline_task = asyncio.create_task(self._run_baseline_persist_loop())

        if self._clob_market_stream and self._redis:
            logger.debug("Starting CLOB market stream (orders)...")
            self._orders_stream_task = asyncio.create_task(self._run_clob_market_stream())
            logger.debug("Starting order subscription reconciliation loop...")
            self._orders_subscribe_task = asyncio.create_task(self._run_orders_subscription_loop())

    async def _run_baseline_persist_loop(self) -> None:
        if not self._stop_event or not self._redis or not self._db_manager:
            return
        if not self._hist_cache or not self._digit_cache:
            return

        interval = self._settings.baseline.persist_interval_seconds
        while not self._stop_event.is_set():
            try:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                    break
                except TimeoutError:
                    pass

                markets = await self._redis.smembers(BASELINE_MARKETS_SET_KEY)
                if not markets:
                    continue

                now = datetime.now(UTC)
                day = now.date()
                async with self._db_manager.get_async_session() as session:
                    from polymarket_insider_tracker.storage.repos import (
                        MarketDailyBaselineDTO,
                        MarketDailyBaselineRepository,
                    )

                    repo = MarketDailyBaselineRepository(session)
                    for raw in markets:
                        market_id = raw.decode() if isinstance(raw, bytes) else str(raw)
                        hist = await self._hist_cache.get_histogram_counts(market_id=market_id, as_of=now)
                        digits = await self._digit_cache.get_distribution(market_id=market_id, as_of=now)
                        await repo.upsert(
                            MarketDailyBaselineDTO(
                                market_id=market_id,
                                day=day,
                                baseline_type="trade_notional_hist",
                                payload_json=json.dumps(hist),
                                computed_at=now,
                            )
                        )
                        await repo.upsert(
                            MarketDailyBaselineDTO(
                                market_id=market_id,
                                day=day,
                                baseline_type="first_digit",
                                payload_json=json.dumps(digits),
                                computed_at=now,
                            )
                        )
                    await session.commit()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Baseline persistence loop error: %s", e)

    async def _run_trade_stream(self) -> None:
        """Run the trade stream in a task."""
        if not self._trade_stream:
            return

        try:
            await self._trade_stream.start()
        except asyncio.CancelledError:
            logger.debug("Trade stream task cancelled")
        except Exception as e:
            logger.error("Trade stream error: %s", e)
            self._stats.last_error = str(e)
            self._stats.errors += 1

    def _orders_wallet_key(self, wallet_address: str) -> str:
        return f"{ORDERS_WALLET_SIGNAL_KEY_PREFIX}{wallet_address.lower()}"

    async def _run_clob_market_stream(self) -> None:
        if not self._clob_market_stream:
            return
        try:
            await self._clob_market_stream.start()
        except asyncio.CancelledError:
            logger.debug("CLOB market stream task cancelled")
        except Exception as e:
            logger.error("CLOB market stream error: %s", e)
            self._stats.last_error = str(e)
            self._stats.errors += 1

    async def _run_orders_subscription_loop(self) -> None:
        if not self._stop_event or not self._redis or not self._clob_market_stream:
            return

        interval = self._settings.orders.resubscribe_interval_seconds
        ttl_seconds = self._settings.liquidity.hot_market_ttl_seconds
        cutoff_seconds = int(ttl_seconds)

        while not self._stop_event.is_set():
            try:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                    break
                except TimeoutError:
                    pass

                now = datetime.now(UTC)
                cutoff = int((now - timedelta(seconds=cutoff_seconds)).timestamp())
                # Prune old assets from the registry.
                await self._redis.zremrangebyscore(ORDERS_HOT_ASSETS_ZSET_KEY, min=0, max=cutoff - 1)

                # Select most recent assets up to max_subscribed_assets.
                raw_assets = await self._redis.zrevrangebyscore(
                    ORDERS_HOT_ASSETS_ZSET_KEY,
                    max="+inf",
                    min=cutoff,
                    start=0,
                    num=self._settings.orders.max_subscribed_assets,
                )
                assets = {
                    a.decode() if isinstance(a, bytes) else str(a)
                    for a in raw_assets
                    if a and (a.decode() if isinstance(a, bytes) else str(a))
                }

                to_subscribe = assets - self._orders_subscribed_assets
                to_unsubscribe = self._orders_subscribed_assets - assets

                if to_unsubscribe:
                    await self._clob_market_stream.request_unsubscribe(to_unsubscribe)
                    self._orders_subscribed_assets -= to_unsubscribe
                if to_subscribe:
                    await self._clob_market_stream.request_subscribe(to_subscribe)
                    self._orders_subscribed_assets |= to_subscribe
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Order subscription loop error: %s", e)

    async def _on_clob_book(self, book: Any) -> None:
        # Lazy import typing to avoid circular imports.
        from polymarket_insider_tracker.ingestor.models import ClobBookEvent

        if not isinstance(book, ClobBookEvent):
            return
        best_bid = book.bids[0].price if book.bids else None
        best_ask = book.asks[0].price if book.asks else None
        self._best_quotes_by_asset[book.asset_id] = (best_bid, best_ask)

    async def _on_clob_price_change(self, pc: Any) -> None:
        from polymarket_insider_tracker.ingestor.models import ClobPriceChangeEvent

        if not isinstance(pc, ClobPriceChangeEvent):
            return
        if not self._clob_client or not self._db_manager or not self._redis or not self._order_count_cache:
            return
        if not (self._rapid_cancel_detector and self._book_impact_detector):
            return

        # Track best quotes before applying the event.
        prev_best_bid, prev_best_ask = self._best_quotes_by_asset.get(pc.asset_id, (None, None))
        # Update after values for subsequent events.
        self._best_quotes_by_asset[pc.asset_id] = (pc.best_bid, pc.best_ask)

        async with self._db_manager.get_async_session() as session:
            order_repo = OrderRepository(session)
            event_repo = OrderEventRepository(session)
            cancel_repo = CancelRepository(session)

            for change in pc.price_changes:
                order_hash = change.order_hash
                if not order_hash:
                    continue
                try:
                    raw = await asyncio.to_thread(self._clob_client.get_order, order_hash)
                except Exception as e:
                    logger.warning("Failed to fetch order details for %s: %s", order_hash, e)
                    continue

                details = _parse_order_details(raw)
                if details is None:
                    continue

                await order_repo.upsert(
                    OrderDTO(
                        order_hash=details["order_hash"],
                        market_id=details["market_id"],
                        asset_id=details["asset_id"],
                        wallet_address=details["wallet_address"],
                        side=details["side"],
                        price=details["price"],
                        size=details["size"],
                        size_matched=details["size_matched"],
                        status=details["status"],
                        created_ts=details["created_ts"],
                    )
                )

                # Determine lifecycle event type (placed/canceled/updated) via status transition cache.
                status_key = f"polymarket:orders:status:{order_hash}"
                prev_status_raw = await self._redis.get(status_key)
                prev_status = prev_status_raw.decode() if isinstance(prev_status_raw, bytes) else prev_status_raw
                new_status = details["status"]
                await self._redis.set(status_key, new_status, ex=self._settings.orders.order_details_cache_ttl_seconds)

                event_type = "UPDATED"
                if prev_status is None:
                    event_type = "PLACED"
                    await self._order_count_cache.record(
                        wallet=details["wallet_address"],
                        market_id=details["market_id"],
                        ts=pc.timestamp,
                        kind="placed",
                    )
                if prev_status is not None and prev_status != new_status and _is_canceled_status(new_status):
                    event_type = "CANCELED"
                    await self._order_count_cache.record(
                        wallet=details["wallet_address"],
                        market_id=details["market_id"],
                        ts=pc.timestamp,
                        kind="canceled",
                    )

                await event_repo.insert(
                    OrderEventDTO(
                        order_hash=order_hash,
                        market_id=details["market_id"],
                        asset_id=details["asset_id"],
                        wallet_address=details["wallet_address"],
                        event_type=event_type,
                        ts=pc.timestamp,
                        best_bid=pc.best_bid,
                        best_ask=pc.best_ask,
                    )
                )

                # On cancel transitions, compute cancel-derived spoofing signals.
                if event_type == "CANCELED":
                    cancel_latency = (pc.timestamp - details["created_ts"]).total_seconds()
                    if cancel_latency < 0:
                        continue

                    moved_best, impact_bps = _compute_book_impact_bps(
                        side=details["side"],
                        order_price=details["price"],
                        prev_best_bid=prev_best_bid,
                        prev_best_ask=prev_best_ask,
                    )
                    fill_ratio = _safe_fill_ratio(size=details["size"], size_matched=details["size_matched"])

                    await cancel_repo.upsert(
                        CancelDTO(
                            order_hash=order_hash,
                            market_id=details["market_id"],
                            asset_id=details["asset_id"],
                            wallet_address=details["wallet_address"],
                            created_ts=details["created_ts"],
                            canceled_at=pc.timestamp,
                            cancel_latency_seconds=Decimal(str(cancel_latency)),
                            size=details["size"],
                            size_matched=details["size_matched"],
                            moved_best=moved_best,
                            impact_bps=Decimal(str(impact_bps)) if impact_bps is not None else None,
                        )
                    )

                    rapid = await self._rapid_cancel_detector.analyze_cancel(
                        wallet_address=details["wallet_address"],
                        market_id=details["market_id"],
                        order_hash=order_hash,
                        canceled_at=pc.timestamp,
                        cancel_latency_seconds=cancel_latency,
                        fill_ratio=fill_ratio,
                    )
                    impact = await self._book_impact_detector.analyze_cancel(
                        wallet_address=details["wallet_address"],
                        market_id=details["market_id"],
                        order_hash=order_hash,
                        canceled_at=pc.timestamp,
                        moved_best=moved_best,
                        impact_bps=impact_bps,
                        cancel_latency_seconds=cancel_latency,
                        fill_ratio=fill_ratio,
                    )

                    if rapid or impact:
                        payload: dict[str, object] = {"ts": pc.timestamp.isoformat()}
                        if rapid:
                            payload["rapid_cancel"] = rapid.to_dict()
                        if impact:
                            payload["book_impact_without_fill"] = impact.to_dict()
                        await self._redis.set(
                            self._orders_wallet_key(details["wallet_address"]),
                            json.dumps(payload, default=str),
                            ex=int(timedelta(minutes=self._settings.orders.lifecycle_window_minutes).total_seconds()),
                        )

            await session.commit()

    def _sniper_wallet_key(self, wallet_address: str) -> str:
        return f"{SNIPER_WALLET_KEY_PREFIX}{wallet_address.lower()}"

    def _coentry_wallet_key(self, wallet_address: str) -> str:
        return f"{COENTRY_WALLET_KEY_PREFIX}{wallet_address.lower()}"

    def _funding_wallet_key(self, wallet_address: str) -> str:
        return f"{FUNDING_WALLET_KEY_PREFIX}{wallet_address.lower()}"

    async def _run_sniper_clustering_loop(self) -> None:
        if not self._sniper_detector or not self._redis:
            return
        if not self._stop_event:
            return

        interval = self._settings.sniper.run_interval_seconds
        ttl_seconds = int(timedelta(days=self._settings.sniper.window_days).total_seconds())
        if not self._db_manager:
            raise RuntimeError("Sniper clustering requires database manager")

        while not self._stop_event.is_set():
            try:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                    break
                except TimeoutError:
                    pass

                window_end = datetime.now(UTC)
                window_start = window_end - timedelta(days=self._settings.sniper.window_days)
                async with self._db_manager.get_async_session() as session:
                    entry_repo = MarketEntryRepository(session)
                    entries_dto = await entry_repo.list_since(since=window_start)

                    from polymarket_insider_tracker.detector.sniper import MarketEntry as SniperMarketEntry

                    entries = [
                        SniperMarketEntry(
                            wallet_address=e.wallet_address,
                            market_id=e.market_id,
                            entry_delta_seconds=float(e.entry_delta_seconds),
                            position_size=e.notional_usdc,
                            entry_rank=e.entry_rank,
                            timestamp=e.ts,
                        )
                        for e in entries_dto
                    ]

                    clusters, signals = await asyncio.to_thread(
                        self._sniper_detector.detect_clusters,
                        entries,
                        window_start=window_start,
                        window_end=window_end,
                    )
                    coentry_signals = await asyncio.to_thread(
                        self._sniper_detector.detect_coentry,
                        entries,
                        window_start=window_start,
                        window_end=window_end,
                    )

                    cluster_repo = SniperClusterRepository(session)
                    for cluster in clusters:
                        await cluster_repo.upsert_cluster(
                            SniperClusterDTO(
                                cluster_id=cluster.cluster_id,
                                cluster_size=len(cluster.wallet_addresses),
                                avg_entry_delta_seconds=Decimal(str(cluster.avg_entry_delta)),
                                markets_in_common=cluster.markets_in_common,
                                confidence=Decimal(str(cluster.confidence)),
                                window_start=window_start,
                                window_end=window_end,
                                computed_at=window_end,
                            ),
                            member_wallets=set(cluster.wallet_addresses),
                        )
                    await cluster_repo.prune_old_clusters(keep_since=window_start)
                    await session.commit()

                for signal in signals:
                    await self._redis.set(
                        self._sniper_wallet_key(signal.wallet_address),
                        json.dumps(signal.to_dict()),
                        ex=ttl_seconds,
                    )
                for signal in coentry_signals:
                    await self._redis.set(
                        self._coentry_wallet_key(signal.wallet_address),
                        json.dumps(signal.to_dict()),
                        ex=ttl_seconds,
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Sniper clustering loop error: %s", e)

    async def _get_sniper_signal(self, wallet_address: str) -> SniperClusterSignal | None:
        if not self._redis:
            return None
        raw = await self._redis.get(self._sniper_wallet_key(wallet_address))
        if not raw:
            if not self._db_manager:
                return None
            async with self._db_manager.get_async_session() as session:
                repo = SniperClusterRepository(session)
                cluster = await repo.get_latest_cluster_for_wallet(wallet_address)
                if not cluster:
                    return None
                return SniperClusterSignal(
                    wallet_address=wallet_address.lower(),
                    cluster_id=cluster.cluster_id,
                    cluster_size=cluster.cluster_size,
                    avg_entry_delta_seconds=float(cluster.avg_entry_delta_seconds),
                    markets_in_common=cluster.markets_in_common,
                    confidence=float(cluster.confidence),
                )
        try:
            if isinstance(raw, bytes):
                raw = raw.decode()
            data = json.loads(str(raw))
            return SniperClusterSignal(
                wallet_address=str(data["wallet_address"]),
                cluster_id=str(data["cluster_id"]),
                cluster_size=int(data["cluster_size"]),
                avg_entry_delta_seconds=float(data["avg_entry_delta_seconds"]),
                markets_in_common=int(data["markets_in_common"]),
                confidence=float(data["confidence"]),
            )
        except Exception as e:
            logger.warning("Failed to parse sniper signal for %s: %s", wallet_address, e)
            return None

    async def _get_coentry_signal(self, wallet_address: str) -> CoEntryCorrelationSignal | None:
        if not self._redis:
            return None
        raw = await self._redis.get(self._coentry_wallet_key(wallet_address))
        if not raw:
            return None
        try:
            if isinstance(raw, bytes):
                raw = raw.decode()
            data = json.loads(str(raw))
            from polymarket_insider_tracker.detector.models import CoEntryCorrelationSignal

            return CoEntryCorrelationSignal(
                wallet_address=str(data["wallet_address"]),
                top_partner_wallet=str(data["top_partner_wallet"]),
                shared_markets=int(data["shared_markets"]),
                markets_considered=int(data["markets_considered"]),
                confidence=float(data["confidence"]),
            )
        except Exception as e:
            logger.warning("Failed to parse co-entry signal for %s: %s", wallet_address, e)
            return None

    async def _get_funding_signal(self, wallet_address: str) -> FundingChainSignal | None:
        if not self._redis:
            return None
        raw = await self._redis.get(self._funding_wallet_key(wallet_address))
        if not raw:
            return None
        try:
            if isinstance(raw, bytes):
                raw = raw.decode()
            data = json.loads(str(raw))
            from polymarket_insider_tracker.detector.models import FundingChainSignal

            return FundingChainSignal(
                wallet_address=str(data["wallet_address"]),
                origin_address=str(data["origin_address"]),
                origin_type=str(data["origin_type"]),
                hop_count=int(data["hop_count"]),
                suspiciousness=float(data["suspiciousness"]),
                confidence=float(data["confidence"]),
            )
        except Exception as e:
            logger.warning("Failed to parse funding signal for %s: %s", wallet_address, e)
            return None

    async def _get_order_signals(
        self,
        *,
        wallet_address: str,
        market_id: str,
        as_of: datetime,
    ) -> tuple[OrderToTradeRatioSignal | None, RapidCancelSignal | None, BookImpactWithoutFillSignal | None]:
        if not (self._settings.orders.enabled and self._redis):
            return (None, None, None)

        ratio_signal: OrderToTradeRatioSignal | None = None
        if self._order_to_trade_ratio_detector and self._order_count_cache and self._flow_cache:
            counts = await self._order_count_cache.get_window(
                wallet=wallet_address, market_id=market_id, as_of=as_of
            )
            _, trade_count = await self._flow_cache.get_window(
                wallet=wallet_address, market_id=market_id, as_of=as_of
            )
            ratio_signal = await self._order_to_trade_ratio_detector.analyze(
                wallet_address=wallet_address,
                market_id=market_id,
                as_of=as_of,
                orders_placed=int(counts.get("placed", 0)),
                trades_executed=int(trade_count),
            )

        rapid: RapidCancelSignal | None = None
        impact: BookImpactWithoutFillSignal | None = None
        raw = await self._redis.get(self._orders_wallet_key(wallet_address))
        if raw:
            try:
                if isinstance(raw, bytes):
                    raw = raw.decode()
                data = json.loads(str(raw))
                ts_raw = data.get("ts")
                if isinstance(ts_raw, str):
                    last_ts = datetime.fromisoformat(ts_raw)
                else:
                    last_ts = None
                if last_ts and (as_of - last_ts) <= timedelta(minutes=self._settings.orders.lifecycle_window_minutes):
                    from polymarket_insider_tracker.detector.models import (
                        BookImpactWithoutFillSignal,
                        RapidCancelSignal,
                    )

                    if isinstance(data.get("rapid_cancel"), dict):
                        rc = data["rapid_cancel"]
                        rapid = RapidCancelSignal(
                            wallet_address=str(rc["wallet_address"]),
                            market_id=str(rc["market_id"]),
                            order_hash=str(rc["order_hash"]),
                            cancel_latency_seconds=float(rc["cancel_latency_seconds"]),
                            latency_percentile=float(rc["latency_percentile"]),
                            fill_ratio=float(rc["fill_ratio"]),
                            confidence=float(rc["confidence"]),
                            timestamp=datetime.fromisoformat(str(rc["timestamp"])),
                        )
                    if isinstance(data.get("book_impact_without_fill"), dict):
                        bi = data["book_impact_without_fill"]
                        impact = BookImpactWithoutFillSignal(
                            wallet_address=str(bi["wallet_address"]),
                            market_id=str(bi["market_id"]),
                            order_hash=str(bi["order_hash"]),
                            moved_best=bool(bi["moved_best"]),
                            impact_bps=float(bi["impact_bps"]),
                            impact_percentile=float(bi["impact_percentile"]),
                            cancel_latency_seconds=float(bi["cancel_latency_seconds"]),
                            latency_percentile=float(bi["latency_percentile"]),
                            fill_ratio=float(bi["fill_ratio"]),
                            confidence=float(bi["confidence"]),
                            timestamp=datetime.fromisoformat(str(bi["timestamp"])),
                        )
            except Exception as e:
                logger.debug("Failed to parse order signals for %s: %s", wallet_address, e)

        return (ratio_signal, rapid, impact)

    async def _compute_funding_signal(self, wallet_address: str, *, as_of: datetime) -> FundingChainSignal:
        if not self._funding_tracer or not self._db_manager:
            raise RuntimeError("Funding tracer and database are required")
        from polymarket_insider_tracker.detector.models import FundingChainSignal

        async with self._db_manager.get_async_session() as session:
            chain = await self._funding_tracer.trace(
                session,
                address=wallet_address,
                as_of=as_of,
            )
            await self._funding_tracer.persist_relationships(
                session,
                chain,
                shares_funder_time_band_minutes=self._settings.funding.shares_funder_time_band_minutes,
                funding_burst_min_wallets=self._settings.funding.funding_burst_min_wallets,
            )
            await session.commit()

        suspiciousness = self._funding_tracer.get_suspiciousness_score(chain)
        return FundingChainSignal(
            wallet_address=wallet_address.lower(),
            origin_address=chain.origin_address.lower(),
            origin_type=chain.origin_type,
            hop_count=chain.hop_count,
            suspiciousness=suspiciousness,
            confidence=suspiciousness,
        )

    async def _stop_background_services(self) -> None:
        """Stop background services."""
        # Stop trade stream
        if self._trade_stream:
            logger.debug("Stopping trade stream...")
            await self._trade_stream.stop()

        if self._clob_market_stream:
            logger.debug("Stopping CLOB market stream (orders)...")
            await self._clob_market_stream.stop()

        # Cancel stream task
        if self._stream_task:
            self._stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._stream_task
            self._stream_task = None

        if self._orders_stream_task:
            self._orders_stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._orders_stream_task
            self._orders_stream_task = None

        if self._orders_subscribe_task:
            self._orders_subscribe_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._orders_subscribe_task
            self._orders_subscribe_task = None

        # Cancel sniper clustering task
        if self._sniper_task:
            self._sniper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sniper_task
            self._sniper_task = None

        if self._baseline_task:
            self._baseline_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._baseline_task
            self._baseline_task = None

        # Stop metadata sync
        if self._metadata_sync:
            logger.debug("Stopping metadata sync...")
            await self._metadata_sync.stop()

    async def _cleanup(self) -> None:
        """Clean up resources."""
        # Close database connections
        if self._db_manager:
            await self._db_manager.dispose_async()
            self._db_manager = None

        # Close Redis connection
        if self._redis:
            await self._redis.aclose()
            self._redis = None

        logger.debug("Resources cleaned up")

    async def _on_trade(self, trade: TradeEvent) -> None:
        """Process a single trade event.

        This is the main event handler that runs the detection pipeline:
        1. Run fresh wallet detection
        2. Run size anomaly detection
        3. Score the combined signals
        4. Send alert if threshold exceeded

        Args:
            trade: The trade event from the WebSocket stream.
        """
        self._stats.trades_processed += 1
        self._stats.last_trade_time = datetime.now(UTC)

        try:
            # Track hot markets for bounded liquidity computation
            if self._metadata_sync:
                await self._metadata_sync.mark_hot_market(trade.market_id)

            if self._redis and self._settings.orders.enabled:
                # Bounded order lifecycle ingestion subscribes to hot assets.
                await self._redis.zadd(
                    ORDERS_HOT_ASSETS_ZSET_KEY,
                    {trade.asset_id: int(trade.timestamp.timestamp())},
                )

            # Persist trade + update rolling volume cache
            if self._volume_cache:
                await self._volume_cache.record_trade_notional(
                    trade.market_id,
                    ts=trade.timestamp,
                    notional_usdc=trade.notional_value,
                )
            if self._hist_cache:
                await self._hist_cache.record(
                    market_id=trade.market_id,
                    ts=trade.timestamp,
                    notional_usdc=trade.notional_value,
                )
            if self._digit_cache:
                await self._digit_cache.record(
                    market_id=trade.market_id,
                    ts=trade.timestamp,
                    notional_usdc=trade.notional_value,
                )
            if self._flow_cache:
                await self._flow_cache.record(
                    wallet=trade.wallet_address,
                    market_id=trade.market_id,
                    ts=trade.timestamp,
                    notional_usdc=trade.notional_value,
                )
            if self._redis:
                await self._redis.sadd(BASELINE_MARKETS_SET_KEY, trade.market_id)
                # Refresh TTL to keep the set bounded to active markets.
                await self._redis.expire(
                    BASELINE_MARKETS_SET_KEY,
                    int(timedelta(hours=self._settings.baseline.window_hours).total_seconds())
                    + self._settings.baseline.bucket_seconds,
                )
            if self._db_manager:
                async with self._db_manager.get_async_session() as session:
                    trade_repo = TradeRepository(session)
                    await trade_repo.upsert(
                        TradeDTO(
                            trade_id=trade.trade_id,
                            market_id=trade.market_id,
                            asset_id=trade.asset_id,
                            wallet_address=trade.wallet_address,
                            side=trade.side,
                            outcome=trade.outcome,
                            outcome_index=trade.outcome_index,
                            price=trade.price,
                            size=trade.size,
                            notional_usdc=trade.notional_value,
                            ts=trade.timestamp,
                        )
                    )
                    price_repo = MarketPriceBarRepository(session)
                    await price_repo.upsert_trade_price(
                        market_id=trade.market_id,
                        ts=trade.timestamp,
                        price=trade.price,
                    )

                    # Maintain market anchor + entry events (sniper clustering inputs)
                    market_state_repo = MarketStateRepository(session)
                    market_state = await market_state_repo.ensure_first_trade_at(
                        trade.market_id,
                        ts=trade.timestamp,
                    )
                    entry_repo = MarketEntryRepository(session)
                    await entry_repo.insert_first_entry(
                        market_id=trade.market_id,
                        wallet_address=trade.wallet_address,
                        ts=trade.timestamp,
                        notional_usdc=trade.notional_value,
                        first_trade_at=market_state.first_trade_at,
                    )

            # Run detectors in parallel (strict: capture failures explicitly).
            detector_results = await asyncio.gather(
                self._detect_fresh_wallet(trade),
                self._detect_size_anomaly(trade),
                self._detect_trade_size_outlier(trade),
                self._detect_digit_distribution(trade),
                self._detect_trade_slicing(trade),
                return_exceptions=True,
            )

            detector_names = [
                "fresh_wallet",
                "size_anomaly",
                "trade_size_outlier",
                "digit_distribution",
                "trade_slicing",
            ]
            errors: list[TradeProcessingErrorDTO] = []
            normalized: list[object | None] = []
            for name, res in zip(detector_names, detector_results, strict=True):
                if isinstance(res, BaseException):
                    errors.append(
                        TradeProcessingErrorDTO(
                            trade_id=trade.trade_id,
                            stage=name,
                            error_type=res.__class__.__name__,
                            message=str(res),
                            created_at=datetime.now(UTC),
                        )
                    )
                    normalized.append(None)
                else:
                    normalized.append(res)

            fresh_signal, size_signal, outlier_signal, digit_signal, slicing_signal = normalized  # type: ignore[misc]

            if errors and self._db_manager:
                async with self._db_manager.get_async_session() as session:
                    repo = TradeProcessingErrorRepository(session)
                    await repo.insert_many(errors)
                self._stats.errors += len(errors)

            # Strictness: do not produce partial assessments/alerts when required
            # detectors fail. Persist the errors and fail this unit of work.
            if errors:
                self._stats.last_error = "; ".join(f"{e.stage}:{e.error_type}" for e in errors)
                logger.error(
                    "Trade %s failed detector stages: %s",
                    trade.trade_id,
                    ", ".join(f"{e.stage}({e.error_type})" for e in errors),
                )
                return

            sniper_signal, coentry_signal, funding_signal = await asyncio.gather(
                self._get_sniper_signal(trade.wallet_address),
                self._get_coentry_signal(trade.wallet_address),
                self._get_funding_signal(trade.wallet_address),
            )

            order_ratio_signal, rapid_cancel_signal, book_impact_signal = await self._get_order_signals(
                wallet_address=trade.wallet_address,
                market_id=trade.market_id,
                as_of=trade.timestamp,
            )

            # Bundle signals
            bundle = SignalBundle(
                trade_event=trade,
                fresh_wallet_signal=fresh_signal,
                size_anomaly_signal=size_signal,
                sniper_cluster_signal=sniper_signal,
                coentry_signal=coentry_signal,
                funding_signal=funding_signal,
                trade_size_outlier_signal=outlier_signal,
                digit_distribution_signal=digit_signal,
                trade_slicing_signal=slicing_signal,
                order_to_trade_ratio_signal=order_ratio_signal,
                rapid_cancel_signal=rapid_cancel_signal,
                book_impact_without_fill_signal=book_impact_signal,
            )

            if self._loaded_model is not None:
                from polymarket_insider_tracker.detector.models import ModelScoreSignal

                score = predict_proba(bundle, loaded=self._loaded_model)
                model_signal = ModelScoreSignal(
                    trade_id=trade.trade_id,
                    score=score,
                    confidence=score,
                )
                bundle = dataclasses.replace(bundle, model_score_signal=model_signal)

            # Persist computed signals for audit/backtests (durable truth).
            if self._db_manager and any(
                [
                    fresh_signal,
                    size_signal,
                    sniper_signal,
                    coentry_signal,
                    funding_signal,
                    outlier_signal,
                    digit_signal,
                    slicing_signal,
                    order_ratio_signal,
                    rapid_cancel_signal,
                    book_impact_signal,
                    bundle.model_score_signal,
                ]
            ):
                async with self._db_manager.get_async_session() as session:
                    signal_repo = TradeSignalRepository(session)
                    computed_at = datetime.now(UTC)
                    for signal_type, signal in (
                        ("fresh_wallet", fresh_signal),
                        ("size_anomaly", size_signal),
                        ("sniper_cluster", sniper_signal),
                        ("coentry", coentry_signal),
                        ("funding", funding_signal),
                        ("trade_size_outlier", outlier_signal),
                        ("digit_distribution", digit_signal),
                        ("trade_slicing", slicing_signal),
                        ("order_to_trade_ratio", order_ratio_signal),
                        ("rapid_cancel", rapid_cancel_signal),
                        ("book_impact_without_fill", book_impact_signal),
                        ("model_score", bundle.model_score_signal),
                    ):
                        if signal is None:
                            continue
                        await signal_repo.upsert(
                            TradeSignalDTO(
                                trade_id=trade.trade_id,
                                signal_type=signal_type,
                                confidence=Decimal(str(signal.confidence)),
                                payload_json=json.dumps(signal.to_dict()),
                                computed_at=computed_at,
                            )
                        )
                    await session.commit()

            # Score/persist features and potentially alert (always, strict audit trail).
            await self._score_and_alert(bundle)

        except Exception as e:
            logger.error("Error processing trade %s: %s", trade.trade_id, e)
            self._stats.errors += 1
            self._stats.last_error = str(e)

    async def _detect_fresh_wallet(self, trade: TradeEvent) -> FreshWalletSignal | None:
        """Run fresh wallet detection."""
        if not self._fresh_wallet_detector or not self._wallet_analyzer:
            raise RuntimeError("Fresh wallet detector is not initialized")

        if trade.notional_value < self._settings.fresh_wallet.min_trade_notional_usdc:
            return None

        wallet_snapshot = await self._wallet_analyzer.analyze(
            trade.wallet_address,
            as_of=trade.timestamp,
        )

        # Persist snapshot (durable truth)
        if self._db_manager:
            async with self._db_manager.get_async_session() as session:
                repo = WalletSnapshotRepository(session)
                await repo.upsert(
                    WalletSnapshotDTO(
                        address=wallet_snapshot.address,
                        as_of_block_number=wallet_snapshot.as_of_block_number,
                        as_of=wallet_snapshot.as_of,
                        nonce_as_of=wallet_snapshot.nonce_as_of,
                        first_funding_at=wallet_snapshot.first_funding_at,
                        age_hours_as_of=Decimal(str(wallet_snapshot.age_hours_as_of)),
                        matic_balance_wei_as_of=wallet_snapshot.matic_balance_wei_as_of,
                        usdc_balance_units_as_of=wallet_snapshot.usdc_balance_units_as_of,
                        computed_at=wallet_snapshot.computed_at,
                    )
                )

        return await self._fresh_wallet_detector.analyze(
            trade,
            wallet_snapshot=wallet_snapshot,
        )

    async def _detect_size_anomaly(self, trade: TradeEvent) -> SizeAnomalySignal | None:
        """Run size anomaly detection."""
        if not self._size_anomaly_detector or not self._metadata_sync or not self._volume_cache:
            raise RuntimeError("Size anomaly detector is not initialized")

        ob_liquidity = await self._metadata_sync.get_liquidity_snapshot(
            trade.market_id,
            trade.asset_id,
        )
        if ob_liquidity is None:
            raise SizeAnomalyError("Missing orderbook liquidity snapshot")

        rolling_volume = await self._volume_cache.get_rolling_volume(
            trade.market_id,
            as_of=trade.timestamp,
        )
        return await self._size_anomaly_detector.analyze(
            trade,
            liquidity=LiquidityInputs(
                rolling_24h_volume_usdc=rolling_volume,
                visible_book_depth_usdc=ob_liquidity.visible_book_depth_usdc,
            ),
        )

    async def _detect_trade_size_outlier(self, trade: TradeEvent):
        if not self._trade_size_outlier_detector:
            raise RuntimeError("Trade-size outlier detector is not initialized")
        return await self._trade_size_outlier_detector.analyze(trade)

    async def _detect_digit_distribution(self, trade: TradeEvent):
        if not self._digit_distribution_detector:
            raise RuntimeError("Digit distribution detector is not initialized")
        return await self._digit_distribution_detector.analyze(trade)

    async def _detect_trade_slicing(self, trade: TradeEvent):
        if not self._trade_slicing_detector:
            raise RuntimeError("Trade slicing detector is not initialized")
        return await self._trade_slicing_detector.analyze(trade)

    async def _score_and_alert(self, bundle: SignalBundle) -> None:
        """Score signals and send alert if threshold exceeded."""
        if not self._risk_scorer or not self._alert_formatter or not self._alert_dispatcher:
            return

        assessment = await self._risk_scorer.assess(bundle)

        # On-demand funding trace for high-risk candidates (strict: trace must succeed if triggered).
        should_trace = assessment.weighted_score >= self._settings.funding.trace_min_score
        if not should_trace and bundle.fresh_wallet_signal is not None:
            should_trace = float(bundle.trade_event.notional_value) >= self._settings.funding.trace_high_water_notional_usdc

        if should_trace and self._funding_tracer and self._redis:
            cached = await self._get_funding_signal(bundle.wallet_address)
            funding_signal = cached
            if funding_signal is None:
                funding_signal = await self._compute_funding_signal(
                    bundle.wallet_address,
                    as_of=bundle.trade_event.timestamp,
                )
                ttl_seconds = int(timedelta(days=self._settings.funding.lookback_days).total_seconds())
                await self._redis.set(
                    self._funding_wallet_key(bundle.wallet_address),
                    json.dumps(funding_signal.to_dict()),
                    ex=ttl_seconds,
                )

            bundle = dataclasses.replace(bundle, funding_signal=funding_signal)
            assessment = await self._risk_scorer.assess(bundle)

        # Persist per-trade features (durable truth, used for scan/training).
        if self._db_manager:
            async with self._db_manager.get_async_session() as session:
                repo = TradeFeatureRepository(session)
                dto = build_trade_features(bundle=bundle, assessment=assessment, computed_at=datetime.now(UTC))
                await repo.upsert(dto)
                await session.commit()

        if assessment.signals_triggered > 0:
            self._stats.signals_generated += 1

        if not assessment.should_alert:
            logger.debug(
                "Trade %s below alert threshold (score=%.2f)",
                bundle.trade_event.trade_id,
                assessment.weighted_score,
            )
            return

        # Format and dispatch alert
        formatted_alert = self._alert_formatter.format(assessment)

        if self._dry_run:
            logger.info(
                "[DRY RUN] Would send alert: wallet=%s, score=%.2f",
                assessment.wallet_address[:10] + "...",
                assessment.weighted_score,
            )
            return

        result = await self._alert_dispatcher.dispatch(formatted_alert)

        if result.all_succeeded:
            self._stats.alerts_sent += 1
            logger.info(
                "Alert sent successfully: wallet=%s, score=%.2f",
                assessment.wallet_address[:10] + "...",
                assessment.weighted_score,
            )
        else:
            logger.warning(
                "Alert partially failed: %d/%d channels succeeded",
                result.success_count,
                result.success_count + result.failure_count,
            )

    async def run(self) -> None:
        """Start the pipeline and run until interrupted.

        This is a convenience method that starts the pipeline and
        blocks until a stop signal is received.

        Example:
            ```python
            pipeline = Pipeline()
            try:
                await pipeline.run()
            except KeyboardInterrupt:
                pass
            ```
        """
        await self.start()

        try:
            if self._stop_event:
                await self._stop_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def __aenter__(self) -> Pipeline:
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.stop()
