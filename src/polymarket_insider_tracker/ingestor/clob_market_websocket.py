"""CLOB market-channel WebSocket client (book / price_change).

This stream is separate from the legacy activity/trades feed and is used for
order lifecycle ingestion (spoofing detection) and optionally book snapshots.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection

from polymarket_insider_tracker.ingestor.models import ClobBookEvent, ClobPriceChangeEvent

logger = logging.getLogger(__name__)

DEFAULT_PING_INTERVAL = 30  # seconds
DEFAULT_MAX_RECONNECT_DELAY = 30  # seconds
DEFAULT_INITIAL_RECONNECT_DELAY = 1  # seconds


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"


@dataclass
class StreamStats:
    books_received: int = 0
    price_changes_received: int = 0
    reconnect_count: int = 0
    last_message_time: float | None = None
    connected_since: float | None = None
    last_error: str | None = None


class MarketStreamError(Exception):
    """Base exception for market stream errors."""


class MarketConnectionError(MarketStreamError):
    """Raised when connection to WebSocket fails."""


BookCallback = Callable[[ClobBookEvent], Awaitable[None]]
PriceChangeCallback = Callable[[ClobPriceChangeEvent], Awaitable[None]]
StateCallback = Callable[[ConnectionState], Awaitable[None]]


class ClobMarketStreamHandler:
    """WebSocket client for the CLOB market channel."""

    def __init__(
        self,
        *,
        host: str,
        on_book: BookCallback | None = None,
        on_price_change: PriceChangeCallback | None = None,
        on_state_change: StateCallback | None = None,
        ping_interval: int = DEFAULT_PING_INTERVAL,
        max_reconnect_delay: int = DEFAULT_MAX_RECONNECT_DELAY,
        initial_reconnect_delay: int = DEFAULT_INITIAL_RECONNECT_DELAY,
    ) -> None:
        self._host = host
        self._on_book = on_book
        self._on_price_change = on_price_change
        self._on_state_change = on_state_change
        self._ping_interval = ping_interval
        self._max_reconnect_delay = max_reconnect_delay
        self._initial_reconnect_delay = initial_reconnect_delay

        self._state = ConnectionState.DISCONNECTED
        self._stats = StreamStats()

        self._ws: ClientConnection | None = None
        self._running = False
        self._stop_event: asyncio.Event | None = None

        self._assets_lock = asyncio.Lock()
        self._subscribed_assets: set[str] = set()
        self._pending_subscribe: set[str] = set()
        self._pending_unsubscribe: set[str] = set()

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def stats(self) -> StreamStats:
        return self._stats

    async def _set_state(self, new_state: ConnectionState) -> None:
        if self._state != new_state:
            old = self._state
            self._state = new_state
            logger.info("CLOB market stream state: %s -> %s", old.value, new_state.value)
            if self._on_state_change:
                try:
                    await self._on_state_change(new_state)
                except Exception as e:  # pragma: no cover
                    logger.error("Error in state change callback: %s", e)

    async def request_subscribe(self, asset_ids: set[str]) -> None:
        if not asset_ids:
            return
        async with self._assets_lock:
            self._pending_subscribe |= {str(a) for a in asset_ids if a}

    async def request_unsubscribe(self, asset_ids: set[str]) -> None:
        if not asset_ids:
            return
        async with self._assets_lock:
            self._pending_unsubscribe |= {str(a) for a in asset_ids if a}

    async def _send_subscription_messages(self, ws: ClientConnection) -> None:
        async with self._assets_lock:
            subscribe = set(self._pending_subscribe)
            unsubscribe = set(self._pending_unsubscribe)
            self._pending_subscribe.clear()
            self._pending_unsubscribe.clear()

        if unsubscribe:
            msg = {"assets_ids": sorted(unsubscribe), "operation": "unsubscribe"}
            await ws.send(json.dumps(msg))
            async with self._assets_lock:
                self._subscribed_assets -= unsubscribe

        if subscribe:
            msg = {"assets_ids": sorted(subscribe), "operation": "subscribe"}
            await ws.send(json.dumps(msg))
            async with self._assets_lock:
                self._subscribed_assets |= subscribe

    async def _connect(self) -> ClientConnection:
        await self._set_state(ConnectionState.CONNECTING)
        try:
            ws = await websockets.connect(
                self._host,
                ping_interval=self._ping_interval,
                ping_timeout=self._ping_interval * 2,
            )
        except Exception as e:
            self._stats.last_error = str(e)
            raise MarketConnectionError(f"Failed to connect to {self._host}: {e}") from e

        # Initial subscription message (can be empty by deferring to subscribe ops).
        async with self._assets_lock:
            assets = sorted(self._subscribed_assets)
        if assets:
            await ws.send(json.dumps({"type": "market", "assets_ids": assets}))

        await self._set_state(ConnectionState.CONNECTED)
        self._stats.connected_since = time.time()
        logger.info("Connected to CLOB market channel: %s", self._host)
        return ws

    async def _handle_message(self, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:  # pragma: no cover
            logger.warning("Invalid JSON message on market channel")
            return

        event_type = data.get("event_type")
        if event_type == "book":
            try:
                book = ClobBookEvent.from_websocket_message(data)
            except Exception as e:  # pragma: no cover
                logger.warning("Failed to parse book event: %s", e)
                return
            self._stats.books_received += 1
            self._stats.last_message_time = time.time()
            if self._on_book:
                await self._on_book(book)
            return

        if event_type == "price_change":
            try:
                pc = ClobPriceChangeEvent.from_websocket_message(data)
            except Exception as e:  # pragma: no cover
                logger.warning("Failed to parse price_change event: %s", e)
                return
            self._stats.price_changes_received += 1
            self._stats.last_message_time = time.time()
            if self._on_price_change:
                await self._on_price_change(pc)
            return

        logger.debug("Ignoring market-channel event_type=%r", event_type)

    async def _listen(self, ws: ClientConnection) -> None:
        try:
            while self._running:
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except TimeoutError:
                    message = None

                if isinstance(message, str):
                    await self._handle_message(message)
                elif message is not None:
                    logger.debug("Ignoring non-text market-channel message")

                await self._send_subscription_messages(ws)
        except websockets.ConnectionClosed as e:
            logger.warning("CLOB market stream connection closed: %s", e)
            raise

    async def start(self) -> None:
        if self._running:
            raise RuntimeError("Market stream already running")
        self._running = True
        self._stop_event = asyncio.Event()

        delay = self._initial_reconnect_delay
        while self._running and self._stop_event and not self._stop_event.is_set():
            try:
                self._ws = await self._connect()
                delay = self._initial_reconnect_delay
                await self._listen(self._ws)
            except Exception as e:
                self._stats.reconnect_count += 1
                self._stats.last_error = str(e)
                await self._set_state(ConnectionState.RECONNECTING)
                await asyncio.sleep(delay)
                delay = min(self._max_reconnect_delay, delay * 2)
            finally:
                with contextlib.suppress(Exception):
                    if self._ws:
                        await self._ws.close()
                self._ws = None

        await self._set_state(ConnectionState.DISCONNECTED)

    async def stop(self) -> None:
        self._running = False
        if self._stop_event:
            self._stop_event.set()
        if self._ws:
            with contextlib.suppress(Exception):
                await self._ws.close()
