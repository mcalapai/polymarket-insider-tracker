# Polymarket Insider Tracker

**Detect informed money before the market moves.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

---

## The Opportunity

On January 3, 2026, a trader spotted a significant political event on Polymarket **before it happened**. How? Not by predicting the future, but by tracking suspicious trading behavior.

> "You don't need to predict the future, you need to track suspicious behavior."
> — [@DidiTrading](https://x.com/DidiTrading)

An insider wallet turned **$35,000 into $442,000** (12.6x return) by entering a position hours before a major market move. The tool that detected this activity flagged five separate alerts before the event occurred.

**This repository builds that tool.**

---

## What This Does

The Polymarket Insider Tracker monitors prediction market trading activity in real-time and identifies patterns that suggest informed trading:

| Signal | What It Detects | Why It Matters |
|--------|-----------------|----------------|
| **Fresh Wallets** | Brand new wallets making large trades | Insiders create new wallets to hide their identity |
| **Unusual Sizing** | Trades that are disproportionately large for the market | Informed traders bet bigger when they have edge |
| **Funding Chains** | Where wallet funds originated from | Links seemingly separate wallets to the same entity |
| **Sniper Clusters** | Coordinated early entrants across markets | Coordinated wallets often share an insider source |
| **Order Behavior** | Rapid cancels and book-moving orders with no fills | Can indicate manipulative intent (spoofing-style) |

When suspicious activity is detected, you receive an instant alert with actionable intelligence.

---

## How It Works

```
┌─────────────────┐     ┌──────────────────┐     ┌────────────────────┐
│  Polymarket API │────>│  Wallet Profiler │────>│  Anomaly Detector  │
│  (Real-time)    │     │  (Blockchain)    │     │  (ML + Heuristics) │
└─────────────────┘     └──────────────────┘     └────────────────────┘
                                                          │
                              ┌────────────────────────────┘
                              v
                   ┌─────────────────────┐
                   │   Alert Dispatcher  │───> Discord / Telegram / Email
                   │   "Fresh wallet     │
                   │    buying YES @7.5¢ │
                   │    on niche market" │
                   └─────────────────────┘
```

### Detection Algorithms

1. **Fresh Wallet Detection**
   - Computes wallet state *as-of trade time* (historical block resolution)
   - Defines wallet age via earliest inbound USDC funding transfer (auditable)
   - Produces a deterministic confidence score from snapshot features

2. **Liquidity Impact Analysis**
   - Measures trade size relative to rolling 24h notional volume and visible orderbook depth
   - No category heuristics and no fabricated metadata fallbacks (strict inputs)

3. **Sniper Cluster Detection**
   - Uses DBSCAN clustering to find wallets that consistently enter markets within minutes of creation
   - Identifies coordinated behavior patterns

4. **Event Correlation**
   - Measures subsequent market movement after the trade over fixed horizons (15m/1h/4h)
   - Normalizes by recent volatility (no external news dependency)

5. **Funding-Chain Tracing**
   - Bounded, on-demand USDC transfer indexing and hop-limited tracing
   - Persists transfers + derived wallet graph relationships

6. **Spoofing-Style Order Signals** (optional)
   - Ingests CLOB market-channel `price_change`/`book` events and attributes orders via Level-2 API
   - Flags rapid cancels and book-moving orders canceled quickly with near-zero fill

---

## Sample Alert

```
SUSPICIOUS ACTIVITY DETECTED

Wallet: 0x7a3...f91 (Age: 2 hours, 3 transactions)
Market: "Will X announce Y by March 2026?"
Action: BUY YES @ $0.075
Size: $15,000 USDC (8.2% of daily volume)

Risk Signals:
  [x] Fresh Wallet (trade-time nonce + first USDC funding)
  [x] Large Position (volume + book depth impact)
  [x] Funding Origin (bounded USDC trace)

Funding Trail:
  --> 0xdef...789 (2-year-old wallet, 500+ txns)
      --> Binance Hot Wallet

Confidence: HIGH (3/4 signals triggered)
```

---

## Quick Start

### Prerequisites

- Python 3.11+
- Docker and Docker Compose
- Polygon RPC endpoint (Alchemy, QuickNode, or self-hosted)
- Polymarket CLOB access (public market data endpoints)
- For `scan`: a local embedding model + Postgres `pgvector` extension
- For order lifecycle ingestion: CLOB Level-2 credentials (optional)

### Installation

```bash
# Clone the repository
git clone https://github.com/pselamy/polymarket-insider-tracker.git
cd polymarket-insider-tracker

# Copy environment template
cp .env.example .env
# Edit .env with your API keys

# Start infrastructure (PostgreSQL, Redis)
docker compose up -d

# Wait for services to be healthy
docker compose ps

# Install Python dependencies
pip install -e .

# Run database migrations
alembic upgrade head

# Run the tracker
python -m polymarket_insider_tracker run

# Historical scan (natural-language query only)
python -m polymarket_insider_tracker scan --query "US election swing states"
```

### Docker Services

The development stack includes:

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL 15 | 5432 | Primary database |
| Redis 7 | 6379 | Caching and pub/sub |
| Adminer | 8080 | Database admin UI (optional) |
| RedisInsight | 5540 | Redis admin UI (optional) |

```bash
# Start core services only
docker compose up -d

# Start with development tools (Adminer, RedisInsight)
docker compose --profile tools up -d

# View logs
docker compose logs -f

# Stop all services
docker compose down

# Stop and remove volumes (reset data)
# Note: required if you change POSTGRES_* credentials after first boot
docker compose down -v
```

### Configuration

```bash
# .env file
POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
POLYMARKET_TRADE_WS_URL=wss://ws-live-data.polymarket.com
POLYMARKET_CLOB_MARKET_WS_URL=wss://ws-subscriptions-clob.polymarket.com/ws/market
POLYMARKET_CLOB_HOST=https://clob.polymarket.com

# Database (async SQLAlchemy)
DATABASE_URL=postgresql+asyncpg://tracker:dev_password@localhost:5432/polymarket_tracker

# Alert destinations (optional)
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Historical scan/backtest (semantic retrieval)
SCAN_EMBEDDING_MODEL=/path/to/local/embedding/model
SCAN_EMBEDDING_DIM=384
```

---

## Project Structure

```
polymarket-insider-tracker/
├── src/
│   ├── ingestor/           # Real-time market data ingestion
│   │   ├── clob_client.py  # Polymarket CLOB API wrapper
│   │   └── websocket.py    # WebSocket event handler
│   ├── profiler/           # Wallet analysis
│   │   ├── analyzer.py     # Core wallet profiling logic
│   │   ├── chain.py        # Polygon blockchain client
│   │   └── funding.py      # Funding chain tracer
│   ├── detector/           # Anomaly detection engines
│   │   ├── fresh_wallet.py
│   │   ├── size_anomaly.py
│   │   ├── sniper.py       # DBSCAN clustering
│   │   └── scorer.py       # Composite risk scoring
│   ├── alerter/            # Notification dispatch
│   │   ├── formatter.py    # Alert message formatting
│   │   └── dispatcher.py   # Multi-channel delivery
│   └── storage/            # Persistence layer
│       ├── models.py       # SQLAlchemy models
│       └── repos.py        # Repository pattern
├── tests/                  # Test suite
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

## Roadmap

### Phase 1: Core Detection (Current)
- [x] Project structure and documentation
- [x] Polymarket CLOB API integration
- [x] Fresh wallet detection (trade-time snapshots)
- [x] Size anomaly detection (volume + depth)
- [x] Basic alerting (Discord/Telegram)

### Phase 2: Advanced Intelligence
- [x] Funding chain analysis (bounded USDC tracing)
- [x] Sniper cluster detection (DBSCAN) + co-entry correlation
- [x] Historical scan/backtest (`scan --query "..."`)

### Phase 3: Production Hardening
- [ ] High-availability deployment
- [ ] Rate limit management
- [ ] False positive feedback loop
- [ ] Web dashboard

---

## Why This Matters

Prediction markets are becoming a critical source of real-time probability estimates for world events. As they grow, so does the incentive for informed actors to exploit information asymmetry.

This tool democratizes access to the same detection capabilities that sophisticated traders use. Whether you are:

- **A trader** looking for alpha signals
- **A researcher** studying market microstructure
- **A platform operator** monitoring for manipulation

...this tracker provides visibility into the hidden flows that move markets.

---

## Technical Background

### Polymarket Architecture

Polymarket is a prediction market platform built on Polygon (Ethereum L2). Key characteristics:

- **CLOB (Central Limit Order Book)**: Centralized matching engine for speed
- **On-chain Settlement**: Final trades settle on Polygon blockchain
- **USDC Collateral**: All positions denominated in USDC stablecoin
- **Binary Outcomes**: Shares priced between $0.00 and $1.00

### Data Sources

| Source | Purpose | Latency |
|--------|---------|---------|
| Polymarket CLOB API | Real-time trades, orderbook | Milliseconds |
| Polygon RPC | Wallet history, nonce, funding | 1-2 seconds |
| Market Metadata API | Market categorization | On-demand |

### Detection Challenges

1. **Sybil Resistance**: Insiders use fresh wallets per trade
2. **Rate Limits**: Polygon RPC calls require caching strategy
3. **Market Classification**: NLP needed to categorize market niches
4. **Timing**: CLOB data leads on-chain by seconds

---

## Contributing

Contributions are welcome! Please read our Contributing Guide before submitting PRs.

### Development Setup

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check src/

# Run type checking
mypy src/
```

---

## Disclaimer

This software is provided for **educational and research purposes only**.

- Trading prediction markets involves significant financial risk
- This tool does not constitute financial advice
- Insider trading is illegal in regulated markets; this tool is for transparency and research
- Users are responsible for compliance with applicable laws

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---

## Acknowledgments

- Inspired by [@DidiTrading](https://x.com/DidiTrading) and [@spacexbt](https://x.com/spacexbt)
- Built on the open Polymarket API ecosystem
- Community contributions welcome

---

**Questions?** Open an issue or start a discussion.
