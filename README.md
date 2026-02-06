# TRKD Modular Runtime

This repository runs an algorithmic trading runtime for Indian markets.
It ingests live ticks, aggregates candles, computes indicators, routes
signals to strategies, and executes paper trades with risk exits.

## Architecture Overview

### Track A — Live Tick Pipeline

1. **WebSocket ingest**: `data/ticks.py` opens a Kite WebSocket and forwards
   ticks to `process_tick_to_1m`. Each tick is normalized to IST and
   converted to *differential volume* so that candle volume reflects per-tick
   changes (not cumulative totals).
2. **1-minute candles**: ticks accumulate into `candles_1m` and minute-close
   detection triggers the 5-minute aggregator.
3. **5-minute candles**: `data/candles.py` aggregates the last five 1-minute
   candles into a completed 5-minute candle for downstream processing.

### Track B — Indicator Backfill

`data/backfill.py` performs one-time session backfills for:
- **VWAP** (from 09:15 IST to now)
- **Opening Range** (09:15–09:45 IST)

Both use IST-aware datetimes to avoid Cloud Run timezone issues.

### Indicators

- `indicators/vwap.py` maintains cumulative VWAP state from completed candles.
  It expects *per-candle* volume, so Track A’s differential volume logic is
  critical.
- `indicators/opening_range.py` updates the opening range during the OR window
  and finalizes it at the 09:40–09:45 candle.

### Strategies + Routing

- Strategy logic lives in `strategies/`.
  - `strategies/orb.py`: VWAP + Opening Range Breakout
  - `strategies/vwap_crossover.py`: VWAP crossover
- `engine/strategy_router.py` routes completed 5-minute candles to enabled
  strategies and collects emitted signals.
- `engine/config_loader.py` loads strategy configuration from Google Sheets
  and caches it once per IST day, unless a forced reload occurs.

### Execution + Risk

- `execution/paper.py` enters and exits paper positions.
- `risk/exits.py` checks for VWAP recross exits, trailing SL, and a hard
  time-based exit.

## Runtime Flow (`main.py`)

1. Bootstrap Google Sheets and Kite clients.
2. Resolve current-month NIFTY and BANKNIFTY futures.
3. Backfill VWAP + Opening Range.
4. Start Kite WebSocket ingestion.
5. On every 5-minute candle close:
   - Update indicators
   - Route strategies
   - Enter paper positions
   - Evaluate exits

## Configuration

Strategy configuration is pulled from the `STRATEGY_CONFIG` Google Sheet.
The configuration is loaded once per IST day unless a reload is forced via
the `/reload-config` endpoint.

Per-direction trade limits can be set per index using:
- `max_trades_per_day_long` / `max_trades_per_day_short`
- or index-specific overrides like `max_trades_per_day_long_NIFTY`.

## Performance Tracking (Signals Only)

Signals are tracked using the ATM option LTP at the time the signal is generated.
The runtime updates marks on each 5-minute candle to keep an in-memory P&L per
signal (no actual trades are placed).

For durable performance analytics across periods (1D, 1W, 1M, 1Q, YTD, 1Y,
custom), persist the signal records to a database (e.g., Postgres or BigQuery)
and visualize via a dashboard (e.g., Metabase, Superset, or a custom web UI).

## Quick Reference: Key Files

- `main.py`: Application entrypoint (modular orchestration)
- `data/ticks.py`: WebSocket ingest + 1m candles
- `data/candles.py`: 5m aggregation
- `data/backfill.py`: VWAP + Opening Range backfills
- `indicators/`: VWAP + Opening Range
- `strategies/`: ORB + VWAP Crossover
- `engine/`: Config loader + strategy routing
- `execution/paper.py`: Paper trading
- `risk/exits.py`: Exit logic
- `performance/tracker.py`: Signal P&L tracking (in-memory)
