# Polymarket Research Lab

Single-process Java 21 research application. Public market-data collection, SQLite persistence, five exploratory strategy modules, parameter sweeps, and a small dashboard. It is paper/research only; there is no order-placement implementation.

## Run

```bash
docker compose up -d --build
docker compose logs -f polymarket-lab
```

Open `http://YOUR_SERVER:8080`.

## Update after a git push

```bash
git pull
docker compose up -d --build
```

## Stop

```bash
docker compose down
```

The named Docker volume `polymarket_data` persists SQLite data. To deliberately delete it (irreversible):

```bash
docker compose down -v
```

## Environment variables

- `PORT` (8080)
- `HISTORY_DAYS` (30)
- `MARKET_LIMIT` (250)
- `MAX_HISTORY_TOKENS_PER_CYCLE` (60)
- `WORKERS` (4)
- `BOOK_POLL_SECONDS` (45)
- `MARKET_REFRESH_MINUTES` (15)
- `HISTORY_REFRESH_HOURS` (6)

## API

- `/health`
- `/api/state`
- `/api/strategies`
- `/api/discoveries`
- `/api/signals`
- `/api/experiments`
- `/api/logs`

## Important research limitations

- Historical price history is not historical order-book depth.
- The first version uses current book snapshots for live observations, not reconstructed historical fills.
- Related market titles do not prove logical implication; nested-market candidates require resolution-rule review.
- The first version's optimizer evaluates price-series reversal scores, not executable net P&L.
- Resolution countdown is metadata-based research, not an oracle for event outcomes.
- Public API availability, rate limits, schemas, and endpoint behavior can change. Review Polymarket's official docs before increasing collection frequency.
