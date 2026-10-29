# Trade-count collector

`trade_count.py` implements `trade_count_1m` from `observability/metric_catalog.yaml`.
It reads Trung's raw `.jsonl` and `.jsonl.gz` files, validates each trade with
`platform/contracts/validate.py`, keeps the earliest copy of each `event_id`,
and excludes Coinbase snapshots. It groups the remaining trades by UTC minute,
source, and canonical symbol.

Preview observations without a database:

```bash
python observability/collectors/trade_count.py --in tests/fixtures/raw
python observability/collectors/trade_count.py --in data/raw \
  --window-end 2026-10-27T02:49:00Z
```

To write to PostgreSQL, first apply `observability/store/001_*.sql` and
`002_*.sql`, run `python -m pip install -r requirements-observability.txt`,
set `FINOBS_METRIC_DSN` to a PostgreSQL connection string, and add `--write-db`.
The collector calls
`upsert_metric_observation`, so rerunning the same minute updates its row.

This first collector emits only groups with at least one live trade. It cannot
infer a healthy zero-trade minute or a source outage from raw files alone.
The source contract notes that Coinbase may replay old trades without a
snapshot flag; a replay whose original event is outside the scanned input
cannot be identified by this collector.
Each run scans the complete input and keeps seen trade IDs in memory. This is
useful for Sprint 1 fixtures and short backfills; a continuous collector will
need incremental file offsets and a persistent deduplication checkpoint.
