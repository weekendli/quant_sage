# Data locations and access

Verified on Brighton on 2026-11-02 using Python and DuckDB 1.5.5. Data is stored in Apache Parquet files and can be queried directly from Python with DuckDB. The files remain outside this repository.

## Treasury futures L2 order book

Root directory:

```text
/data/marketdata-backup/futures/treasury_futures/parquet/l2_snapshot/v1
```

Layout: `year=YYYY/month=MM/part-0.parquet`, with `_manifest.json` and `_quality.json` beside each monthly file. The verified inventory contains 79 Parquet files, from January 2020 through July 2026, totaling 5,903,156,058 bytes (about 5.50 GiB). Parquet footer metadata reports 257,407,377 rows; this was not a full row-level quality audit.

The January 2026 file was queried successfully. It has 66 physical columns: 62 business fields and four ingestion metadata fields. It contains five-level bid/ask snapshots for CFFEX government bond futures. Important columns include:

| Columns | Meaning / DuckDB type |
| --- | --- |
| `TRADINGDATE`, `TRADINGTIME` | Trading date (`DATE`) and event timestamp (`TIMESTAMP`, without timezone) |
| `SYMBOL`, `MARKET`, `product` | Contract, exchange, product (`VARCHAR`) |
| `BUYPRICE01`–`05`, `SELLPRICE01`–`05` | Five bid/ask prices (`DECIMAL(9,3)`) |
| `BUYVOLUME01`–`05`, `SELLVOLUME01`–`05` | Five bid/ask sizes (`DECIMAL(19,2)`) |
| `UNIX` | Event epoch milliseconds (`BIGINT`) |
| `source_file`, `source_row`, `ingest_run_id` | Source lineage; these do not establish reception time |

See the [full field reference](references/gov_bond_l2.schema.md) and [machine-readable schema](references/gov_bond_l2.schema.json) for the remaining fields and interpretation constraints. Snapshots do not provide a complete order or trade log. `available_time` is absent; timestamp timezone and availability must be handled explicitly under the [shared contracts](COMMON_CONTRACTS.md).

### Query with Python and DuckDB

Use a Python environment with `duckdb` installed. The verification environment had DuckDB 1.5.5; pandas and PyArrow are unnecessary for `fetchall()`.

```python
import duckdb

root = "/data/marketdata-backup/futures/treasury_futures/parquet/l2_snapshot/v1"
con = duckdb.connect(":memory:")
con.execute("SET memory_limit = '1GB'")
con.execute("SET threads = 2")

# Start with one month and selected columns, rather than loading all data.
rows = con.execute("""
    SELECT TRADINGDATE, TRADINGTIME, SYMBOL, MARKET, product,
           BUYPRICE01, SELLPRICE01
    FROM read_parquet(?, hive_partitioning=false)
    LIMIT 3
""", [f"{root}/year=2026/month=01/part-0.parquet"]).fetchall()
print(rows)
con.close()
```

The first returned snapshot was `T2603`, `CFFEX`, at `2026-01-05 09:29:00.400000`, with bid `107.920` and ask `107.925`. `LIMIT` without `ORDER BY` is a preview, not a chronological ordering guarantee. Python returns prices as `decimal.Decimal`.

For a broader scan, pass `f"{root}/year=*/month=*/*.parquet"` to `read_parquet`. Restrict the file glob to the months needed and filter by `SYMBOL` and `TRADINGDATE`; explicitly sort by event time when required. `hive_partitioning=false` reads only physical columns. Enable it if you want DuckDB to add the directory-derived `year` and `month` columns.

## Trading calendar

Calendar files are under:

```text
/data/marketdata/canonical/reference/calendars/v3/layout=l1/
```

The active catalog uses **`part-3024-001.parquet`**. The directory also contains `part-2024-001.parquet`; both contain the same 4,055 `(calendar_id, trade_date)` keys. Reading `*.parquet` returns 8,110 rows and duplicates every calendar date. Prefer the catalog view or its selected file.

Verified active data:

- Relation: `reference.calendars` (API: `marketdata.reference.calendars`).
- Calendar ID: **`CN_STOCK` only**, with 4,055 dates from 2010-01-04 to 2026-09-11.
- Physical fields: `calendar_id VARCHAR`, `trade_date DATE`, `market VARCHAR`, `index DOUBLE`, `event_id VARCHAR`, `business_version_id VARCHAR`.
- `reference.trading_sessions` and `reference.settlement_calendars` are empty placeholder views containing only an `_empty` column.

A subsequent [CFFEX source verification](references/cffex_calendar_evidence.md) compared the calendar against issuer-authored annual closure notices, including the 2020 COVID amendment. Date membership matches exactly for **2020-01-02 through 2026-07-31** (1,594 trading dates). This establishes bounded applicability for the current onboarding scope; the provider ID remains `CN_STOCK` and the result does not authorize extrapolation. All 1,587 dates observed in the 79 raw files are covered; seven expected 2021 trading dates have no source rows and must be reported as source gaps.

The [machine-readable evidence](../configs/reference/cffex_calendar_evidence.json) also records separately verified historical session rules, including the 2020-07-20 schedule change, auction phases and morning-only last trading days. Freeze those explicit rules for onboarding; the existing session view still provides no rules.

### Resolve the active DuckDB catalog

Read `/data/marketdata/catalog/duckdb/current.json` rather than choosing a release directory by name. At verification it selected `db-restructure-full-v16`.

```python
import json
from pathlib import Path
import duckdb

root = Path("/data/marketdata")
pointer = json.loads((root / "catalog/duckdb/current.json").read_text())
with duckdb.connect(str(root / pointer["database"]), read_only=True) as con:
    rows = con.execute("""
        SELECT calendar_id, count(*) AS dates,
               min(trade_date) AS first_date, max(trade_date) AS last_date
        FROM reference.calendars
        GROUP BY calendar_id ORDER BY calendar_id
    """).fetchall()
    print(rows)
```

For direct file access, use an in-memory connection and `read_parquet('/data/marketdata/canonical/reference/calendars/v3/layout=l1/part-3024-001.parquet', hive_partitioning=false)`. This file was queried successfully, but future catalog releases may select different files. The DuckDB database contains views referencing external Parquet files, so copying the database alone is insufficient.

### Calendar access over the LAN API

The existing service at `https://192.168.1.102/marketdata-api/` exposes read-only DuckDB queries. Both `/health` and the following calendar query returned HTTP 200 during verification, reporting release `db-restructure-full-v16`, 4,055 dates, and `truncated=false`.

```bash
curl --cacert /home/jianl/ops/marketdata-api/server-ca.crt \
  https://192.168.1.102/marketdata-api/query \
  -H 'Content-Type: application/json' \
  --data '{"sql":"SELECT calendar_id, count(*) AS n, min(trade_date) AS first_date, max(trade_date) AS last_date FROM marketdata.reference.calendars GROUP BY calendar_id ORDER BY calendar_id","max_rows":100}'
```

The certificate path above is local to Brighton; copy the public certificate through a trusted channel and use its client-local path on another machine. Access is scoped to the LAN and loopback. JSON `rows` are positional arrays matching `columns`; check `truncated` and retain the returned `release` when freezing a calendar snapshot. Server-local usage documentation and an optional Python client are at `/home/jianl/ops/marketdata-api/README.md` and `/home/jianl/ops/marketdata-api/client.py`.

For reproducible onboarding, record the input files and hashes, catalog release, calendar selection, and verified session rules as described in the [onboarding delivery checklist](steps/02_data_onboarding/DELIVERY.md).
