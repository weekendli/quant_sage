"""Reproduce the descriptive sample audit; this is not a Stage 02 adapter.

From the repository root, with pyarrow 21.0.0 and pandas 2.3.3:
    python3 docs/references/inspect_l2_sample.py
Prints JSON to stdout; does not modify the sample or repository files.
"""
import hashlib
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def audit(path):
    parquet = pq.ParquetFile(path)
    columns = [
        "TRADINGDATE", "TRADINGTIME", "SYMBOL", "MARKET", "product",
        "source_file", "source_row", "ingest_run_id", "UNIX",
    ]
    frame = parquet.read(columns=columns).to_pandas()
    event_key = ["MARKET", "SYMBOL", "TRADINGDATE", "TRADINGTIME"]
    provenance_key = ["ingest_run_id", "source_file", "source_row"]

    def duplicate_counts(key):
        sizes = frame.groupby(key, dropna=False, sort=False).size()
        repeated = sizes[sizes > 1]
        return {
            "duplicate_key_groups": int(len(repeated)),
            "excess_rows": int((repeated - 1).sum()),
            "max_multiplicity": int(sizes.max()),
        }

    by_instrument = []
    for (symbol, product), group in frame.groupby(["SYMBOL", "product"]):
        by_instrument.append({
            "instrument": symbol, "product": product, "row_count": len(group),
            "trading_day_count": group.TRADINGDATE.nunique(),
            "time_min": str(group.TRADINGTIME.min()),
            "time_max": str(group.TRADINGTIME.max()),
        })
    # Preserve physical row order; group boundaries do not imply session boundaries.
    groups = ["ingest_run_id", "source_file", "MARKET", "SYMBOL", "TRADINGDATE"]
    gaps = frame.groupby(groups, sort=False).TRADINGTIME.diff().dt.total_seconds()
    positive_gaps = gaps[gaps > 0]
    quality = {
        "unix_local_offset_mismatch_count": int((
            frame.TRADINGTIME.astype("datetime64[ms]").astype("int64")
            - frame.UNIX != 28_800_000
        ).sum()),
        "event_date_mismatch_count": int((
            frame.TRADINGTIME.dt.date != frame.TRADINGDATE
        ).sum()),
    }
    nulls = {name: 0 for name in parquet.schema_arrow.names}
    checks = {
        "crossed_l1_count": lambda b: pc.greater(b["BUYPRICE01"], b["SELLPRICE01"]),
        "locked_l1_count": lambda b: pc.equal(b["BUYPRICE01"], b["SELLPRICE01"]),
        "nonpositive_l1_count": lambda b: pc.or_(
            pc.less_equal(b["BUYPRICE01"], 0), pc.less_equal(b["SELLPRICE01"], 0)),
        "negative_l1_size_count": lambda b: pc.or_(
            pc.less(b["BUYVOLUME01"], 0), pc.less(b["SELLVOLUME01"], 0)),
        "negative_volume_count": lambda b: pc.or_(
            pc.less(b["TRADEVOLUME"], 0), pc.less(b["TOTALVOLUME"], 0)),
        "orderrate_outside_ratio_count": lambda b: pc.or_(
            pc.less(b["ORDERRATE"], -1), pc.greater(b["ORDERRATE"], 1)),
        "zero_settleprice_count": lambda b: pc.equal(b["SETTLEPRICE"], 0),
        "zero_closeprice_count": lambda b: pc.equal(b["CLOSEPRICE"], 0),
    }
    quality.update({name: 0 for name in checks})
    orderrate_min, orderrate_max = None, None
    scanned_rows = 0
    for batch in parquet.iter_batches(batch_size=131_072):
        scanned_rows += batch.num_rows
        for name in nulls:
            nulls[name] += batch.column(name).null_count
        values = {name: batch.column(name) for name in batch.schema.names}
        for name, check in checks.items():
            quality[name] += pc.sum(pc.cast(check(values), pa.int64())).as_py() or 0
        bounds = pc.min_max(values["ORDERRATE"]).as_py()
        if bounds["min"] is not None:
            orderrate_min = min(orderrate_min, bounds["min"]) if orderrate_min is not None else bounds["min"]
            orderrate_max = max(orderrate_max, bounds["max"]) if orderrate_max is not None else bounds["max"]
    quality.update(orderrate_min=str(orderrate_min), orderrate_max=str(orderrate_max))
    assert scanned_rows == parquet.metadata.num_rows == len(frame)
    result = {
        "sample_path": path.name,
        "sample_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "scope": "Full single-file descriptive audit; not Stage 02 acceptance or market-quality certification",
        "reproduce_command": "python3 docs/references/inspect_l2_sample.py",
        "tool_versions": {"pyarrow": pa.__version__, "pandas": pd.__version__},
        "row_count": scanned_rows, "column_count": len(parquet.schema_arrow),
        "row_group_count": parquet.num_row_groups,
        "coverage": {
            "trading_day_count": int(frame.TRADINGDATE.nunique()),
            "trade_date_min": str(frame.TRADINGDATE.min()),
            "trade_date_max": str(frame.TRADINGDATE.max()),
            "event_time_min": str(frame.TRADINGTIME.min()),
            "event_time_max": str(frame.TRADINGTIME.max()),
            "instruments": sorted(frame.SYMBOL.unique().tolist()),
            "products": sorted(frame["product"].unique().tolist()),
            "markets": sorted(frame.MARKET.unique().tolist()),
            "source_files": sorted(frame.source_file.unique().tolist()),
            "source_row_min": int(frame.source_row.min()),
            "source_row_max": int(frame.source_row.max()),
            "ingest_runs": sorted(frame.ingest_run_id.unique().tolist()),
        },
        "by_instrument": by_instrument, "null_counts": nulls, "quality": quality,
        "duplicate_event_keys": duplicate_counts(event_key),
        "duplicate_provenance_keys": duplicate_counts(provenance_key),
        "physical_order": {
            "group_keys": groups, "time_reorder_count": int((gaps < 0).sum()),
            "adjacent_same_time_count": int((gaps == 0).sum()),
            "positive_gap_count": int(len(positive_gaps)),
            "positive_gap_seconds_quantiles": {
                str(q): float(positive_gaps.quantile(q)) for q in [0.5, 0.9, 0.99]
            },
            "limitation": "Includes intraday breaks; no calendar/session filter; not a staleness distribution",
        },
        "limitations": [
            "No full-payload duplicate/conflict classification performed",
            "L1 checks do not certify all five levels or market-session validity",
            "No cumulative/increment reconciliation, tick-size or sentinel validation performed",
            "No per-record availability or source publishing mechanism verified",
        ],
    }
    return result


if __name__ == "__main__":
    sample = Path(__file__).resolve().with_name("part-0.parquet")
    print(json.dumps(audit(sample), ensure_ascii=False, indent=2))
