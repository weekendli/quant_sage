"""Rebuild the tiny, serialized L2 source fixture used by Step 02 acceptance tests."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
_arrow_type = import_module("quantsage_discovery.ingest.schema")._arrow_type

DEFAULT_OUTPUT = Path(__file__).resolve().parent / "part-0.parquet"
CASES = (
    ("T2603", "2026-01-05T09:29:00.000000", "auction_matching"),
    ("TF2603", "2026-01-05T09:30:00.000000", "irregular_update"),
    ("T2603", "2026-01-05T09:30:00.000000", "irregular_update"),
    ("TF2603", "2026-01-05T09:30:00.700000", "irregular_update"),
    ("T2603", "2026-01-05T10:00:00.000000", "crossed_l1"),
    ("TF2603", "2026-01-05T10:00:00.000000", "negative_l1_size"),
    ("T2603", "2026-01-05T10:01:00.000000", "duplicate_event_key_a"),
    ("T2603", "2026-01-05T10:01:00.000000", "duplicate_event_key_b"),
    ("T2603", "2026-01-05T11:29:59.500000", "before_lunch_close"),
    ("TF2603", "2026-01-05T15:15:00.000000", "close_boundary_out_of_session"),
    ("T2603", "2026-01-05T13:00:00.000000", "after_lunch_open"),
    ("T2603", "2026-01-05T15:14:59.999000", "last_millisecond_before_close"),
    ("TF2603", "2026-01-06T09:30:04.000000", "next_trading_day"),
    ("T2603", "2026-01-06T09:30:00.000000", "next_trading_day"),
)
EXPECTED = {"input_rows": 14, "accepted_rows": 9, "quarantined_rows": 5,
            "row_group_rows": [4, 4, 4, 2]}


def _row(index: int, symbol: str, timestamp: str, case: str,
         fields: list[dict[str, Any]]) -> dict[str, Any]:
    local_time = datetime.fromisoformat(timestamp)
    result = {}
    for field in fields:
        name, dtype = field["name"], _arrow_type(field["arrow_type"])
        if pa.types.is_string(dtype):
            value = ""
        elif pa.types.is_decimal(dtype):
            value = Decimal(0).quantize(Decimal(1).scaleb(-dtype.scale))
        elif pa.types.is_integer(dtype):
            value = 0
        elif pa.types.is_date(dtype):
            value = local_time.date()
        elif pa.types.is_timestamp(dtype):
            value = local_time
        else:
            raise ValueError(f"fixture builder does not support {name}: {dtype}")
        result[name] = value
    result.update({
        "TRADINGDATE": local_time.date(),
        "TRADINGTIME": local_time,
        "SYMBOL": symbol,
        "MARKET": "CFFEX",
        "product": "TF" if symbol.startswith("TF") else "T",
        "source_file": "fixture/TFL2_TAQ_202601.csv",
        "source_row": 1001 + index,
        "ingest_run_id": "fixture-import-20261103",
        "LASTPRICE": Decimal("100.000"),
        "SETTLEPRICE": Decimal("0.000"),
        "CLOSEPRICE": Decimal("0.000"),
        "UNIX": int(local_time.replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp() * 1000),
    })
    for level in range(1, 6):
        result[f"BUYPRICE{level:02}"] = Decimal(f"{100 - (level - 1) * 0.250:.3f}")
        result[f"SELLPRICE{level:02}"] = Decimal(f"{100.250 + (level - 1) * 0.250:.3f}")
        result[f"BUYVOLUME{level:02}"] = Decimal(f"{10 + level:.2f}")
        result[f"SELLVOLUME{level:02}"] = Decimal(f"{8 + level:.2f}")
    if case == "crossed_l1":
        result["SELLPRICE01"] = Decimal("99.750")
    elif case == "negative_l1_size":
        result["BUYVOLUME01"] = Decimal("-1.00")
    elif case.startswith("duplicate_event_key_"):
        result["CONTINUESIGN"] = "DUPLICATE-PAIR"
    return result


def build_fixture(output_path: str | Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    schema_path = REPO_ROOT / "docs/references/gov_bond_l2.schema.json"
    schema_doc = json.loads(schema_path.read_text(encoding="utf-8"))
    fields = schema_doc["sample"]["fields"]
    schema = pa.schema([
        pa.field(field["name"], _arrow_type(field["arrow_type"]), nullable=field["nullable"])
        for field in fields
    ])
    table = pa.Table.from_pylist([
        _row(index, symbol, timestamp, case, fields)
        for index, (symbol, timestamp, case) in enumerate(CASES)
    ], schema=schema)
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output, row_group_size=4, compression="zstd")
    parquet = pq.ParquetFile(output)
    group_rows = [parquet.metadata.row_group(i).num_rows
                  for i in range(parquet.metadata.num_row_groups)]
    return {
        **EXPECTED,
        "path": str(output),
        "bytes": output.stat().st_size,
        "schema_matches": parquet.schema_arrow.equals(schema, check_metadata=False),
        "column_count": len(parquet.schema_arrow.names),
        "row_group_rows": group_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="fixture Parquet output path")
    args = parser.parse_args()
    result = build_fixture(args.output)
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["schema_matches"] or result["row_group_rows"] != EXPECTED["row_group_rows"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
