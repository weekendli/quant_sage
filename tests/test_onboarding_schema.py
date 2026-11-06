from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quantsage_discovery.ingest.schema import (
    L2_FIELD_MAPPING,
    MissingFieldError,
    load_source_schema,
    validate_physical_schema,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "docs/references/gov_bond_l2.schema.json"
SAMPLE_PATH = Path(
    "/data/marketdata-backup/futures/treasury_futures/parquet/l2_snapshot/v1/"
    "year=2026/month=01/part-0.parquet"
)


def test_mapping_uses_explicit_names_and_preserves_increment_semantics() -> None:
    assert L2_FIELD_MAPPING["TRADINGTIME"] == "event_time"
    assert L2_FIELD_MAPPING["SYMBOL"] == "instrument_id"
    assert L2_FIELD_MAPPING["BUYPRICE05"] == "bid_price_5"
    assert L2_FIELD_MAPPING["SELLVOLUME03"] == "ask_size_3"
    assert L2_FIELD_MAPPING["TRADEVOLUME"] == "volume_increment"
    assert L2_FIELD_MAPPING["TOTALVOLUME"] == "volume"
    assert L2_FIELD_MAPPING["TRADEAMOUNT"] == "turnover_increment"
    assert L2_FIELD_MAPPING["TOTALAMOUNT"] == "turnover"


def test_source_schema_reuses_json_decimal_types_and_metadata_fields() -> None:
    schema = load_source_schema(SCHEMA_PATH)

    assert schema.version == "2.0"
    assert len(schema.fields) == 66
    assert schema.field("BUYPRICE01").arrow_type == pa.decimal128(9, 3)
    assert schema.field("BUYVOLUME01").arrow_type == pa.decimal128(19, 2)
    assert schema.field("TRADINGTIME").arrow_type == pa.timestamp("us")
    assert schema.field("source_row").arrow_type == pa.int64()
    assert schema.field("source_row").nullable is True


def test_real_january_parquet_matches_the_pinned_physical_schema() -> None:
    if not SAMPLE_PATH.is_file():
        pytest.skip("Brighton marketdata sample is unavailable")
    schema = load_source_schema(SCHEMA_PATH)
    physical = pq.ParquetFile(SAMPLE_PATH).schema_arrow

    validate_physical_schema(physical, schema)


def test_missing_required_time_or_book_fields_is_a_schema_error() -> None:
    schema = load_source_schema(SCHEMA_PATH)
    physical = pa.schema(
        [
            pa.field("TRADINGDATE", pa.date32()),
            pa.field("SYMBOL", pa.string()),
            pa.field("BUYPRICE01", pa.decimal128(9, 3)),
        ]
    )

    with pytest.raises(MissingFieldError, match="TRADINGTIME") as error:
        validate_physical_schema(physical, schema)
    assert error.value.code == "MISSING_FIELD"
