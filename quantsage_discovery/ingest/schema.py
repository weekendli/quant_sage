"""Source schema loading and explicit L2 snapshot field mapping."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa


class MissingFieldError(ValueError):
    """Required physical fields are absent from the selected source file."""

    code = "MISSING_FIELD"


L2_FIELD_MAPPING: dict[str, str] = {
    "TRADINGDATE": "trade_date",
    "TRADINGTIME": "event_time",
    "SYMBOL": "instrument_id",
    "MARKET": "exchange",
    "LASTPRICE": "last_price",
    "TOTALPOSITION": "open_interest",
    "TOTALVOLUME": "volume",
    "TOTALAMOUNT": "turnover",
    "TRADEVOLUME": "volume_increment",
    "TRADEAMOUNT": "turnover_increment",
    "UNIX": "source_event_unix_ms",
}
for _level in range(1, 6):
    _suffix = f"{_level:02d}"
    L2_FIELD_MAPPING[f"BUYPRICE{_suffix}"] = f"bid_price_{_level}"
    L2_FIELD_MAPPING[f"SELLPRICE{_suffix}"] = f"ask_price_{_level}"
    L2_FIELD_MAPPING[f"BUYVOLUME{_suffix}"] = f"bid_size_{_level}"
    L2_FIELD_MAPPING[f"SELLVOLUME{_suffix}"] = f"ask_size_{_level}"


@dataclass(frozen=True)
class SourceField:
    name: str
    arrow_type: pa.DataType
    nullable: bool
    description: str
    source_field: str | None


@dataclass(frozen=True)
class SourceSchema:
    version: str
    fields: tuple[SourceField, ...]
    sha256: str
    path: Path

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.fields)

    def field(self, name: str) -> SourceField:
        for field in self.fields:
            if field.name == name:
                return field
        raise KeyError(name)

    def standardized_name(self, source_name: str) -> str:
        if source_name not in self.names:
            raise KeyError(source_name)
        return L2_FIELD_MAPPING.get(source_name, source_name)

    def standardized_names(self) -> tuple[str, ...]:
        names = tuple(self.standardized_name(name) for name in self.names)
        if len(names) != len(set(names)):
            raise ValueError("explicit L2 field mapping creates a column-name collision")
        return names


def _arrow_type(spec: str) -> pa.DataType:
    match = re.fullmatch(r"decimal128\((\d+),\s*(\d+)\)", spec)
    if match:
        return pa.decimal128(int(match.group(1)), int(match.group(2)))
    if spec == "date32[day]":
        return pa.date32()
    if spec.startswith("timestamp[") and spec.endswith("]"):
        unit = spec[10:-1].split(",", maxsplit=1)[0].strip()
        return pa.timestamp(unit)
    aliases: dict[str, pa.DataType] = {
        "string": pa.string(),
        "int64": pa.int64(),
        "int32": pa.int32(),
        "float64": pa.float64(),
        "bool": pa.bool_(),
    }
    try:
        return aliases[spec]
    except KeyError as exc:
        raise ValueError(f"unsupported source Arrow type in schema JSON: {spec!r}") from exc


def load_source_schema(path: str | Path) -> SourceSchema:
    schema_path = Path(path).expanduser().resolve()
    raw_bytes = schema_path.read_bytes()
    document = json.loads(raw_bytes)
    fields = document.get("sample", {}).get("fields")
    if not isinstance(fields, list) or not fields:
        raise ValueError("source schema JSON must include a non-empty sample.fields list")
    parsed = tuple(
        SourceField(
            name=str(item["name"]),
            arrow_type=_arrow_type(str(item["arrow_type"])),
            nullable=bool(item["nullable"]),
            description=str(item.get("description", "")),
            source_field=(str(item["source_field"]) if item.get("source_field") is not None else None),
        )
        for item in fields
    )
    names = tuple(field.name for field in parsed)
    if len(names) != len(set(names)):
        raise ValueError("source schema JSON contains duplicate field names")
    return SourceSchema(
        version=str(document.get("schema_version", "")),
        fields=parsed,
        sha256="sha256:" + hashlib.sha256(raw_bytes).hexdigest(),
        path=schema_path,
    )


def validate_physical_schema(physical: pa.Schema, source: SourceSchema) -> None:
    """Require the pinned physical column set, order, and Arrow types."""
    expected_names = source.names
    actual_names = tuple(physical.names)
    if actual_names != expected_names:
        missing = [name for name in expected_names if name not in actual_names]
        extra = [name for name in actual_names if name not in expected_names]
        if missing or extra:
            error_type = MissingFieldError if missing else ValueError
            raise error_type(
                "physical source schema columns differ from the pinned L2 schema; "
                f"missing={missing}, extra={extra}"
            )
        raise ValueError("physical source columns are not in the pinned L2 schema order")
    mismatches = [
        f"{field.name}: expected {field.arrow_type}, got {physical.field(field.name).type}"
        for field in source.fields
        if physical.field(field.name).type != field.arrow_type
    ]
    if mismatches:
        raise ValueError("physical source schema type mismatch: " + "; ".join(mismatches))
