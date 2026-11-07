"""Strict schemas and canonicalization helpers for FeatureSet freezing."""

from __future__ import annotations

import math
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from quantsage_discovery.core.utils import canonical_json

from .errors import ResearchValidationError

_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")
_RESOURCE_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_FEATURE_SET_REF_RE = re.compile(r"^([A-Za-z][A-Za-z0-9_.-]*)@([1-9][0-9]*)$")
_DURATION_RE = re.compile(r"^([+]?(?:\d+(?:\.\d*)?|\.\d+))(ns|us|ms|s|m|h|d)$")
_ISO_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+(?:\.\d+)?)D)?T?"
    r"(?:(?P<hours>\d+(?:\.\d+)?)H)?"
    r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?$"
)
_NS_FACTORS = {
    "ns": Decimal(1), "us": Decimal(1_000), "ms": Decimal(1_000_000),
    "s": Decimal(1_000_000_000), "m": Decimal(60_000_000_000),
    "h": Decimal(3_600_000_000_000), "d": Decimal(86_400_000_000_000),
}
_TRUSTED_PROVIDER_KEYS = {
    "builtin.arithmetic",
    "builtin.business_windows",
    "builtin.identity",
    "builtin.l2_primitives",
}
_PARAM_TYPES = {"string", "integer", "number", "boolean", "object", "array", "decimal", "duration"}


def _error(path: str, message: str) -> ResearchValidationError:
    return ResearchValidationError(f"{path}: {message}", details={"path": path})


def _obj(value: Any, required: set[str], optional: set[str], path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _error(path, "must be an object")
    if any(not isinstance(key, str) for key in value):
        raise _error(path, "object keys must be strings")
    missing, unknown = required - value.keys(), value.keys() - required - optional
    if missing:
        raise _error(path, f"missing fields: {', '.join(sorted(missing))}")
    if unknown:
        raise _error(path, f"unknown fields: {', '.join(sorted(unknown))}")
    return value


def _text(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise _error(path, "must be a non-empty string without surrounding whitespace")
    return value


def _positive_int(value: Any, path: str) -> int:
    if type(value) is not int or value < 1:
        raise _error(path, "must be an integer >= 1")
    return value


def parse_resource_ref(ref: Any, path: str) -> tuple[str, str]:
    text = _text(ref, path)
    if text.count("@") != 1:
        raise _error(path, "must be an exact version ref such as resource@1")
    resource_id, version = text.split("@", 1)
    if not _ID_RE.fullmatch(resource_id) or not _RESOURCE_VERSION_RE.fullmatch(version):
        raise _error(path, "must contain a valid id and exact version")
    if version.lower() in {"latest", "current", "head", "main"}:
        raise _error(path, "must pin an immutable resource version")
    return resource_id, version


def parse_feature_set_ref(ref: Any, path: str) -> tuple[str, int]:
    text = _text(ref, path)
    match = _FEATURE_SET_REF_RE.fullmatch(text)
    if not match:
        raise _error(path, "must be an exact numeric feature-set ref such as set_id@1")
    return match.group(1), int(match.group(2))


def feature_set_ref(set_id: str, version: int) -> str:
    return f"{set_id}@{version}"


def provider_ref(document: dict[str, Any]) -> str:
    return f"{document['provider_id']}@{document['version']}"


def template_ref(document: dict[str, Any]) -> str:
    return f"{document['template_id']}@{document['version']}"


def metric_ref(document: dict[str, Any]) -> str:
    return f"{document['metric_id']}@{document['version']}"


def metric_set_ref(document: dict[str, Any]) -> str:
    return f"{document['metric_set_id']}@{document['version']}"


def validate_provider(document: Any) -> dict[str, Any]:
    doc = _obj(document, {"schema_version", "provider_id", "version", "provider_key",
                          "implementation_version", "supported_steps"}, set(), "provider")
    if doc["schema_version"] != "provider_v1":
        raise _error("provider.schema_version", "must be provider_v1")
    provider_id = _text(doc["provider_id"], "provider.provider_id")
    if not _ID_RE.fullmatch(provider_id):
        raise _error("provider.provider_id", "must be a stable identifier")
    version = _version_tag(doc["version"], "provider.version")
    key = _text(doc["provider_key"], "provider.provider_key")
    if key not in _TRUSTED_PROVIDER_KEYS:
        raise _error("provider.provider_key", "is not a registered trusted implementation key")
    implementation_version = _text(doc["implementation_version"],
                                   "provider.implementation_version")
    steps = doc["supported_steps"]
    if not isinstance(steps, list) or not steps or any(type(step) is not int for step in steps):
        raise _error("provider.supported_steps", "must be a non-empty array of step integers")
    if len(set(steps)) != len(steps) or any(step not in {3, 4, 5} for step in steps):
        raise _error("provider.supported_steps", "values must be unique members of 3, 4, 5")
    return {**doc, "version": version, "provider_id": provider_id,
            "provider_key": key, "implementation_version": implementation_version,
            "supported_steps": steps}


def _validate_parameter_schema(schema: Any, path: str) -> dict[str, Any]:
    schema = _obj(schema, {"type", "properties", "required", "additionalProperties"}, set(), path)
    if schema["type"] != "object" or not isinstance(schema["properties"], dict):
        raise _error(path, "must be a JSON Schema object with properties")
    if schema["additionalProperties"] is not False:
        raise _error(path, "additionalProperties must be false")
    required = schema["required"]
    if not isinstance(required, list) or any(not isinstance(name, str) for name in required):
        raise _error(f"{path}.required", "must be an array of property names")
    if len(set(required)) != len(required) or not set(required).issubset(schema["properties"]):
        raise _error(f"{path}.required", "must uniquely name declared properties")
    properties = {}
    for name, raw_prop in schema["properties"].items():
        if not isinstance(name, str) or not _ID_RE.fullmatch(name):
            raise _error(f"{path}.properties", "property names must be stable identifiers")
        prop = _obj(raw_prop, {"type"}, {"default", "enum", "minimum", "maximum", "unit"},
                    f"{path}.properties.{name}")
        if prop["type"] not in _PARAM_TYPES:
            raise _error(f"{path}.properties.{name}.type", "uses an unsupported parameter type")
        if "unit" in prop:
            _text(prop["unit"], f"{path}.properties.{name}.unit")
        if "minimum" in prop:
            _finite_decimal(prop["minimum"], f"{path}.properties.{name}.minimum")
        if "maximum" in prop:
            _finite_decimal(prop["maximum"], f"{path}.properties.{name}.maximum")
        if ("minimum" in prop and "maximum" in prop
                and _finite_decimal(prop["minimum"], name) > _finite_decimal(prop["maximum"], name)):
            raise _error(f"{path}.properties.{name}", "minimum exceeds maximum")
        properties[name] = prop
    return {"type": "object", "properties": properties, "required": required,
            "additionalProperties": False}


def _validate_resource_common(document: Any, *, schema_version: str, resource_id_key: str,
                              extra_required: set[str], extra_optional: set[str]) -> dict[str, Any]:
    required = {"schema_version", resource_id_key, "version", "name", "description",
                "formula_ref", "provider_ref", "parameters_schema", "defaults", "output_schema",
                "unit", "status"} | extra_required
    optional = {"reason"} | extra_optional
    doc = _obj(document, required, optional, resource_id_key)
    if doc["schema_version"] != schema_version:
        raise _error(f"{resource_id_key}.schema_version", f"must be {schema_version}")
    resource_id = _text(doc[resource_id_key], f"{resource_id_key}.{resource_id_key}")
    if not _ID_RE.fullmatch(resource_id):
        raise _error(f"{resource_id_key}.{resource_id_key}", "must be a stable identifier")
    version = _version_tag(doc["version"], f"{resource_id_key}.version")
    _text(doc["name"], f"{resource_id_key}.name")
    _text(doc["description"], f"{resource_id_key}.description")
    parse_resource_ref(doc["formula_ref"], f"{resource_id_key}.formula_ref")
    parse_resource_ref(doc["provider_ref"], f"{resource_id_key}.provider_ref")
    param_schema = _validate_parameter_schema(doc["parameters_schema"],
                                              f"{resource_id_key}.parameters_schema")
    if not isinstance(doc["defaults"], dict):
        raise _error(f"{resource_id_key}.defaults", "must be an object")
    if set(doc["defaults"]) - set(param_schema["properties"]):
        raise _error(f"{resource_id_key}.defaults", "contains undeclared parameters")
    for param_name, param_value in doc["defaults"].items():
        _normalize_parameter(param_value, param_schema["properties"][param_name],
                             f"{resource_id_key}.defaults.{param_name}")
    if not isinstance(doc["output_schema"], dict) or not doc["output_schema"]:
        raise _error(f"{resource_id_key}.output_schema", "must be a non-empty object")
    for field, type_name in doc["output_schema"].items():
        _text(field, f"{resource_id_key}.output_schema field")
        _text(type_name, f"{resource_id_key}.output_schema.{field}")
    _text(doc["unit"], f"{resource_id_key}.unit")
    if (not isinstance(doc["status"], str)
            or doc["status"] not in {"ready", "pending_semantics"}):
        raise _error(f"{resource_id_key}.status", "must be ready or pending_semantics")
    if doc["status"] == "pending_semantics":
        _text(doc.get("reason"), f"{resource_id_key}.reason")
    elif "reason" in doc:
        _text(doc["reason"], f"{resource_id_key}.reason")
    doc = {**doc, "version": version, "parameters_schema": param_schema}
    for field in ("time_policy", "source_requirements", "validity", "sampling"):
        if field in doc and not isinstance(doc[field], dict):
            raise _error(f"{resource_id_key}.{field}", "must be an object")
    return doc


def validate_template(document: Any) -> dict[str, Any]:
    doc = _validate_resource_common(
        document, schema_version="feature_template_v1", resource_id_key="template_id",
        extra_required={"step", "kind", "family", "time_policy", "source_requirements",
                        "validity"},
        extra_optional={"sampling"},
    )
    if type(doc["step"]) is not int or doc["step"] not in {3, 4, 5}:
        raise _error("template.step", "must be 3, 4, or 5")
    expected_kind = {3: "primitive", 4: "business", 5: "statistical"}[doc["step"]]
    if doc["kind"] != expected_kind:
        raise _error("template.kind", f"step {doc['step']} requires kind {expected_kind}")
    _text(doc["family"], "template.family")
    return doc


def validate_metric(document: Any) -> dict[str, Any]:
    return _validate_resource_common(
        document, schema_version="metric_v1", resource_id_key="metric_id",
        extra_required=set(), extra_optional=set(),
    )


def validate_metric_set(document: Any) -> dict[str, Any]:
    doc = _obj(document, {"schema_version", "metric_set_id", "version", "metrics"}, set(),
               "metric_set")
    if doc["schema_version"] != "metric_set_v1":
        raise _error("metric_set.schema_version", "must be metric_set_v1")
    metric_set_id = _text(doc["metric_set_id"], "metric_set.metric_set_id")
    if not _ID_RE.fullmatch(metric_set_id):
        raise _error("metric_set.metric_set_id", "must be a stable identifier")
    version = _positive_int(doc["version"], "metric_set.version")
    if not isinstance(doc["metrics"], list) or not doc["metrics"]:
        raise _error("metric_set.metrics", "must be a non-empty array")
    metrics = []
    for index, ref in enumerate(doc["metrics"]):
        parse_resource_ref(ref, f"metric_set.metrics[{index}]")
        metrics.append(ref)
    if len(set(metrics)) != len(metrics):
        raise _error("metric_set.metrics", "metric refs must be unique")
    return {**doc, "version": version, "metrics": metrics}


def validate_feature_set_draft(document: Any) -> dict[str, Any]:
    required = {"schema_version", "feature_set_id", "version", "name", "description", "items",
                "created_by", "change_reason"}
    optional = {"parent_ref", "defaults", "status", "draft_revision", "created_at", "updated_at",
                "updated_by"}
    doc = _obj(document, required, optional, "feature_set")
    if doc["schema_version"] != "feature_set_v1":
        raise _error("feature_set.schema_version", "must be feature_set_v1")
    set_id = _text(doc["feature_set_id"], "feature_set.feature_set_id")
    if not _ID_RE.fullmatch(set_id):
        raise _error("feature_set.feature_set_id", "must be a stable identifier")
    version = _positive_int(doc["version"], "feature_set.version")
    _text(doc["name"], "feature_set.name")
    _text(doc["description"], "feature_set.description")
    _feature_actor(doc["created_by"], "feature_set.created_by")
    _text(doc["change_reason"], "feature_set.change_reason")
    if "parent_ref" in doc:
        parse_feature_set_ref(doc["parent_ref"], "feature_set.parent_ref")
    if doc.get("status", "draft") != "draft":
        raise _error("feature_set.status", "only drafts can be saved")
    if "draft_revision" in doc:
        _positive_int(doc["draft_revision"], "feature_set.draft_revision")
    defaults = doc.get("defaults", {})
    if not isinstance(defaults, dict):
        raise _error("feature_set.defaults", "must be an object")
    allowed_defaults = {"lookback", "profile_window", "sampling", "validity"}
    if set(defaults) - allowed_defaults:
        raise _error("feature_set.defaults", "only lookback/profile_window/sampling/validity defaults are supported")
    normalized_defaults = _normalize_time_overrides(defaults, "feature_set.defaults")
    items = doc["items"]
    if not isinstance(items, list):
        raise _error("feature_set.items", "must be an array")
    normalized_items = []
    item_ids: set[str] = set()
    aliases: set[str] = set()
    for index, item in enumerate(items):
        path = f"feature_set.items[{index}]"
        item = _obj(item, {"item_id"}, {"alias", "feature_ref", "recipe"}, path)
        item_id = _text(item["item_id"], f"{path}.item_id")
        alias = _text(item.get("alias", item_id), f"{path}.alias")
        if item_id in item_ids:
            raise _error(path, "item_id values must be unique")
        if alias in aliases:
            raise _error(path, "alias values must be unique")
        item_ids.add(item_id)
        aliases.add(alias)
        if ("feature_ref" in item) == ("recipe" in item):
            raise _error(path, "exactly one of feature_ref or recipe is required")
        if "feature_ref" in item:
            parse_feature_ref(item["feature_ref"], f"{path}.feature_ref")
            normalized_items.append({"item_id": item_id, "alias": alias,
                                     "feature_ref": item["feature_ref"]})
        else:
            normalized_items.append({"item_id": item_id, "alias": alias,
                                     "recipe": _validate_recipe(item["recipe"], path)})
    result = {"schema_version": "feature_set_v1", "feature_set_id": set_id,
              "version": version, "name": doc["name"], "description": doc["description"],
              "items": normalized_items, "defaults": normalized_defaults,
              "created_by": _feature_actor(doc["created_by"], "feature_set.created_by"),
              "change_reason": doc["change_reason"]}
    if "parent_ref" in doc:
        result["parent_ref"] = doc["parent_ref"]
    try:
        canonical_json(result)
    except (TypeError, ValueError) as exc:
        raise _error("feature_set", f"contains invalid JSON data: {exc}") from exc
    return result


def _validate_recipe(value: Any, item_path: str) -> dict[str, Any]:
    path = f"{item_path}.recipe"
    required = {"template_ref", "step", "parameters"}
    optional = {"input", "inputs", "lookback", "profile_window", "max_endpoint_age",
                "sampling", "validity", "metrics", "metric_set_ref"}
    recipe = _obj(value, required, optional, path)
    parse_resource_ref(recipe["template_ref"], f"{path}.template_ref")
    if type(recipe["step"]) is not int or recipe["step"] not in {3, 4, 5}:
        raise _error(f"{path}.step", "must be 3, 4, or 5")
    if not isinstance(recipe["parameters"], dict):
        raise _error(f"{path}.parameters", "must be an object")
    result = {"template_ref": recipe["template_ref"], "step": recipe["step"],
              "parameters": recipe["parameters"]}
    if "input" in recipe and "inputs" in recipe:
        raise _error(path, "input and inputs are mutually exclusive")
    if "input" in recipe:
        result["input"] = _validate_input_reference(recipe["input"], f"{path}.input")
    if "inputs" in recipe:
        if not isinstance(recipe["inputs"], dict) or not recipe["inputs"]:
            raise _error(f"{path}.inputs", "must be a non-empty object")
        inputs = {}
        for role, inp in recipe["inputs"].items():
            role = _text(role, f"{path}.inputs role")
            if not _ID_RE.fullmatch(role):
                raise _error(f"{path}.inputs role", "must be a stable identifier")
            inputs[role] = _validate_input_reference(inp, f"{path}.inputs.{role}")
        result["inputs"] = inputs
    for field in ("lookback", "profile_window"):
        if field in recipe:
            result[field] = normalize_duration(recipe[field], f"{path}.{field}")
    if "max_endpoint_age" in recipe:
        result["max_endpoint_age"] = normalize_duration(
            recipe["max_endpoint_age"], f"{path}.max_endpoint_age"
        )
    if "sampling" in recipe:
        result["sampling"] = _normalize_sampling(recipe["sampling"], f"{path}.sampling")
    if "validity" in recipe:
        result["validity"] = _normalize_validity(recipe["validity"], f"{path}.validity")
    if "metrics" in recipe and "metric_set_ref" in recipe:
        raise _error(path, "metrics and metric_set_ref are mutually exclusive")
    if "metrics" in recipe:
        if not isinstance(recipe["metrics"], list) or not recipe["metrics"]:
            raise _error(f"{path}.metrics", "must be a non-empty array")
        refs = []
        for index, ref in enumerate(recipe["metrics"]):
            parse_resource_ref(ref, f"{path}.metrics[{index}]")
            refs.append(ref)
        if len(set(refs)) != len(refs):
            raise _error(f"{path}.metrics", "metric refs must be unique")
        result["metrics"] = refs
    if "metric_set_ref" in recipe:
        parse_resource_ref(recipe["metric_set_ref"], f"{path}.metric_set_ref")
        result["metric_set_ref"] = recipe["metric_set_ref"]
    return result


def _validate_input_reference(value: Any, path: str) -> dict[str, Any]:
    inp = _obj(value, set(), {"input_feature_ref", "item_id", "output_id"}, path)
    has_feature = "input_feature_ref" in inp
    has_item = "item_id" in inp
    if has_feature == has_item:
        raise _error(path, "requires exactly input_feature_ref or item_id")
    if has_feature:
        if "output_id" in inp:
            raise _error(path, "output_id is only valid with item_id")
        parse_feature_ref(inp["input_feature_ref"], f"{path}.input_feature_ref")
        return {"input_feature_ref": inp["input_feature_ref"]}
    item_id = _text(inp["item_id"], f"{path}.item_id")
    out = {"item_id": item_id}
    if "output_id" in inp:
        out["output_id"] = _text(inp["output_id"], f"{path}.output_id")
    return out


def normalize_duration(value: Any, path: str) -> str:
    if not isinstance(value, str):
        raise _error(path, "must be a duration string")
    text = value.strip()
    match = _DURATION_RE.fullmatch(text)
    total_ns: Decimal
    try:
        if match:
            total_ns = Decimal(match.group(1)) * _NS_FACTORS[match.group(2)]
        else:
            iso = _ISO_DURATION_RE.fullmatch(text)
            if iso is None or not any(iso.groupdict().values()):
                raise _error(path, "duration must use ns/us/ms/s/m/h/d or an ISO-8601 PnDTnHnMnS form")
            parts = {name: Decimal(number or "0") for name, number in iso.groupdict().items()}
            total_ns = (parts["days"] * _NS_FACTORS["d"]
                        + parts["hours"] * _NS_FACTORS["h"]
                        + parts["minutes"] * _NS_FACTORS["m"]
                        + parts["seconds"] * _NS_FACTORS["s"])
    except InvalidOperation as exc:
        raise _error(path, "duration must be finite and positive") from exc
    if not total_ns.is_finite() or total_ns <= 0 or total_ns != total_ns.to_integral_value():
        raise _error(path, "duration must be positive and represent an integer number of nanoseconds")
    return f"{int(total_ns)}ns"


def _normalize_time_overrides(value: dict[str, Any], path: str) -> dict[str, Any]:
    result = {}
    for field in ("lookback", "profile_window"):
        if field in value:
            result[field] = normalize_duration(value[field], f"{path}.{field}")
    if "sampling" in value:
        result["sampling"] = _normalize_sampling(value["sampling"], f"{path}.sampling")
    if "validity" in value:
        result["validity"] = _normalize_validity(value["validity"], f"{path}.validity")
    return result


def _normalize_sampling(value: Any, path: str) -> dict[str, Any]:
    allowed = {"axis", "time_basis", "interval", "evaluation_stride", "max_feature_age",
               "max_quote_age", "max_age", "as_of", "alignment", "grid_origin", "on_gap",
               "on_session_boundary", "resampling", "missing_policy", "gap_policy_ref"}
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise _error(path, "must be an object")
    unknown = set(value) - allowed
    if unknown:
        raise _error(path, f"unknown sampling fields: {', '.join(sorted(unknown))}")
    result = dict(value)
    valid_axes = {"clock_time", "event_time", "quote_event_index"}
    if ("axis" in result
            and (not isinstance(result["axis"], str) or result["axis"] not in valid_axes)):
        raise _error(f"{path}.axis", "must be clock_time, event_time, or quote_event_index")
    if "time_basis" in result:
        if not isinstance(result["time_basis"], str) or result["time_basis"] not in valid_axes:
            raise _error(f"{path}.time_basis", "must be clock_time, event_time, or quote_event_index")
        if "axis" in result and result["axis"] != result["time_basis"]:
            raise _error(path, "axis and time_basis must agree")
        result["axis"] = result.pop("time_basis")
    if "max_age" in result:
        if "max_feature_age" in result:
            raise _error(path, "max_age and max_feature_age cannot both be set")
        result["max_feature_age"] = result.pop("max_age")
    for field in ("interval", "evaluation_stride", "max_feature_age", "max_quote_age"):
        if field in result:
            result[field] = normalize_duration(result[field], f"{path}.{field}")
    if "alignment" in result:
        alignment = result.pop("alignment")
        if not isinstance(alignment, str) or alignment not in {"backward_asof", "backward", "exact"}:
            raise _error(f"{path}.alignment", "must be backward_asof, backward, or exact")
        if "as_of" in result and result["as_of"] != alignment:
            raise _error(path, "alignment and as_of must agree")
        result["as_of"] = alignment
    if "as_of" in result:
        if not isinstance(result["as_of"], str) or result["as_of"] not in {
            "backward", "backward_asof", "exact"
        }:
            raise _error(f"{path}.as_of", "must be backward_asof, backward, or exact")
        if result["as_of"] == "backward":
            result["as_of"] = "backward_asof"
    if "grid_origin" in result and result["grid_origin"] != "session_start":
        raise _error(f"{path}.grid_origin", "must be session_start")
    if ("on_gap" in result
            and (not isinstance(result["on_gap"], str)
                 or result["on_gap"] not in {"invalidate_window", "mark_na"})):
        raise _error(f"{path}.on_gap", "must be invalidate_window or mark_na")
    if "on_session_boundary" in result and result["on_session_boundary"] != "reset":
        raise _error(f"{path}.on_session_boundary", "must be reset")
    if "resampling" in result and not isinstance(result["resampling"], dict):
        raise _error(f"{path}.resampling", "must be an object")
    for field in ("missing_policy", "gap_policy_ref"):
        if field in result:
            _text(result[field], f"{path}.{field}")
    if "gap_policy_ref" in result:
        parse_resource_ref(result["gap_policy_ref"], f"{path}.gap_policy_ref")
    return result


def _normalize_validity(value: Any, path: str) -> dict[str, Any]:
    allowed = {"min_samples", "missing_policy", "zero_denominator_policy", "stale_policy",
               "reason_codes", "allow_partial_window", "emit_subsegment_summaries"}
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise _error(path, "must be an object")
    unknown = set(value) - allowed
    if unknown:
        raise _error(path, f"unknown validity fields: {', '.join(sorted(unknown))}")
    result = dict(value)
    if "min_samples" in result:
        result["min_samples"] = _positive_int(result["min_samples"], f"{path}.min_samples")
    for field in ("allow_partial_window", "emit_subsegment_summaries"):
        if field in result and type(result[field]) is not bool:
            raise _error(f"{path}.{field}", "must be a boolean")
    for field in ("missing_policy", "zero_denominator_policy", "stale_policy"):
        if field in result:
            _text(result[field], f"{path}.{field}")
    if ("reason_codes" in result
            and (not isinstance(result["reason_codes"], list)
                 or any(not isinstance(code, str) or not code for code in result["reason_codes"]))):
        raise _error(f"{path}.reason_codes", "must be an array of non-empty strings")
    return result


def _finite_decimal(value: Any, path: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise _error(path, "must be a finite number")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise _error(path, "must be a finite number") from exc
    if not number.is_finite():
        raise _error(path, "must be a finite number")
    return number


def _version_tag(value: Any, path: str) -> str:
    if type(value) is int:
        if value < 1:
            raise _error(path, "integer versions must be >= 1")
        version = str(value)
    else:
        version = _text(value, path)
    if not _RESOURCE_VERSION_RE.fullmatch(version) or version.lower() in {
        "latest", "current", "head", "main"
    }:
        raise _error(path, "must be an exact immutable version tag")
    return version


def _normalize_parameter(value: Any, spec: dict[str, Any], path: str) -> Any:
    kind = spec["type"]
    if kind == "string":
        if not isinstance(value, str):
            raise _error(path, "must be a string")
        normalized = value
    elif kind == "integer":
        if type(value) is not int:
            raise _error(path, "must be an integer")
        normalized = value
    elif kind == "number":
        number = _finite_decimal(value, path)
        normalized = int(number) if number == number.to_integral_value() else float(number)
        if isinstance(normalized, float) and not math.isfinite(normalized):
            raise _error(path, "must be a finite number")
    elif kind == "decimal":
        normalized = format(_finite_decimal(value, path).normalize(), "f")
        if normalized in {"-0", "-0.0"}:
            normalized = "0"
    elif kind == "duration":
        normalized = normalize_duration(value, path)
    elif kind == "boolean":
        if type(value) is not bool:
            raise _error(path, "must be a boolean")
        normalized = value
    elif kind == "object":
        if not isinstance(value, dict):
            raise _error(path, "must be an object")
        if path.endswith(".size"):
            size = _obj(value, {"value", "unit"}, set(), path)
            amount = _finite_decimal(size["value"], f"{path}.value")
            if amount <= 0:
                raise _error(f"{path}.value", "size must be positive")
            normalized = {"value": format(amount.normalize(), "f"),
                          "unit": _text(size["unit"], f"{path}.unit")}
            if normalized["value"] in {"-0", "-0.0"}:
                normalized["value"] = "0"
        else:
            normalized = _normalize_nested_numbers(value, path)
    elif kind == "array":
        if not isinstance(value, list):
            raise _error(path, "must be an array")
        normalized = value
    else:  # pragma: no cover - guarded by parameter schema validation
        raise _error(path, "unsupported parameter type")
    if "enum" in spec:
        enum_values = [_normalize_parameter(item, {"type": kind}, f"{path}.enum")
                       for item in spec["enum"]]
        if normalized not in enum_values:
            raise _error(path, "value is not in the declared enum")
    if kind in {"integer", "number", "decimal"}:
        comparison_value = _finite_decimal(normalized, path)
        if "minimum" in spec and comparison_value < _finite_decimal(spec["minimum"], path):
            raise _error(path, "is below the declared minimum")
        if "maximum" in spec and comparison_value > _finite_decimal(spec["maximum"], path):
            raise _error(path, "is above the declared maximum")
    return normalized


def normalize_parameters(value: dict[str, Any], schema: dict[str, Any], path: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise _error(path, "must be an object with string keys")
    properties = schema["properties"]
    unknown = set(value) - set(properties)
    if unknown:
        raise _error(path, f"unknown parameters: {', '.join(sorted(unknown))}")
    missing = set(schema["required"]) - set(value)
    if missing:
        raise _error(path, f"missing required parameters: {', '.join(sorted(missing))}")
    return {key: _normalize_parameter(val, properties[key], f"{path}.{key}")
            for key, val in sorted(value.items())}


def parse_feature_ref(value: Any, path: str) -> tuple[str, int]:
    return parse_feature_set_ref(value, path)


def _feature_actor(value: Any, path: str) -> dict[str, str]:
    actor = _obj(value, {"kind", "id"}, set(), path)
    if not isinstance(actor["kind"], str) or actor["kind"] not in {"human", "agent"}:
        raise _error(f"{path}.kind", "must be human or agent")
    return {"kind": actor["kind"], "id": _text(actor["id"], f"{path}.id")}


def resource_ref_from_document(document: dict[str, Any], id_key: str) -> str:
    return f"{document[id_key]}@{document['version']}"


def _normalize_nested_numbers(value: Any, path: str) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if isinstance(value, float):
        number = _finite_decimal(value, path)
        return int(number) if number == number.to_integral_value() else format(number.normalize(), "f")
    if isinstance(value, list):
        return [_normalize_nested_numbers(item, f"{path}[]") for item in value]
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise _error(path, "object keys must be strings")
        return {key: _normalize_nested_numbers(item, f"{path}.{key}")
                for key, item in sorted(value.items())}
    raise _error(path, f"contains non-JSON value type {type(value).__name__}")
