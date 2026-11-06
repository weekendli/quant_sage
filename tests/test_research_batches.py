from __future__ import annotations

import copy
import json
from dataclasses import replace

import pytest

from quantsage_discovery.cli import main
from quantsage_discovery.core.artifacts import artifact_path
from quantsage_discovery.core.errors import ConfigError
from quantsage_discovery.core.research import BatchEngine, batch_status
from tests.research_provider import ArithmeticCatalog


@pytest.fixture
def environment(tmp_path):
    from tests.research_provider import make_environment

    return make_environment(tmp_path)


def engine(env, config=None):
    cfg, registry, store = env
    return BatchEngine(config or cfg, registry=registry, artifact_store=store)


def test_iterative_time_and_feature_expansion(environment):
    cfg, registry, _ = environment
    a = engine(environment).run_batch(run_id="A")
    assert a["summary"] == "COMPLETE"
    assert sum(r["selection"] == "REQUESTED" for r in a["items"]) == 3
    assert next(x for x in a["inventory"] if x["alias"] == "long")["selection"] == "NOT_SELECTED"
    assert a["state_counts"] == {"SUCCEEDED": 5}  # base, business, two metrics, quality
    for row in a["items"]:
        for part in row["partitions"]:
            for raw in part["artifacts"].values():
                payload = json.loads(artifact_path(raw["uri"]).read_text())
                if row["kind"] == "statistical":
                    assert payload["values"] in ([6.0], [9])
    bcfg = copy.deepcopy(cfg)
    bcfg["research_batch"].update(id="B", parent_batch_ref="A")
    bcfg["research_batch"]["data_scope"]["output_interval"]["end"] = "2026-01-07T00:00:00+00:00"
    b = engine(environment, bcfg).run_batch(run_id="B")
    assert b["state_counts"] == {"CACHED": 4, "SUCCEEDED": 5}
    assert all(p["metrics"]["n_valid"] > 0 for r in b["items"] for p in r["partitions"])
    ccfg = copy.deepcopy(bcfg)
    ccfg["research_batch"].update(id="C", parent_batch_ref="B")
    ccfg["research_batch"]["targets"]["business_features"].append("long")
    c = engine(environment, ccfg).run_batch(run_id="C")
    assert c["state_counts"] == {"CACHED": 8, "SUCCEEDED": 3}
    assert len(c["delta"]["added_definitions"]) == 1
    assert not c["delta"]["added_partitions"]
    assert batch_status(registry, "A")["state_counts"] == {"SUCCEEDED": 5}
    assert c["validation"][0]["partitions"][0]["metrics"]["verdict"] == "PASS"


def test_failure_resume_partial_and_attempt_history(environment):
    cfg, _, _ = environment
    cfg["research_batch"]["data_scope"]["output_interval"]["end"] = "2026-01-07T00:00:00+00:00"
    ArithmeticCatalog.fail_start = "2026-01-06T00:00:00+00:00"
    failed = engine(environment).run_batch(run_id="failure")
    business = next(r for r in failed["items"] if r["kind"] == "business")
    assert business["summary"] == "PARTIAL"
    assert failed["status"] == "FAILED"
    ArithmeticCatalog.fail_start = None
    recovered = engine(environment).run_batch(resume="failure")
    assert recovered["status"] == "SUCCEEDED"
    business = next(r for r in recovered["items"] if r["kind"] == "business")
    assert business["partitions"][1]["attempts"] == 2
    assert [e["state"] for e in business["partitions"][1]["history"]] == [
        "RUNNING",
        "FAILED",
        "RUNNING",
        "SUCCEEDED",
    ]


def test_statistical_only_reuses_upstream_and_blocks_missing(environment):
    cfg, _, _ = environment
    engine(environment).run_batch(run_id="seed")
    cfg = copy.deepcopy(cfg)
    cfg["research_batch"]["targets"]["business_features"] = []
    cfg["research_batch"]["execution"]["allowed_stages"] = ["statistical_atlas", "quality"]
    report = engine(environment, cfg).run_batch(run_id="stats_only")
    assert report["status"] == "SUCCEEDED"
    cfg["research_batch"]["data_scope"]["output_interval"]["end"] = "2026-01-07T00:00:00+00:00"
    blocked = engine(environment, cfg).run_batch(run_id="missing")
    assert blocked["status"] == "FAILED"
    assert blocked["state_counts"]["BLOCKED"] > 0
    assert not any(
        p["state"] == "SUCCEEDED"
        for r in blocked["items"]
        if r["kind"] == "primitive"
        for p in r["partitions"]
    )


def test_empty_selection_means_no_work(environment):
    cfg, _, _ = environment
    cfg["research_batch"]["targets"] = {}
    report = engine(environment).run_batch(run_id="empty")
    assert report["summary"] == "NO_WORK" and report["items"] == []
    assert all(r["selection"] == "NOT_SELECTED" for r in report["inventory"])


@pytest.mark.parametrize(
    "mutation",
    [
        lambda c: c["business_features"]["items"][0].update(space=["d0", "d1"]),
        lambda c: c["business_features"]["items"][0].update(size="q25"),
        lambda c: c["business_features"]["items"][1].update(id="short"),
        lambda c: c["statistical_features"]["items"][0].update(metrics=["unknown"]),
        lambda c: c["statistical_features"]["items"][0].update(metric_set="toy_pair"),
        lambda c: c["research_batch"]["data_scope"]["output_interval"].update(start="2026-01-05"),
        lambda c: c["research_batch"]["validation"].update(mode="confirmatory"),
        lambda c: c["research_batch"]["targets"].update(business_features=["missing"]),
        lambda c: c["research_batch"]["data_scope"]["output_interval"].update(
            end="2026-03-01T00:00:00Z"
        ),
    ],
)
def test_invalid_requests_rejected(environment, mutation):
    cfg, _, _ = environment
    mutation(cfg)
    with pytest.raises(ConfigError):
        engine(environment)


def test_alias_order_and_metric_sets_do_not_change_cache(environment):
    cfg, _, _ = environment
    engine(environment).run_batch(run_id="seed")
    item = cfg["statistical_features"]["items"][0]
    item.pop("metrics")
    item["metric_set"] = "toy_pair"
    cfg["business_features"]["items"].reverse()
    cfg["research_batch"]["id"] = "reordered"
    result = engine(environment).run_batch(run_id="reordered")
    assert result["state_counts"] == {"CACHED": 5}


def test_valid_empty_result_and_corrupt_cache(environment):
    ArithmeticCatalog.empty_start = "2026-01-05T00:00:00+00:00"
    report = engine(environment).run_batch(run_id="empty_values")
    business = next(r for r in report["items"] if r["kind"] == "business")
    assert business["summary"] == "COMPLETE"
    assert business["partitions"][0]["metrics"]["n_valid"] == 0
    raw = next(iter(business["partitions"][0]["artifacts"].values()))
    artifact_path(raw["uri"]).write_text("corrupt")
    ArithmeticCatalog.empty_start = None
    result = engine(environment).run_batch(run_id="repair")
    assert result["status"] == "SUCCEEDED"
    assert (
        next(r for r in result["items"] if r["kind"] == "business")["partitions"][0]["state"]
        == "SUCCEEDED"
    )


def test_cycle_budget_and_scope_boundaries(environment, monkeypatch):
    cfg, _, _ = environment
    cfg["resources"]["max_feature_candidates"] = 2
    with pytest.raises(ConfigError, match="budget"):
        engine(environment)
    cfg["resources"]["max_feature_candidates"] = 40
    cfg["resources"]["max_work_partitions"] = 1
    cfg["research_batch"]["data_scope"]["output_interval"]["end"] = "2026-01-07T00:00:00Z"
    with pytest.raises(ConfigError, match="partition budget"):
        engine(environment)
    cfg["resources"]["max_work_partitions"] = 10
    original = ArithmeticCatalog.get

    def cyclic(self, key):
        definition = original(self, key)
        return replace(definition, dependencies=("base",)) if key == "base" else definition

    monkeypatch.setattr(ArithmeticCatalog, "get", cyclic)
    with pytest.raises(ConfigError, match="cycle"):
        engine(environment)


def test_cache_only_policy_never_runs_missing_work(environment):
    cfg, _, _ = environment
    cfg["research_batch"]["execution"]["allowed_stages"] = []
    blocked = engine(environment).run_batch(run_id="no_authorization")
    assert blocked["status"] == "FAILED"
    assert set(blocked["state_counts"]) == {"BLOCKED"}


def test_resume_rejects_changed_config(environment):
    cfg, _, _ = environment
    engine(environment).run_batch(run_id="seed")
    cfg["seed"] = 2
    with pytest.raises(ConfigError, match="frozen"):
        engine(environment).run_batch(resume="seed")


def test_cli_plan_run_status_resume(environment, tmp_path, capsys):
    import yaml

    cfg, _, _ = environment
    path = tmp_path / "batch.yaml"
    path.write_text(yaml.safe_dump(cfg))
    assert main(["plan", "--config", str(path)]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert len(plan["research_plan"]["requested"]) == 3
    assert main(["run", "--config", str(path), "--run-id", "cli_batch"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["summary"] == "COMPLETE"
    assert main(["status", "--config", str(path), "--run-id", "cli_batch"]) == 0
    assert json.loads(capsys.readouterr().out)["items"]
    assert main(["run", "--resume", "cli_batch", "--registry", cfg["storage"]["registry_uri"]]) == 0
    assert json.loads(capsys.readouterr().out)["summary"] == "COMPLETE"
    assert main(["artifacts", "--config", str(path), "--run-id", "cli_batch"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 5
    assert (
        main(
            [
                "status",
                "--config",
                str(path),
                "--run-id",
                "cli_batch",
                "--item",
                "short",
                "--item-kind",
                "business",
                "--item-state",
                "SUCCEEDED",
            ]
        )
        == 0
    )
    filtered = json.loads(capsys.readouterr().out)
    assert len(filtered["items"]) == 1
    assert filtered["items"][0]["aliases"] == ["short"]


def test_pending_definition_never_executes(environment, monkeypatch):
    original = ArithmeticCatalog.resolve

    def pending(self, kind, parameters):
        definition = original(self, kind, parameters)
        if kind == "business":
            definition = replace(definition, pending_reason="aggregation_not_defined")
            self.definitions[definition.key] = definition
        return definition

    monkeypatch.setattr(ArithmeticCatalog, "resolve", pending)
    report = engine(environment).run_batch(run_id="pending_semantics")
    item = next(x for x in report["items"] if x["kind"] == "business")
    assert item["summary"] == "BLOCKED"
    assert item["partitions"][0]["attempts"] == 0


def test_missing_payload_and_invalid_source_never_reused(environment):
    cfg, _, _ = environment
    first = engine(environment).run_batch(run_id="seed")
    business = next(x for x in first["items"] if x["kind"] == "business")
    ref = next(iter(business["partitions"][0]["artifacts"].values()))
    artifact_path(ref["uri"]).unlink()
    next_run = engine(environment).run_batch(run_id="missing_payload")
    assert (
        next(x for x in next_run["items"] if x["kind"] == "business")["partitions"][0]["state"]
        == "SUCCEEDED"
    )
    cfg["research_batch"]["data_scope"]["snapshot_ref"] = "unknown_snapshot"
    with pytest.raises(ConfigError, match="not found"):
        engine(environment)
