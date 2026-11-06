from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from quantsage_discovery.core import (
    ArtifactRef,
    ArtifactStore,
    ArtifactValidationError,
    GraphError,
    Registry,
    StageResult,
    StageStatus,
    WorkflowEngine,
    WorkflowError,
)
from quantsage_discovery.core.artifacts import artifact_path
from tests.synthetic_stages import (
    AtomicWriter,
    EmptyInput,
    FailsValidation,
    JsonStage,
    MutatesAfterRegister,
    RetriesOnce,
    read_record,
)

ROOT = Path(__file__).resolve().parents[1]


class GlobalCodeError(ValueError):
    code = "MISSING_FIELD"


class NoArtifactStage(JsonStage):
    name = "empty_source"

    def __init__(self):
        super().__init__(name="empty_source")

    def execute(self, ctx, inputs):
        return StageResult()


class MissingInputStage(JsonStage):
    name = "missing_input"

    def __init__(self):
        super().__init__(name="missing_input", dependencies=("empty_source",))

    def validate(self, ctx, inputs):
        self.validate_calls += 1
        if "empty_source.result" not in inputs:
            raise GlobalCodeError("required upstream artifact is absent")


def make_engine(tmp_path: Path, stages=(), config=None):
    config = config or {}
    artifacts = ArtifactStore(tmp_path / "artifacts")
    registry = Registry(tmp_path / "registry.sqlite")
    return WorkflowEngine(stages=stages, config=config,
                          artifact_store=artifacts, registry=registry), artifacts, registry


def stage_status(summary, name):
    return summary.stage_results[name][0]["status"]


def test_graph_topology_rejects_missing_dependency_and_cycle(tmp_path):
    later = JsonStage(name="z_later", dependencies=("a_first",))
    first = JsonStage(name="a_first")
    engine, _, _ = make_engine(tmp_path, [later, first])
    assert engine.topological_order() == ["a_first", "z_later"]

    missing_engine, _, _ = make_engine(tmp_path / "missing", [JsonStage(name="child", dependencies=("absent",))])
    with pytest.raises(GraphError, match="missing dependency.*absent"):
        missing_engine.topological_order()

    cyclic_engine, _, _ = make_engine(tmp_path / "cycle", [
        JsonStage(name="a", dependencies=("b",)),
        JsonStage(name="b", dependencies=("a",)),
    ])
    with pytest.raises(GraphError, match="dependency cycle"):
        cyclic_engine.topological_order()


def test_empty_dag_is_valid_and_explicit(tmp_path):
    engine, _, _ = make_engine(tmp_path)
    assert engine.plan()["message"] == "No stages registered; no work will run."
    summary = engine.run(run_id="empty-run")
    assert summary.status == "SUCCEEDED"
    assert summary.message == "No stages registered; no work was executed."
    assert summary.stage_results == {}


def test_success_cache_and_provenance_are_serialized(tmp_path):
    stage = JsonStage(name="only_stage", value="serialized")
    engine, artifacts, registry = make_engine(tmp_path, [stage], {"data_snapshot_id": "snapshot-a",
                                                                   "splits": {"split_id": "split-a"}})
    first = engine.run(run_id="first")
    ref = ArtifactRef.from_dict(first.stage_results["only_stage"][0]["artifacts"]["result"])
    manifest = artifacts.verify(ref)
    assert manifest.data_snapshot_id == "snapshot-a"
    assert manifest.split_id == "split-a"
    assert manifest.stage_name == "only_stage"
    assert manifest.input_artifact_ids == ()
    assert manifest.artifact_id == "art_" + manifest.content_hash.removeprefix("sha256:")
    assert json.loads(Path(ref.manifest_uri.removeprefix("file://")).read_text()) == manifest.to_dict()
    assert read_record(ref) == {"value": "serialized"}

    second = engine.run(run_id="second")
    assert stage_status(first, "only_stage") == "SUCCEEDED"
    assert stage_status(second, "only_stage") == "CACHED"
    assert stage.execute_calls == 1
    assert len(registry.get_run_stages("first")) == 1
    assert len(registry.get_run_stages("second")) == 1
    assert registry.get_run_stages("second")[0]["source_stage_run_id"] == registry.get_run_stages("first")[0]["stage_run_id"]


def test_config_change_propagates_to_descendants_only_and_force_is_scoped(tmp_path):
    source = JsonStage(name="source", config_keys=("threshold",), value="no-signal")
    descendant = JsonStage(name="descendant", dependencies=("source",), config_keys=(), value="child")
    sibling = JsonStage(name="sibling", config_keys=("unrelated",), value="sibling")
    engine, _, _ = make_engine(tmp_path, [source, descendant, sibling],
                               {"threshold": 1, "unrelated": "same"})
    engine.run(run_id="config-1")
    changed = engine.run(run_id="config-2", config={"threshold": 2, "unrelated": "same"})
    assert stage_status(changed, "source") == "SUCCEEDED"
    assert stage_status(changed, "descendant") == "SUCCEEDED"
    assert stage_status(changed, "sibling") == "CACHED"
    assert source.execute_calls == 2
    assert descendant.execute_calls == 2
    assert sibling.execute_calls == 1

    forced = engine.run(run_id="forced", force_stages=["source"])
    assert stage_status(forced, "source") == "SUCCEEDED"
    assert stage_status(forced, "descendant") == "SUCCEEDED"
    assert stage_status(forced, "sibling") == "CACHED"
    assert source.execute_calls == 3
    assert descendant.execute_calls == 3
    assert sibling.execute_calls == 1


def test_unscoped_plugin_hashes_entire_config(tmp_path):
    stage = JsonStage(name="reads_global", config_keys=None,
                      value=lambda ctx, inputs: ctx.config["threshold"])
    engine, _, _ = make_engine(tmp_path, [stage], {"threshold": 1, "other": "unchanged"})
    engine.run(run_id="global-a")
    changed = engine.run(run_id="global-b", config={"threshold": 2, "other": "unchanged"})
    assert stage_status(changed, "reads_global") == "SUCCEEDED"
    assert stage.execute_calls == 2


def test_partition_resume_after_real_process_exit_restores_scopes(tmp_path):
    registry_path = tmp_path / "registry.sqlite"
    artifact_root = tmp_path / "artifacts"
    marker = tmp_path / "crash-once.marker"
    config = {
        "project": "crash-resume",
        "seed": 7,
        "storage": {"artifacts_uri": str(artifact_root), "registry_uri": str(registry_path)},
        "resources": {"max_concurrent_stages": 1},
        "crash_marker": str(marker),
        "workflow": {"stages": [{"import": "tests.synthetic_stages:CrashByPartition",
                                   "config_keys": [], "partitioned": True}]},
    }
    config_path = tmp_path / "workflow.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    run_id = "killed-partition-run"
    common = [sys.executable, "-m", "quantsage_discovery.cli", "run", "--config", str(config_path),
              "--run-id", run_id, "--partition-scope", "item=a", "--partition-scope", "item=b"]
    first = subprocess.run(common, cwd=ROOT, text=True, capture_output=True, check=False)
    assert first.returncode == 23, first.stderr

    registry = Registry(registry_path)
    interrupted = registry.get_run(run_id)
    assert interrupted["status"] == "RUNNING"
    assert interrupted["execution"]["partitions"] == [{"item": "a"}, {"item": "b"}]
    before = registry.get_run_stages(run_id)
    assert len(before) == 2
    assert {row["partition"]["item"]: row["status"] for row in before} == {
        "a": StageStatus.SUCCEEDED.value, "b": StageStatus.RUNNING.value,
    }
    original_a = before[0]["artifacts"].get("result") or before[1]["artifacts"].get("result")
    assert original_a is not None
    registry.close()

    resumed = subprocess.run(
        [sys.executable, "-m", "quantsage_discovery.cli", "run", "--resume", run_id,
         "--registry", str(registry_path)],
        cwd=ROOT, text=True, capture_output=True, check=False,
    )
    assert resumed.returncode == 0, resumed.stderr + resumed.stdout
    result = json.loads(resumed.stdout)
    assert result["status"] == "SUCCEEDED"
    registry = Registry(registry_path)
    after = registry.get_run_stages(run_id)
    assert len(after) == 2
    assert {row["partition"]["item"]: row["status"] for row in after} == {
        "a": StageStatus.SUCCEEDED.value, "b": StageStatus.SUCCEEDED.value,
    }
    a_after = next(row["artifacts"]["result"] for row in after if row["partition"]["item"] == "a")
    assert a_after == original_a
    assert len(list((artifact_root / "runs" / run_id / "stages").glob("*/artifacts/*/manifest.json"))) == 2
    assert len(list((artifact_root / ".staging").glob("*"))) == 0


def test_resume_cleans_bundle_published_before_ledger_success(tmp_path):
    registry_path = tmp_path / "registry.sqlite"
    artifact_root = tmp_path / "artifacts"
    marker = tmp_path / "published.marker"
    config = {"storage": {"artifacts_uri": str(artifact_root),
                          "registry_uri": str(registry_path)}}
    code = (
        "from quantsage_discovery.core import ArtifactStore, WorkflowEngine; "
        "from tests.recovery_registry import ExitAfterPublicationRegistry; "
        "from tests.synthetic_stages import JsonStage; "
        f"cfg={config!r}; "
        f"reg=ExitAfterPublicationRegistry({str(registry_path)!r}, {str(marker)!r}); "
        "engine=WorkflowEngine(stages=[JsonStage(name='published')], config=cfg, "
        f"artifact_store=ArtifactStore({str(artifact_root)!r}), registry=reg); "
        "engine.run(run_id='post-publish-crash')"
    )
    killed = subprocess.run([sys.executable, "-c", code], cwd=ROOT, text=True,
                            capture_output=True, check=False)
    assert killed.returncode == 24, killed.stderr
    assert marker.exists()
    registry = Registry(registry_path)
    before = registry.get_stage_run("post-publish-crash", "published", {})
    assert before["status"] == StageStatus.RUNNING.value
    assert before["artifacts"] == {}
    bundle = artifact_root / "runs" / "post-publish-crash" / "stages" / before["stage_run_id"]
    assert len(list(bundle.glob("artifacts/*/manifest.json"))) == 1

    stage = JsonStage(name="published")
    engine = WorkflowEngine(stages=[stage], config=config,
                            artifact_store=ArtifactStore(artifact_root), registry=registry)
    resumed = engine.run(resume="post-publish-crash")
    assert resumed.status == "SUCCEEDED"
    after = registry.get_stage_run("post-publish-crash", "published", {})
    assert after["status"] == StageStatus.SUCCEEDED.value
    assert len(list(bundle.glob("artifacts/*/manifest.json"))) == 1
    assert stage.execute_calls == 1


def test_all_outputs_validate_before_atomic_publication_and_old_output_survives(tmp_path):
    writer = AtomicWriter()
    engine, artifacts, registry = make_engine(tmp_path, [writer], {"bad_output": False})
    first = engine.run(run_id="atomic-good")
    assert first.status == "SUCCEEDED"
    old_ref = ArtifactRef.from_dict(first.stage_results["writer"][0]["artifacts"]["good"])
    old_manifest_text = Path(old_ref.manifest_uri.removeprefix("file://")).read_text()

    failed = engine.run(run_id="atomic-bad", config={"bad_output": True})
    assert failed.status == "FAILED"
    assert stage_status(failed, "writer") == "FAILED"
    failed_row = registry.get_stage_run("atomic-bad", "writer", {})
    failed_bundle = artifacts.root / "runs" / "atomic-bad" / "stages" / failed_row["stage_run_id"]
    assert not failed_bundle.exists()
    assert failed_row["artifacts"] == {}
    assert artifacts.verify(old_ref).artifact_id == old_ref.artifact_id
    assert Path(old_ref.manifest_uri.removeprefix("file://")).read_text() == old_manifest_text


def test_output_mutation_after_registration_fails_before_publish(tmp_path):
    stage = MutatesAfterRegister()
    engine, artifacts, registry = make_engine(tmp_path, [stage])
    summary = engine.run(run_id="mutated")
    assert summary.status == "FAILED"
    row = registry.get_stage_run("mutated", "mutator", {})
    assert row["status"] == StageStatus.FAILED.value
    assert row["artifacts"] == {}
    assert not (artifacts.root / "runs" / "mutated" / "stages" / row["stage_run_id"]).exists()


def test_transient_only_retry_and_nonretryable_validation_blocking(tmp_path):
    transient = RetriesOnce()
    bad = FailsValidation()
    child = JsonStage(name="blocked_child", dependencies=("bad_validation",))
    unrelated = JsonStage(name="unrelated")
    config = {"resources": {"max_retries": 1}}
    engine, _, _ = make_engine(tmp_path, [transient, bad, child, unrelated], config)
    result = engine.run(run_id="failure-semantics")
    assert stage_status(result, "retry_once") == "SUCCEEDED"
    assert transient.attempts == 2
    assert transient.execute_calls == 1
    assert stage_status(result, "bad_validation") == "FAILED"
    assert bad.validate_calls == 1
    assert bad.execute_calls == 0
    assert stage_status(result, "blocked_child") == "BLOCKED"
    assert stage_status(result, "unrelated") == "SUCCEEDED"


def test_missing_input_fails_with_machine_readable_error_code(tmp_path):
    source = NoArtifactStage()
    consumer = MissingInputStage()
    engine, _, registry = make_engine(tmp_path, [source, consumer])
    result = engine.run(run_id="missing-input")
    assert stage_status(result, "empty_source") == "SUCCEEDED"
    assert stage_status(result, "missing_input") == "FAILED"
    row = registry.get_stage_run("missing-input", "missing_input", {})
    assert row["error"]["code"] == "MISSING_FIELD"


def test_empty_artifact_schema_and_ref_metadata_validation(tmp_path):
    stage = EmptyInput()
    engine, artifacts, _ = make_engine(tmp_path, [stage])
    result = engine.run(run_id="empty-data")
    assert result.status == "SUCCEEDED"
    ref = ArtifactRef.from_dict(result.stage_results["empty_input"][0]["artifacts"]["rows"])
    assert ref.row_count == 0
    assert json.loads(artifact_path(ref.uri).read_text()) == []
    assert artifacts.verify(ref).row_count == 0
    with pytest.raises(ArtifactValidationError, match="row_count mismatch"):
        artifacts.verify(replace(ref, row_count=1))
    with pytest.raises(ArtifactValidationError, match="stage_run_id provenance mismatch"):
        artifacts.verify(replace(ref, stage_run_id="stage_run_forged"))


def test_validate_cli_plan_run_status_artifacts_and_explain(tmp_path):
    registry_path = tmp_path / "cli.sqlite"
    artifact_root = tmp_path / "cli-artifacts"
    config = {
        "project": "cli-smoke",
        "seed": 42,
        "storage": {"artifacts_uri": str(artifact_root), "registry_uri": str(registry_path)},
        "resources": {"max_concurrent_stages": 1},
        "workflow": {"stages": [{"import": "tests.synthetic_stages:CliStage", "config_keys": []}]},
    }
    config_path = tmp_path / "cli.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    def cli(*args):
        return subprocess.run([sys.executable, "-m", "quantsage_discovery.cli", *args],
                              cwd=ROOT, text=True, capture_output=True, check=False)

    valid = cli("validate", "--config", str(config_path))
    assert valid.returncode == 0, valid.stderr
    assert json.loads(valid.stdout)["valid"] is True
    plan = cli("plan", "--config", str(config_path), "--only", "cli_stage")
    assert json.loads(plan.stdout)["selected"] == ["cli_stage"]
    bounded_plan = cli("plan", "--config", str(config_path), "--from", "cli_stage",
                       "--until", "cli_stage")
    assert json.loads(bounded_plan.stdout)["selected"] == ["cli_stage"]
    run = cli("run", "--config", str(config_path), "--only", "cli_stage")
    assert run.returncode == 0, run.stderr + run.stdout
    run_value = json.loads(run.stdout)
    run_id = run_value["run_id"]
    status = cli("status", "--run-id", run_id, "--config", str(config_path))
    assert json.loads(status.stdout)["status"] == "SUCCEEDED"
    listed = cli("artifacts", "--run-id", run_id, "--stage", "cli_stage",
                 "--config", str(config_path))
    artifact_list = json.loads(listed.stdout)
    assert len(artifact_list) == 1
    artifact_id = artifact_list[0]["artifact"]["artifact_id"]
    explain = cli("explain", "--artifact-id", artifact_id, "--config", str(config_path))
    assert explain.returncode == 0
    assert json.loads(explain.stdout)[0]["manifest"]["artifact_id"] == artifact_id
    forced = cli("run", "--config", str(config_path), "--force-stage", "cli_stage")
    assert forced.returncode == 0, forced.stderr + forced.stdout
    assert json.loads(forced.stdout)["stage_results"]["cli_stage"][0]["status"] == "SUCCEEDED"


def test_resume_rejects_changed_graph_or_code(tmp_path):
    stage = JsonStage(name="single")
    engine, _, _ = make_engine(tmp_path, [stage])
    engine.run(run_id="resume-graph")
    different, _, _ = make_engine(tmp_path / "other", [JsonStage(name="different")])
    different.registry = engine.registry
    with pytest.raises(WorkflowError, match="stage graph differs"):
        different.run(resume="resume-graph")
