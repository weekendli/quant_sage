# QuantSage Discovery Engine

QuantSage is a Python framework for reproducible empirical research over irregular market data. The repository implements the generic Step 01 workflow engine, the CFFEX government-bond-futures L2 data onboarding stage (Step 02), and the 02-A frozen-source/metadata/gap adapter. Step 02 completed full 79-month onboarding and independent artifact verification; the adapted `cffex_treasury_l2_full79@2` DataSource now exposes bounded refs, session intervals and traceable gap evidence. Step 03 and later computations remain unimplemented. See the [Step 02 results](docs/steps/02_data_onboarding/RESULTS.md) for frozen entries, verification, adapted-source hashes, counts, commands and research limits.

## Install and verify

Use an isolated project environment so the shared machine environment stays untouched:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
python -m pytest
```

The command line is available as `quantsage` after installation, or as `python -m quantsage_discovery.cli`.

## Implemented onboarding

The onboarding stages read the local Parquet catalog using a frozen source inventory, normalize and validate records, quarantine invalid rows, and publish a single `onboarding.json` entry with artifact manifests. The raw data is machine-local and is not stored in this repository. The completed production run and its independent consumer audit are recorded in [Step 02 results](docs/steps/02_data_onboarding/RESULTS.md); the baseline template is [configs/full_79months.yaml](configs/full_79months.yaml). The run's resolved configuration and output artifacts are retained under the ignored `artifacts/step02/` directory.

The completed run ID is `step02-full79-20261103-r1`. Do not rerun it with the same run ID; use the linked results and published artifacts for inspection. This repository stops at Step 02, so no statistical discovery or hypothesis-testing stage has been executed.

## CLI

The generic explicit research-batch increment can now be exercised with isolated arithmetic data:

```bash
python -m tests.research_demo
```

It runs a small batch, expands dates, then adds a feature, printing fresh/reused work counts and saving configs/plans/reports under a new `/tmp` directory. This does not implement L2/catch22 or scientific effect validation. See [batch increment results and provider API](docs/steps/01_workflow_engine/RESEARCH_BATCH_RESULTS.md) and the [target domain model](docs/DOMAIN_MODEL.md).

The supplied example validates with an empty stage graph. The engine reports that no work will run; it does not invent production stages.

```bash
quantsage validate --config docs/examples/quant_discovery.yaml
quantsage plan --config docs/examples/quant_discovery.yaml
quantsage run --config docs/examples/quant_discovery.yaml
```

Configure Python stage plugins under `workflow.stages` to execute work. A plugin entry can set cache-scope paths explicitly:

```yaml
workflow:
  stages:
    - import: my_research.onboard:OnboardStage
      config_keys:
        - input
        - market.calendar_id
      partitioned: false
```

With plugins registered, inspect the graph, run or resume it, inspect its stage states and list or explain artifacts:

```bash
quantsage plan --config workflow.yaml --from onboard --until atlas
quantsage run --config workflow.yaml --partition-scope trade_date=2026-01-05,instrument_id=TF2603
quantsage run --resume run_0123456789abcdef
quantsage run --config workflow.yaml --force-stage onboard
quantsage status --run-id run_0123456789abcdef --config workflow.yaml
quantsage artifacts --run-id run_0123456789abcdef --stage onboard
quantsage explain --artifact-id art_<sha256>
```

`--only stage_name` runs that stage only when its dependencies have valid cached artifacts. `--from` and `--until` include their named boundaries; dependencies outside the selected range must be available from cache. A resumed run restores its original partitions and selection when those options are omitted.

## Stage extension API

Stages are ordinary Python objects. They validate their inputs, write outputs through the per-stage transaction, and return artifact references. Register validators in `register_validators` so cache hits receive the same schema and content checks as new output. The transaction validates every returned output before atomically publishing the complete stage bundle.

```python
from pathlib import Path

from quantsage_discovery.core import ArtifactStore, RunContext, StageResult


class ExampleStage:
    name = "example"
    version = "1.0"
    dependencies = ()
    config_keys = ("example.threshold",)
    partitioned = True

    def register_validators(self, store: ArtifactStore) -> None:
        def check_json(path: Path) -> None:
            import json

            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, dict) or "rows" not in value:
                raise ValueError("expected a JSON object with rows")

        store.register_validator("example_rows", "1.0", check_json)

    def validate(self, ctx: RunContext, inputs) -> None:
        if ctx.partition_keys.get("trade_date") is None:
            raise ValueError("trade_date partition is required")

    def execute(self, ctx: RunContext, inputs) -> StageResult:
        ref = ctx.artifact_transaction.write_json(
            "rows",
            {"rows": []},
            artifact_type="example_rows",
            schema_version="1.0",
            row_count=0,
        )
        return StageResult(artifacts={"rows": ref}, metrics={"rows": 0})
```

Stages can stream large files directly into the transaction with `reserve_output(alias, relative_path=...)`, then call `register_output(alias)` after the writer closes. `final_ref(ref)` returns the post-commit URI for embedding in a sibling manifest. Returning a prior input ref as a new output is rejected; create a new transaction-owned artifact to preserve stage provenance.

`RunContext` exposes the full validated config, scoped `stage_config`, partition keys, the fixed seed, artifact store and transaction, structured logger, snapshot/split/as-of metadata and resource limits. A stage can declare `config_keys` as dotted paths; omitted keys mean the full normalized config participates in its cache key. A changed cache identity flows to descendants through each produced artifact's cache provenance.

All payloads require a validator registered for `(artifact_type, schema_version)`. `ArtifactStore.register_json_schema` supports common structural checks for JSON; Parquet and domain-specific validators should inspect physical schemas and required contract metadata with the relevant library.
