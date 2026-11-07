# Step 01 verification — 2026-11-02

The generic workflow engine is implemented. No downstream research algorithms were introduced. Production stage plugins are registered through `workflow.stages`; the engine's market-stage names in tests are synthetic fixtures only.

## Commands and results

Run from the repository root using `/data/src/research/.venv/bin/python`:

```bash
mkdir -p artifacts/verification
python -m pytest -q tests/test_workflow_engine.py --basetemp artifacts/verification/step01-20261102
python -m ruff check quantsage_discovery tests
```

Result: **14 passed in 1.39s**; **All checks passed!** The first attempt to preserve test evidence failed during fixture setup because the parent `artifacts/verification` directory did not exist. Creating that parent and rerunning produced the successful results above; no implementation change was needed for that setup error.

Test-created registries, manifests, payloads and logs are retained under `artifacts/verification/step01-20261102/` (ignored by Git). A separate parent-agent smoke also verified a four-node DAG: first execution succeeded, the second cached all outputs, and changing one upstream config reran only that branch and its descendants while the unrelated sibling stayed cached.

## Acceptance mapping

| Requirement | Executed evidence in tests/test_workflow_engine.py |
| --- | --- |
| Cycle / missing dependency rejection | `test_graph_topology_rejects_missing_dependency_and_cycle` |
| Stable cache, hashes and lineage | `test_success_cache_and_provenance_are_serialized` |
| Config invalidates only affected descendants; force invalidation | `test_config_change_propagates_to_descendants_only_and_force_is_scoped`; identical output content still propagates producer cache identity |
| Unscoped plugin config changes cannot silently reuse cache | `test_unscoped_plugin_hashes_entire_config` |
| Killed partition resumes exactly once | `test_partition_resume_after_real_process_exit_restores_scopes`; actual subprocess `os._exit(23)`, resume without restating scopes, unchanged completed partition |
| Crash after atomic rename before success ledger write | `test_resume_cleans_bundle_published_before_ledger_success`; actual subprocess exit |
| Failed schema never publishes a successful bundle | `test_all_outputs_validate_before_atomic_publication_and_old_output_survives` |
| Post-registration mutation rejected before publication | `test_output_mutation_after_registration_fails_before_publish` |
| Retry only transient execution failures; downstream blocking | `test_transient_only_retry_and_nonretryable_validation_blocking` |
| Missing input and machine-readable failure | `test_missing_input_fails_with_machine_readable_error_code` |
| Empty data retains schema and metadata tampering is rejected | `test_empty_artifact_schema_and_ref_metadata_validation` |
| validate / plan / run / status / artifacts / explain CLI | `test_validate_cli_plan_run_status_artifacts_and_explain`; resume CLI covered by the process-exit test |
| Empty graph and changed-graph resume behavior | `test_empty_dag_is_valid_and_explicit`; `test_resume_rejects_changed_graph_or_code` |

Artifacts carry schema/version, code and config digests, raw snapshot and split IDs, producer cache identity, input artifact IDs, partition keys and content hashes. Validators are registered before cache lookup. All output payloads are validated before the complete private stage bundle is atomically published. Successful input artifacts cannot be returned as newly produced outputs; stages must publish transaction-owned artifacts.

## Scope and limits

Execution is local, with bounded thread concurrency and file-backed artifacts. The engine streams file hashing and passes references rather than data frames; domain plugins receive resource budgets and are responsible for configuring their own DuckDB or Arrow work. This gate uses small serialized fixtures and process failures, not a large-market-data benchmark. The full onboarding performance run belongs to Step 02. Source code digests include uncommitted package and plugin source; resume requires the original code/config/graph and restores the stored selection and partition intent.

## 01-A DuckDB Registry 与嵌入 owner（2026-11-06）

Acceptance: **COMPLETE for increment 01-A only**. The historical engine and onboarding evidence above remains unchanged. Study/FeatureSet/Review and real Step 03–05 computations were not implemented or run in this increment. The next increment is 01-B.

The initial implementation was delegated to GPT-6 Luna with xhigh reasoning. That agent reached its usage limit before running tests; the root agent completed compatibility fixes, lifecycle checks and verification. No test pass is attributed to the interrupted agent.

### Changes and lifecycle

- Registry now uses DuckDB, preserving its public run/stage/status/cache/lineage behavior. New default paths use `registry.duckdb`; explicit paths are passed as supplied, and a filename suffix does not determine database format. There is no historical database import and no frozen data modification.
- Each resolved database path has one process-local owner thread and connection. Registry calls from scheduler/worker threads are queued and executed in that owner. Same-process handles to that path share it; a shared `.quantsage-owner.lock` in the registry parent directory defines workspace ownership. A second database filename in that same workspace, or a second process, receives `WorkspaceBusyError` with `code=WORKSPACE_BUSY`. Read-only CLI inspection also uses this owner model; running-time RPC queries belong to 01-G.
- `Registry.close()` and context managers release a handle; the final handle drains queued operations, closes the connection and releases the lock. CLI/onboarding close paths handle normal and error exits. The subprocess resume regression now closes the old owner before starting the next process.
- Every owner request uses explicit BEGIN/COMMIT/ROLLBACK. State changes and event writes are atomic. Sequences supply explicit stage/event ordering; sequence gaps after rollback are allowed.
- `schema_migrations` records version, checksum, applied time and DuckDB version. Schema v1 is the base DuckDB ledger; v2 adds execution scope storage. Tests build a serialized v1 fixture, preserve its records through upgrade, and inject failure after DDL/version writes to verify transactional rollback and retry. Future versions and checksum tampering are rejected.
- Cross-table references are checked explicitly inside the serialized transactions. Foreign keys to mutable stage rows are avoided because indexed status updates otherwise fail when event rows already reference them on the tested DuckDB version. The execution scope column uses a supported ADD COLUMN default; public writes always serialize a non-null object.

Use an explicit lifetime when embedding the registry:

```python
with Registry(workspace / "research.duckdb") as registry:
    # Call existing Registry APIs or pass this handle to WorkflowEngine.
    # All worker calls are marshalled to the owner thread.
    ...
```

### Executed verification

Environment: Python 3.12.3, DuckDB 1.5.5, PyArrow 25.0.1, pytest 8.4.2. No dependencies were installed or upgraded. Tests use temporary DuckDB/JSON artifact workspaces, not market data.

```bash
python -m pytest -q tests/test_registry_duckdb.py --tb=short
# 13 passed in 1.56s
python -m pytest -q --tb=short
# 136 passed in 14.87s
python -m ruff check quantsage_discovery scripts tests
# All checks passed!
```

The full suite includes existing CLI validate/plan/run/status/artifacts/explain behavior, cache/lineage, research batch behavior, real subprocess loss before completion and after artifact publication, and the onboarding regression tests. These are executable regression checks; the full frozen 79-month producer/consumer run was not repeated.

New Registry checks cover fresh/reopened files and canonical path sharing; supported schema upgrade and preserved historical fixture records; future-version/checksum rejection; rollback of both state and event; explicit ordering and latest forced attempt selection; second-process WORKSPACE_BUSY then reopen after release; 24 concurrent run/stage updates through eight calling threads; invalid parent rejection; in-flight request draining; idempotent close and abandoned-handle finalization.

Two independent GPT-6 Luna/xhigh reviews checked Standards and Spec. Their findings (mutation callbacks returning the owner connection internally, and lock scope being per file rather than per workspace) were corrected; both reviewers confirmed their findings addressed. Final checks above ran after those corrections, including different-filename workspace contention tests.

### Limits

This increment supplies local file-backed storage and ownership, not a distributed executor or background RPC service. Queue requests are serialized without a new timeout protocol. Physical schema introspection and arbitrary manual database repair are not provided. Existing frozen source/config/code evidence is preserved; old database files must not be opened as if this were a format migration. Subsequent CLI executions use new workspaces and unchanged public argument forms.

## 01-B Study / DataSource 管理（2026-11-06）

Acceptance: **COMPLETE for 01-B**. GPT-6 Luna/xhigh implementation and test agents delivered strict schemas, ResearchRepository and ResearchService. Root independently reviewed and verified the changes. Registry schema v3 adds data_sources/studies/study_revisions through the existing transactional version/checksum mechanism.

Public Python API: `ResearchService(registry).register_source(document)`, `get_source(ref)`, `create_study(document)`, `update_study(study_id, changes, expected_revision=..., actor=..., reason=...)`, `get_study(study_id, revision=None)`, `study_history(study_id)` and `ensure_active_for_execution(study_id)`. Return values are independent JSON-serializable documents. Unknown fields, mutable-version replacement and stale revisions are rejected with structured research error codes.

The generic source contract uses hashed local `{uri, content_hash}` refs for manifests, schema, calendar, sessions, access/split and optional metadata/gap/quality documents. Every data artifact is hashed in bounded streaming chunks. Metadata validates finite positive decimal ticks, canonical longest product prefixes, exclusive date ranges, overlap and actual trade-date coverage. Unknown source availability remains research_only. Study limits use `allowed_steps`, `allowed_scope` (array), and `budget` (including max_work_units/max_partitions/max_concurrent_jobs); permission bounds and observed coverage remain separate so internal source gaps can be queried.

Metadata input format is supplied at [configs/templates](../../../configs/templates/README.md). The template contains unfilled placeholders, not real metadata. User metadata path remains pending. This increment uses serialized generic fixtures; no frozen onboarding data or historical evidence was rewritten. Source documents gain immutable versions while reusing unchanged data artifacts.

Executed verification:

```bash
python -m pytest -q tests/test_research_management.py --tb=short
# 30 passed in 1.30s
python -m pytest -q --tb=short
# 166 passed in 16.04s
python -m ruff check quantsage_discovery scripts tests
# All checks passed!
python -m examples.research_management --help
```

A real subprocess invocation of `python -m examples.research_management --workspace ... --source ... --study ...` used JSON documents from `tests/research_fixtures.py` in a temporary directory. It registered the source, created Study revision 1 and preserved research_only; assertions passed. Tests cover persistence/reopen, immutable source versions, metadata-only revisions without data rewrites, malformed/tampered refs including large files, schema/time/calendar/session/access/split checks, T-only metadata rejection for TF/TS/TL, tick/date failures, known internal gaps, concurrent revision conflict/history and paused/closed execution guards. Next increment: 01-C; FeatureSet freezing and execution are not claimed here.

Read-only test-agent review identified missing research-use permission checks and weak referenced schema/split payload type checks. Root corrected these and added four regressions before the final 166-test run.

## 01-C Feature / FeatureSet 冻结（2026-11-06）

Acceptance: **COMPLETE for 01-C**. Luna/xhigh implementation and test agents delivered catalog registration and freezing; root integrated ResearchService, reviewed strict schemas and added output-identity/creation-audit fixes. Schema v4 adds providers, templates, metrics, metric sets, concrete Features, FeatureSets, draft revisions and output mappings.

The service now provides register/get provider/template/metric/metric-set, save/freeze/get FeatureSet and get/list Feature methods. Only trusted implementation keys can be registered; resources are immutable versioned documents. Draft updates check expected_revision, while freeze resolves the full explicit configuration and atomically stores concrete definitions and output mappings. Unknown refs, ambiguous multi-output inputs and cycles are rejected. Pending semantics propagate without invented values. Study backlog entries now require actual items in frozen FeatureSets.

Canonical duration spellings and numeric size representations share definitions. Feature identity excludes item aliases and FeatureSet identity, and includes input definition hashes, formula/provider/metric versions, parameters, units and time/validity rules. Config hashes retain the selection provenance. Direct statistical Feature references retain their output IDs; concrete Feature creation records and original FeatureSet creation actors are preserved.

Executed:

```bash
python -m pytest -q tests/test_feature_sets.py --tb=short
# 20 passed in 1.72s
python -m pytest -q --tb=short
# 186 passed in 17.89s
python -m ruff check quantsage_discovery scripts tests
# All checks passed!
```

Tests cover strict catalog/provider refs, documented recipe/sampling/validity fields, canonical duration/size, alias/set-version independence, changed formulas/parameters, three-metric outputs, exact input/output refs, cycles/ambiguity, pending definitions, stale draft revisions, failed freeze rollback, reopen and Study backlog bindings. These are synthetic catalog tests, not real L2/catch22 execution. Next increment: 01-D.

## 01-D Run 规划与幂等登记（2026-11-07）

Acceptance: **COMPLETE for 01-D**. Luna/xhigh implementation and test agents supplied the planner and serialized fixtures. Root integrated the service facade, reviewed scope/history behavior, completed the direct-input versus transitive-history distinction, and verified the increment. Registry schema v5 stores immutable requests/plans, environment snapshots, idempotency keys and work-level task inventory.

`ResearchService.plan_run`, `submit_run` and `get_run` now expose selection-aware planning without numerical execution. Omitted selection means all frozen items; an explicit empty list is NO_WORK. The dependency DAG records requested/dependency-only roles, exact source/split/definition refs, exchange trade dates, sessions, output ranges and bounded history. Statistical direct inputs retain W while transitive raw history includes upstream L; neither crosses sessions or authorized Study/split ranges. Missing source partitions retain diagnostic work, while absent exchange metadata is blocked. Budgets reject the entire closure before registration. Study revision/status and request-key uniqueness are checked atomically with Run/tasks/events; duplicate retries return the original Run after subsequent Study changes.

The first full regression exposed two abrupt-process-exit failures in DuckDB 1.5.5 WAL replay of schema ALTER records. Startup now checkpoints after the migration transaction commits and before dispatch. Both existing real-process crash/resume regressions pass; failed migrations still roll back. No prior frozen evidence was modified.

Executed verification in the existing Python 3.12.3 / DuckDB 1.5.5 environment:

```bash
python -m pytest -q tests/test_run_planner.py tests/test_registry_duckdb.py --tb=short
# 30 passed in 3.84s (17 planning, 13 Registry)
python -m pytest -q tests/test_workflow_engine.py::test_partition_resume_after_real_process_exit_restores_scopes tests/test_workflow_engine.py::test_resume_cleans_bundle_published_before_ledger_success tests/test_registry_duckdb.py --tb=short
# 15 passed in 2.99s after startup checkpoint fix
python -m pytest -q --tb=short
# 203 passed in 21.62s
python -m ruff check quantsage_discovery scripts tests
# All checks passed!
```

Tests cover night-session trade dates independent of UTC date, normalized identities, multi-level history, split/read authorization, missing metadata/source partitions, selection/pending behavior, whole-plan budget rejection without Run side effects, stale plan hashes, paused Studies, concurrent idempotency and persistence after reopen. All inputs are temporary serialized fixtures; real market execution, verified result reuse and publication recovery are not claimed here. Next increment: 01-E.

Review boundary: this increment has no result index yet, so cache candidates are consistently false; 01-E must populate and verify candidates once real results exist. Preflight enforces measurable whole-graph work/partition limits. CPU, memory and concurrency caps are frozen for enforcement by the 01-E executor, not represented as proven static cost estimates.
