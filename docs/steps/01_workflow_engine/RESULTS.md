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
