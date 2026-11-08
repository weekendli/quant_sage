# Step 13 Results — End-to-End Integration

## E2E A/B/C ready-subset closure — 2026-11-07

**Status: COMPLETE for the ready Step 02–05 subset. Full Step 13 remains PARTIAL because events, regimes, phenomena, hypotheses, cubes, falsification and planner stages are outside the current execution-spec scope; real user market metadata and F07/F08 aggregation semantics are also still unavailable.**

### Source and scope

| Object | Value |
|---|---|
| DataSource | `cffex_treasury_l2_full79@2` |
| Availability | `research_only`; `available_time` unavailable |
| Market metadata | null; no user tick/unit metadata was substituted |
| Gap manifest | `sha256:1f9f7105ab2ab0e807f55d40d0dc65487af6a360970e4c9295cf985c02cb8333` |
| Instrument / trade date | `T2603` / `2026-01-05` |
| Sessions | continuous morning `01:30–03:30 UTC`; continuous afternoon `05:00–07:15 UTC` |
| Frozen split | `split_b5997676e0f567043a6fa8043c5b0de41a2d7c9eef1f50084775a7bfb038d71b` |

### A/B/C objects

| Object | Value |
|---|---|
| Study | `real_abc_study`; final revision 3 in the captured report run |
| FeatureSet A/B | `real_abc_candidates@1` |
| FeatureSet C | `real_abc_candidates@2`; parent `real_abc_candidates@1` |
| New C definition | `feature_9fcd52b0a338f6190b55d61c@1` (`tsf.spectral_entropy`) |
| Run A | `run_6c28e5dea706479ba2efea1a611e9e8b` (`COMPLETE`) |
| Run B | `run_632117f4b057478287818a90fd9aaf20` (`COMPLETE`) |
| Run C | `run_546aa84b5a024f27bd527340b4b62f73` (`COMPLETE`) |
| Pending check | `run_33d0aeecd0e64a6d98d61f194fbc86f2` (`BLOCKED`, `PENDING_SEMANTICS`) |
| Review A | `e2e-real-20260105-v2-review-a` |

The finite backlog includes three ready business candidates
(`obi_30s`, `wobi_1m`, `coverage_5m`) plus pending
`fill_dist_asym_1m`. A selects only the first two business candidates and two
statistics: `obi_mean_1m` and `wobi_catch22_f1ecac_5m`. The unselected
coverage and pending candidates remain queryable as `NOT_SELECTED`.

### Results

- A completed 6 work units and 4 requested outputs, with 1 cached primitive dependency. The output refs are `result_145d7ca5b964a5798bb030548f1b1e81` (`obi_30s`), `result_ee20060d0a1d8648071a55f456f62574` (`wobi_1m`), `result_4ff6601011afbdc87f4d8c8e8acf1b4c` (`obi_mean_1m`) and `result_5457d7f15fa4774448d7fe40a12f3211` (`wobi_catch22_f1ecac_5m`).
- A coverage reports 6 targets, 4 available results, 2 unprocessed/unselected intervals and no source gaps. The ready business and statistical result refs were inspected through the result index with artifact verification.
- Review A cites serialized results, records actor/scope/limitations and selects `investigate`; it does not alter Run status or permissions.
- B expands the same frozen FeatureSet to the afternoon session. It reuses all four A requested outputs, computes six missing works and skips two dependency-only works already satisfied by verified parents. State counts: `CACHED=4`, `SUCCEEDED=6`, `SKIPPED=2`; result links include all A requested refs.
- C keeps the same scope, adds exactly one `tsf.spectral_entropy` definition and computes two new session partitions in the primary run. State counts: `CACHED=8`, `SUCCEEDED=2`, `SKIPPED=4`; all unchanged B requested outputs are reused.
- A separate cached replay chain (`run_d6cab995e8ba409d9117209c008eb544` → `run_700db8aff6894327b2c9d86e955e3f3c` → `run_1c88a79ba6ca4fc1b1bb76f566a7e355`) verified long-lived reuse. B’s eight requested outputs were all cached, and B/C comparison reported one added definition on the right and none on the left.
- Explicit empty selection returns `NO_WORK` with zero work units. Selecting pending F07 `fill_dist_asym_1m` returns `BLOCKED` with `PENDING_SEMANTICS`; selecting it is not used to block unrelated ready work.

### Fault and recovery matrix

Focused temporary-workspace tests passed (9/9):

- pre-publication failure retains attempt history and resumes only failed work;
- pre-publication crash returns RUNNING work to recoverable state;
- duplicate receipt recovery publishes one result;
- database-commit crash is idempotent and preserves the producer;
- invalid receipts are quarantined and never adopted;
- missing artifacts become repair work and retry without overwriting history;
- owner `SIGKILL` after a receipt recovers the same attempt/result;
- empty selection is `NO_WORK`, while all-NA and zero-row outputs remain distinct valid execution outcomes.

These failures were injected only into temporary test workspaces. No frozen source or production registry artifact was damaged.

### Captured report

The machine-readable acceptance summary is:
`artifacts/research/reports/real_e2e_abc_20260105.json`

- SHA-256: `07336b2b8312079662b6f33fe168e1ba31267d996e5db0f8a680bef83c71c1a4`
- Size: 22,047 bytes
- Captures the clean replay A/B/C, B/C comparison, Review, coverage summary, pending Run and `NO_WORK` plan.

### Commands actually run

```bash
/data/src/research/.venv/bin/python -m examples.research_end_to_end \
  --database artifacts/research/registry.duckdb \
  --socket /tmp/quantsage-1000/registry-08ab336dc96daf95.sock \
  --run-prefix e2e-real-20260105-v2 \
  --report artifacts/research/reports/real_e2e_abc_20260105.json

/data/src/research/.venv/bin/python -m pytest -q \
  tests/test_research_execution.py::test_prepublication_failure_retries_only_failed_work_and_preserves_attempt_history \
  tests/test_research_execution.py::test_prepublication_crash_reconciles_running_attempt_and_resumes_only_unfinished_work \
  tests/test_research_execution.py::test_published_receipt_recovers_once_even_if_the_same_receipt_is_discovered_twice \
  tests/test_research_execution.py::test_database_commit_crash_is_idempotent_and_does_not_create_a_new_producer \
  tests/test_research_execution.py::test_invalid_receipt_is_quarantined_and_never_adopted \
  tests/test_research_execution.py::test_cache_reuses_actual_producer_and_missing_file_retries_with_new_attempt \
  tests/test_research_owner_rpc.py::test_sigkill_after_published_receipt_recovers_same_attempt_and_result \
  tests/test_research_execution.py::test_empty_selection_is_no_work_but_zero_rows_and_all_na_are_published_results
```

The real workflow command completed in approximately 26 seconds including the cached replay; the focused fault matrix passed in 5.02 seconds. These are small-scope local measurements, not full-history performance claims.

### Remaining external and scope limitations

- User-provided market tick/unit metadata remains absent. No real tick-dependent primitive/business result is claimed.
- F07/F08 business aggregation remains `pending_semantics`; only direct snapshot F07/F08 primitives are available.
- The current execution-spec E2E target is the ready Step 02–05 subset. It does not claim the later events/regimes/phenomena/hypothesis/cube/falsification/planner stages from the full Step 13 roadmap.
- Availability remains `research_only`; descriptive results do not establish causal effects or future predictive value.

## 2026-11-08 remediation replay

After the F1/F2/F3/T9 fixes, the isolated real A→Review→B→C path was rerun against a new catalog namespace and workspace:

| Stage | Run | Result |
|---|---|---|
| A | `run_979612f3321f4f7da50f0eb4aedcb9c6` | COMPLETE, 6 works |
| Review | `remediation-20261108-review-a` | `investigate`, evidence-bound |
| B | `run_2a73644414724e1188ebc02e46bd9060` | COMPLETE; `CACHED=4`, `SUCCEEDED=6`, `SKIPPED=2` |
| C | `run_926c31dd06f840ec9dfdb8b1482d6b7a` | COMPLETE; one added definition; `CACHED=8`, `SUCCEEDED=2`, `SKIPPED=4` |
| Pending F07 | `run_afaef3f6d2144c4d8ff06d3f2dc08081` | BLOCKED / `PENDING_SEMANTICS` |

Explicit empty selection remained `NO_WORK` with zero work units. Full pytest: 344 passed. See [remediation verification](../../acceptance/2026-11-08/REMEDIATION_VERIFICATION.md).
