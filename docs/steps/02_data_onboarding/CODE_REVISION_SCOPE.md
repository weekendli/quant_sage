# Step 02 code revision scope note

Date: 2026-11-04
Run: `step02-full79-20261103-r1`

The frozen code archive and the workflow registry record revision hashes at different scopes. The archive directory and `snapshot_manifest.json` use `sha256:ddb9154418f5e225e772add97f5315d803311d5855508d724f3f8eb2a6896da8`. This matches the registry's `onboard` stage revision and the `code_revision` embedded in `onboarding.json`. The registry's run-level revision is `sha256:134977720b12e3fabf7c6ca3e95b0e8a73cdf62461f6b195f47c7ff2621e2666`; it covers the workflow package and both stage plugins. The registry's bootstrap-stage revision is `sha256:62a5069ce4732b4aae14437b39d2efc08e99e04e40ee96ff8486a71a8d345811`.

The onboarding stage revision is derived from the run-level revision and the `OnboardStage` source bytes. Recomputing the run-level revision from the archived `BootstrapStage` and `OnboardStage` classes returns `sha256:134977720b12e3fabf7c6ca3e95b0e8a73cdf62461f6b195f47c7ff2621e2666`. Recomputing the onboarding-stage revision from that run hash and the archived `onboard.py` bytes returns `sha256:ddb9154418f5e225e772add97f5315d803311d5855508d724f3f8eb2a6896da8`. The archived modules used for this check resolve under the archive's `source/` directory.

The existing archive label and frozen `snapshot_manifest.json` are preserved as written. Use the run-level hash when identifying the full workflow revision and the `ddb915…` hash when identifying the onboard-stage revision. This note records the scope distinction; it does not rewrite or replace the archived evidence.

## Post-run payload identity correction

After the full run, payload comparison was corrected so `product` participates alongside the 62 L2 business fields. The event key remains `(exchange, instrument_id, trade_date, event_time)`; the three provenance fields remain excluded from payload comparison. This change was not used for `step02-full79-20261103-r1`, does not alter its frozen artifacts or archive, and has not been applied by rerunning production.

The current working-tree run-level code revision is `sha256:8730b89606a6f07b4845c05ede1bcc3eb345780c01fa1ca649d4ee5100bc71f1`; its current onboard-stage revision is `sha256:807f3d1d19ed9bac55278f8f87982bff2931918fa908988629e89e4d162d8dcf`. The frozen full-run hashes above remain `134977…` and `ddb915…`, respectively. The independent 79-file baseline compared product plus all 62 business fields and found zero conflicting duplicate groups, so adding product to the producer fingerprint is expected not to change this input's duplicate classification. That baseline is independent evidence, not a rerun under the corrected code.

The serialized regression changes only `product` on one member of an event-key duplicate pair, then checks that both rows are quarantined as a conflicting payload group and that the published fixture passes the consumer verifier. The focused stage suite passed (`7 passed`), and Ruff passed for the changed producer and test files. Commands:

```bash
python -m pytest -q tests/test_onboarding_stage.py --basetemp artifacts/verification/product-payload-stage-20261104
python -m ruff check quantsage_discovery/stages/onboard.py tests/test_onboarding_stage.py
```

## Recompute from archived source

Run from any working directory. `-B` prevents Python from writing bytecode into the frozen archive.

```bash
python -B - <<'PY'
import os
import sys
from pathlib import Path

archive = Path(
    "/data/src/quant_sage/artifacts/step02/code_snapshots/"
    "snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2/"
    "ddb9154418f5e225e772add97f5315d803311d5855508d724f3f8eb2a6896da8/source"
).resolve()
sys.path.insert(0, str(archive))
os.chdir("/tmp")

from quantsage_discovery.core.workflow import source_code_revision
from quantsage_discovery.stages.bootstrap import BootstrapStage
from quantsage_discovery.stages.onboard import OnboardStage

print(source_code_revision([BootstrapStage(), OnboardStage()]))
PY
```

Expected output:

```text
sha256:134977720b12e3fabf7c6ca3e95b0e8a73cdf62461f6b195f47c7ff2621e2666
```

The stage-revision calculation follows [`WorkflowEngine._stage_code_revision`](../../../quantsage_discovery/core/workflow.py): SHA-256 over the run-level revision string followed by the selected stage source bytes. For `OnboardStage`, the source is `source/quantsage_discovery/stages/onboard.py` in the archive.

To verify that stage-scoped value directly:

```bash
python -B - <<'PY'
import hashlib
from pathlib import Path

run_revision = "sha256:134977720b12e3fabf7c6ca3e95b0e8a73cdf62461f6b195f47c7ff2621e2666"
stage_source = Path(
    "/data/src/quant_sage/artifacts/step02/code_snapshots/"
    "snapshot_196d8ba3203cebdc50f94420a373638c4b0db6f98f4d7b2fe87b3a0220465fc2/"
    "ddb9154418f5e225e772add97f5315d803311d5855508d724f3f8eb2a6896da8/"
    "source/quantsage_discovery/stages/onboard.py"
)
digest = hashlib.sha256()
digest.update(run_revision.encode("utf-8"))
digest.update(stage_source.read_bytes())
print("sha256:" + digest.hexdigest())
PY
```

Expected output:

```text
sha256:ddb9154418f5e225e772add97f5315d803311d5855508d724f3f8eb2a6896da8
```
