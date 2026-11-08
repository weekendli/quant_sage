# Step 05-B Backend Lock

Step 05-B uses two fixed numerical backends. Do not replace either with an
unversioned package or a Python package that merely has the same name.

## catch22

- Package: `pycatch22==0.4.5`
- Source revision: Git tag `v0.4.5`
- Selection: canonical long names in `quantsage_discovery/research/statistical_backends.py`
- Invocation: one selected long-name function per feature window

The Python dependency is declared in `pyproject.toml`. The exact installed
Python numerical environment used for the goldens is recorded in
`tests/fixtures/statistical_backends_v1/python_backend.lock`.

## tsfeatures

- R: `4.4.3`
- Package: R `tsfeatures` `1.1.1`
- CRAN source: `tsfeatures_1.1.1.tar.gz`
- Source SHA-256: `5eeb239f51552a42900c68895712c1bbcc6a5bb82b5ae33936876b28d80d0cfc`
- conda-forge binary: `r-tsfeatures-1.1.1-r44hc72bb7e_2.conda`
- Binary SHA-256: `b6b3dcc35059b5b3d2649cefacf308bfdf4fc5909a7ac9b9165e393a8fffc48d`

The complete explicit dependency graph is
`tests/fixtures/statistical_backends_v1/r_environment.lock`. Recreate the
isolated backend without modifying system R:

```bash
conda create -y -p .r-backend --file \
  tests/fixtures/statistical_backends_v1/r_environment.lock
```

The runtime resolves `.r-backend/bin/Rscript` by default. Set
`QUANTSAGE_RSCRIPT` only to another installation that matches the lock.
The R adapter verifies both the R and `tsfeatures` package versions before
accepting results.

## Goldens

Regenerate the deterministic inputs and references from the locked backends:

```bash
/data/src/research/.venv/bin/python -m scripts.generate_statistical_goldens
```

Generated files:

- `tests/fixtures/statistical_backends_v1/inputs.json`
- `tests/fixtures/statistical_backends_v1/golden.json`

The generator records the input SHA-256 in the golden document. Numeric
tolerances are fixed at `atol=1e-12`, `rtol=1e-12`; crossing, discrete lag and
count metrics are exact.
