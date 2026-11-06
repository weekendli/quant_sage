# Tiny onboarding acceptance input

`part-0.parquet` is a synthetic 14-row source file with the pinned 66-column
L2 Arrow schema. It is intended for bootstrap/onboard integration tests; it is
not market data and does not encode availability timestamps.

## Contents and hand-counted outcomes

- Contracts: `T2603` and `TF2603`; trading dates: 2026-01-05 and 2026-01-06.
- Valid rows include 09:29 auction matching, irregular morning updates,
  11:29:59.500 and 13:00 lunch boundaries, 15:14:59.999, and next-day rows.
- One crossed L1 row and one negative L1 size row are invalid.
- Two otherwise identical `T2603` rows share the 2026-01-05 10:01 event key;
  both must be quarantined with the identical duplicate-event reason.
- A `TF2603` row exactly at 15:15 is outside the half-open session and must be
  quarantined.
- `UNIX` is milliseconds derived from the `Asia/Shanghai` wall clock. The
  source has no availability field, so accepted output rows must retain null
  `available_time` and research-only status.

Expected reconciliation: 14 input rows = 9 accepted + 5 quarantined. The five
quarantined rows are the crossed book, negative size, two duplicate-key rows,
and the 15:15 session boundary row. All 14 raw rows must retain distinct
provenance IDs. The Parquet has four row groups with counts `[4, 4, 4, 2]`.

The independent calendar evidence confirms both dates as CFFEX trading days.
