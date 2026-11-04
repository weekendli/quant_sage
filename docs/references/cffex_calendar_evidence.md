# CFFEX trading-date and session evidence

Verified 2026-11-02 for the complete L2 inventory scope: all 79 monthly files from `year=2020/month=01` through `year=2026/month=07`. Machine-readable closure intervals, source references, session schedules and comparison results are in [`configs/reference/cffex_calendar_evidence.json`](../../configs/reference/cffex_calendar_evidence.json).

## Trading-date calendar

The active Marketdata pointer selects `db-restructure-full-v16`; its `reference.calendars` relation contains only `CN_STOCK`. The catalog-selected file is `canonical/reference/calendars/v3/layout=l1/part-3024-001.parquet` (SHA-256 `840bad7d23af58e3419ec3f0f6cf4f873c188de3eac4178c93853ee6e6ddb4cd`). The sibling `part-2024-001.parquet` has the same SHA-256, so a directory wildcard doubles the 4,055 calendar rows.

I compared the complete `CN_STOCK` date set in **2020-01-02 through 2026-07-31** with weekdays after applying each year's CFFEX holiday notice and the 2020 COVID amendment. The sets match exactly: 1,594 expected dates and 1,594 catalog dates, with no missing or unexpected keys. This verifies `CN_STOCK` as a trading-date membership source for this bounded CFFEX period; it does not change its provider ID or establish applicability outside the compared range.

I also scanned the `TRADINGDATE` column in all 79 source files. The 1,587 distinct dates span 2020-01-02 through 2026-07-31; every source date is present in `CN_STOCK`. Seven catalog dates have no source rows: 2021-01-08, 2021-01-11, 2021-04-07, 2021-05-06, 2021-05-07, 2021-06-03 and 2021-06-17. That is source coverage, not a calendar mismatch. This was a date-membership scan, not a row-level quality audit.

Annual CFFEX-authored notice copies and their original document IDs / mirror provenance are listed in the JSON fixture. The comparison includes specific exception cases: the 2020-01-27 notice extends the Spring Festival closure through Sunday 2020-02-02, making Friday 2020-01-31 closed and Monday 2020-02-03 open; CFFEX's 2024 notice closes 2024-02-09 through 2024-02-17 while 2024-02-08 and 2024-02-19 are trading dates; and CFFEX's 2026 notice closes 2026-01-01 through 2026-01-03 and 2026-02-15 through 2026-02-23, reopening 2026-01-05 and 2026-02-24 respectively.

## Intraday session rules

The primary rules are CFFEX contract-rule PDFs, with rule revisions dated 2020-06-12 and CFFEX's rule index stating the T, TF and TS revisions took effect on 2020-07-20. Older T / TF / TS rules used a 09:10–09:15 opening call auction and continuous trading at 09:15–11:30 and 13:00–15:15. From 2020-07-20, the auction moved to 09:25–09:30 and continuous trading to 09:30–11:30 and 13:00–15:15. On the last trading day, continuous trading is morning-only under both rule versions.

The rules divide the opening auction into order submission and matching: 09:10–09:14 / 09:14–09:15 before the change, and 09:25–09:29 / 09:29–09:30 after it. Treat those as auction phases, separate from continuous sessions. For implementation, the fixture represents clock intervals as half-open `[start, end)` intervals; the PDFs give clock times but do not specify software boundary inclusivity. Session times are exchange-local; mapping them to `Asia/Shanghai` is supported by the sample's independent `TRADINGTIME` / `UNIX` offset check, rather than a timezone field in the rule PDFs.

The last trading day is the second Friday of the contract month; if that date is a statutory holiday or otherwise does not trade, the rule moves it to the next trading day. The product-specific session is therefore AM-only for the actual last trading date, including a shifted date. TL first listed on 2023-04-21 and its issuer-authored rule PDF specifies the same post-change auction and continuous hours and morning-only last trading day.

For classification, 2026 sample rows at 09:29 fall inside the 09:29–09:30 auction-matching phase, not continuous trading. A row at 15:24 is after the CFFEX 15:15 close and has no evidenced post-close session; quarantine it as out-of-session unless the upstream publisher supplies a separately verified timestamp/publication rule. The 2026 sample's first timestamp around 09:29 and maximum around 15:24 must not be used to infer exchange sessions.

## Use in Step 02

Use the catalog-resolved `CN_STOCK` snapshot only with this bounded comparison evidence attached to the onboarding provenance. Freeze the exact selected file, hash and release. Keep session rules as a separate versioned artifact; the date list cannot assign `session_id`. For this dataset range, CFFEX trading-date membership and the product sessions have independent evidence. Re-run the comparison if the source date range or Marketdata release changes.
