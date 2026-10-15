# DECISIONS.md

1. **Indexes.** On `employees`: unique `emp_code` (identity, and makes duplicate creates fail cleanly),
`(department, emp_code)` for the filtered list and the trend, and `joined_on` / `(department, joined_on)` for the summary's
headcount match. On `attendance_logs`: unique `(emp_code, date)` (one record per day, also serves the monthly report),
`(date desc, emp_code)` for the default list sort and the leaderboard's month range, `(emp_code, punch_in desc)` for
punch-out's "latest record", and `(status, date, emp_code)` for the status filter. I rejected `(date, late_minutes)`:
the month range already narrows things enough.

2. **Punch-in race.** There's no pre-check. Both requests build the document and call `insert_one`. The unique
`(emp_code, date)` index lets exactly one in; the other gets `DuplicateKeyError`, which I return as 409.

3. **Ties.** `$setWindowFields` with `$rank` gives tied employees the same rank, and I filter `rank <= limit` afterwards.
So both tied people come back, even if that means `limit + 1` rows, ordered by `emp_code`.

4. **Headcount.** The pipeline starts from `employees` (joined by month end), looks up that month's logs and unwinds with
`preserveNullAndEmptyArrays`, so someone with no logs still produces one row and gets counted.

5. **100x data.** Keep pre-aggregated daily totals per employee (updated on write or nightly) so analytics read small
summary documents instead of raw logs.