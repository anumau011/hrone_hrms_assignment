# REVIEW.md

These are the problems I found in the starter `app/main.py`. Line numbers are from the original starter file, not my
version. Most of them I caught by reading the code next to openapi.yaml; a few I only noticed after writing small
tests (the late-minutes one and the rounding one especially).

| # | Where | What's wrong | How I noticed / how to reproduce | Fix |
|---|---|---|---|---|
| 1 | `MongoClient`, L22 | Client is created without `tz_aware=True`, so every datetime coming back from Mongo is naive. | Overtime calc subtracts a naive punch_out from an IST-aware datetime and blows up with `TypeError` (500). | Added `tz_aware=True`. Everything internally is now aware UTC. |
| 2 | top of file | No indexes at all, not even the unique ones. | explain on the attendance list shows COLLSCAN. Also nothing stops duplicate employees or two records for the same day. | `ensure_indexes()` runs at startup (and again from /health if Mongo wasn't up yet). Unique on `emp_code`, unique on `(emp_code, date)`, plus the ones the list/analytics queries need. |
| 3 | `compute_late_minutes`, L36-37 | Grace check is done on the already-floored minutes (`minutes > 10`). | Shift 09:30, punch 09:40:01 -> int(10.01) = 10 -> `10 > 10` is false -> 0. R2's own example says this should be 10. | Check seconds instead: late if `secs > 600`, then `secs // 60`. |
| 4 | `compute_late_minutes`, L35 | `punch_in.replace(hour=9, minute=30)` applies the IST shift time to whatever clock punch_in is in. | On a UTC machine, sample #2 (10:05 IST = 04:35 UTC) comes out as 0 late instead of 35. | Shift start is built as an actual IST datetime on the attendance date. |
| 5 | `compute_late_minutes`, L35 | Uses the punch's own calendar day, which is wrong for night shifts. | 22:00-06:00 shift, punch at 00:30 IST. Starter compares to 22:00 the same day, gets a negative number, returns 0. Should be 150 since the shift started the night before. | Compare against the shift start on the attendance date from R1. |
| 6 | `compute_work_hours`, L41 | Uses Python `round()`, which isn't half-up. | 2h 0m 18s = 2.005 h. `round(2.005, 2)` gives 2.0, R4 wants 2.01. | Decimal arithmetic with `ROUND_HALF_UP`. |
| 7 | helpers / punch-in | Times are never truncated to whole seconds (R1). | 09:40:00.900 should count as 09:40:00 (on time). Starter treats it as just over 10 min. The milliseconds also get stored. | Truncate to seconds as soon as the timestamp comes in; store and return the truncated value. |
| 8 | `compute_overtime`, L47 | The 30-minute minimum from R3 is missing. | Sample #1, out at 18:35 -> starter says 5 min overtime, should be 0. | Return 0 unless it's at least 30. |
| 9 | `compute_overtime`, L46 | Shift end is put on the start date even for overnight shifts. | Sample #7 (22:00-06:00, out 06:40 next morning) gives roughly 1120 min instead of 40. | If `shift_end <= shift_start`, shift end is on the next day. |
| 10 | `EmployeeIn`, L53-60 | No validation on any field. | `"9:30"`, `"25:00"`, bad emails, `joined_on: "2026-02-30"` all go through. A bad shift time then crashes punch-in later. Same start/end shift is also accepted. | Added the patterns/lengths from the contract, a real date check, and a check that start != end. All give 422. |
| 11 | `PunchInIn.punched_at`, L65 | Plain `int`, so pydantic happily coerces. | `"1783312500000"` (string), `1783312500000.0` (float), `true`, and `1783312500` (seconds, ends up in 1970) are all accepted. | Strict int with the min/max from the contract. |
| 12 | `PunchInIn.status`, L66 | Any string allowed. | Can punch in with `"LEAVE"` or `"FOO"`. | Literal of PRESENT / WFH / ON_DUTY. |
| 13 | `punch_in`, L102 | `if body.punched_at` treats 0 as "not given". | `punched_at: 0` silently becomes now. | Use `is not None` (and 0 fails validation anyway now). |
| 14 | `health`, L72 | Always returns ok, never checks Mongo. | Stop Mongo: /health still says 200 and the next real request 500s. | Ping Mongo; 503 if it fails. |
| 15 | `create_employee`, L79-83 | find_one then insert, so it races. | Fire two creates with the same emp_code in parallel and both get 201. | Removed the pre-check, rely on the unique index, catch `DuplicateKeyError` -> 409. |
| 16 | `create_employee`, L82 | `datetime.now()` is naive local time and gets stored as if it were UTC. Response also returns a datetime, not ms. | On a machine set to IST, created_at ends up 5h30m in the future. | UTC-aware time, response converted to epoch ms. |
| 17 | `list_employees`, L93 | `skip = page * page_size` | page=1 skips the first 20 people. With 25 employees you get 5 back. | `(page - 1) * page_size` |
| 18 | `list_employees`, L94 | total uses `count_documents({})`, ignores the department filter. | `?department=Sales` still reports the total for everyone. | Count with the same filter. |
| 19 | `list_employees` | No sort and no limits on page / page_size; created_at not converted. | Order changes between calls. `page_size=0` returns everything (limit 0 = no limit), negative page -> negative skip -> 500, `page_size=1000` accepted. | Sort by emp_code, `page >= 1`, `page_size` 1-100, ms conversion. |
| 20 | `punch_in`, L101/113 | Doesn't check the employee exists. | Unknown emp_code -> `'NoneType' object is not subscriptable` -> 500 instead of 404. | 404 if not found. |
| 21 | `punch_in`, L102-103 | `fromtimestamp()` / `now()` use the server's timezone, so the date isn't the IST date, and night shifts aren't handled. | On a UTC server a 02:00 IST punch lands on the previous day. 00:30 punch for a night shift also gets the wrong date. | Convert from ms as UTC, then work out the date in IST following R1 (including the overnight rule). |
| 22 | `punch_in`, L104-118 | Check-then-insert again. | Two identical punch-ins at once -> two records for one day, both 201. | Unique `(emp_code, date)` index, insert directly, duplicate key -> 409. |
| 23 | `punch_in`, L119-121 | Returns an `id` field and the raw datetime. | Response has `id` (contract says no ids) and an ISO string for punch_in. | One formatter that outputs exactly the contract fields with ms timestamps. |
| 24 | `list_attendance`, L144-147 | Pulls every matching doc into memory and sorts/pages in Python. | Fine on 9 docs, slow on 100k, and explain shows a full scan. | Sort/skip/limit in Mongo with an index, `count_documents` for total. |
| 25 | `list_attendance`, L145 | Only sorts by date, no emp_code tie-break. | Same-date rows come back in random order, so paging can repeat or skip rows. | Sort date desc, emp_code asc. |
| 26 | `list_attendance`, L148-149 | Leaks `_id` as `id`, timestamps not converted (including inside history), old records without `half_day`/`history` returned as-is. | Sample #9 comes back without half_day and history. | Same formatter as #23: ms everywhere, missing half_day -> false, missing history -> []. |
| 27 | `list_attendance` params | Nothing is validated. | `date_from` after `date_to` returns 200 with nothing (should be 422). `2026-7-1` breaks the string compare. `status=FOO` accepted. `page=0` gives an empty page. | Proper date parsing, from > to -> 422, status enum, page limits. |

## Things I checked and left alone

- `load_dotenv()` - doesn't override env vars that are already set, so real environment wins like the spec asks. Fine.
- The IST offset (+05:30) is right, and India has no DST so a fixed offset is ok.
- Comparing dates as `YYYY-MM-DD` strings works because the format sorts the same as the dates do. Only an issue if
  the input isn't validated (that's #27).
- `doc.pop("_id")` in create_employee looked odd at first but it's needed, insert_one adds `_id` to the dict.
- Endpoints are plain `def`, not `async`. That's ok - FastAPI runs them in a threadpool and pymongo is thread safe.
- Punching in after the shift has ended (say 19:00 on a 09:30-18:30 shift) is accepted. You get a big late_minutes
  and, after punch-out, possibly overtime too, because R3 only looks at shift_end. Looks strange but it's what R2/R3
  say and there's no error defined for it, so I kept it. Would ask the business about it in a real project.
- In punch-out I deliberately don't rewrite `late_minutes`. It's set at punch-in and only a correction (PATCH) should
  change it, so punch-out only touches work_hours, overtime_minutes and half_day.