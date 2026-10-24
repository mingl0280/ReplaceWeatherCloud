# Server review, 2026-10-24

The server source was downloaded before review and committed as `1e5399c`.
The existing local Git history was retained. The pre-import local files and
downloaded server archive are preserved in `.local-backups/` (not in Git).

## Persistent write failure

`main.py` imported `db_conn` directly. `check_db()` replaced the connection in
`scripts.db_ops`, but the write and latest endpoints retained the original
object. Once closed, every subsequent insert used that same closed connection.
A process restart cleared the stale reference; a machine reboot was unnecessary.
Shared non-autocommit read transactions also allowed a SQL error to leave the
connection in an aborted transaction state.

Each operation now owns and closes its connection. Writes use explicit
transactions, reads use autocommit, and connection, statement, and lock waits
are bounded. Blocking database calls run in worker threads. Errors return 503,
and application error messages omit database credentials. The five-second
duplicate cache is bounded and only records successful responses.

## Other fixes

- Failed inserts are durably saved to individual JSON files and replayed using
  a transactional recovery receipt. See `recovery.md` for operational limits.
- Empty and short chart datasets work; the moving average covers every row
  with endpoint padding and does not overwrite the original data.
- Optional wind periods no longer compare `None` with integers. Invalid time
  ranges return validation errors; the existing 48-hour range cap remains.
- Detail charts start with a valid one-hour range. Empty barometer results
  display an empty summary rather than throwing a JavaScript exception.
- Configuration and static resources resolve relative to the project.
  Configuration is safely parsed and validated. PyYAML is declared explicitly.
- The broken legacy startup wrapper is repaired. Database setup is guarded,
  commits on success, rolls back on failure, and has no hardcoded role grants.

## Validation

38 unit and HTTP regression tests passed locally on Python 3.13 and on the
server's Python 3.10 environment. Six real PostgreSQL tests passed in an isolated
schema, covering refused connections, SQL/constraint errors, recovery files,
receipt rollback, and an archival failure after a successful replay commit.
The temporary schema was removed afterward.

## Deployment verification

The reviewed source was deployed to `/var/www/fakeweather`, and only
`weather.service` was restarted. The original source backup is
`/var/backups/fakeweather/source-before-20261024-review.tar.gz`.

The Browser verified the dashboard, history charts, native date controls, and
an empty historical range without console errors. A tagged browser GET write
created PostgreSQL row `1310464` at `2026-10-24 08:58:41` server local time.
The exact tagged test row was removed after verification. Real station rows
continued arriving afterward. All current-data endpoints returned HTTP 200;
empty historical endpoints returned empty arrays; malformed timestamps returned
HTTP 422. No application errors appeared in the new service invocation.

The wire protocol remains unchanged. Its `wid`, `key`, `date`, and `time`
parameters retain their existing behavior; this update does not introduce
station authentication or reinterpret timestamps. Existing proxy/access logs
may include station query parameters, so restrict log access and retention.
The replay mechanism cannot establish exactly-once ingestion when an original
live commit loses its acknowledgement or the station submits a new retry.
