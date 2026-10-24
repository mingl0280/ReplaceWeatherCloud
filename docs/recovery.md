# Failed weather record recovery

Failed inserts are saved as one JSON file per normalized weather observation in
`var/failed-records/`, relative to the application directory, or in the directory
specified by `FAKEWEATHER_FAILED_DIR`. The original server observation timestamp
and scaled values are preserved. Station credentials and database exception
messages are not stored. On Linux directories and files are created with modes
0700 and 0600. Run recovery as the service account or an administrator.

Files are written to a temporary file, flushed to disk, then atomically renamed.
If disk persistence fails, the request remains an error; no successful recovery
claim is made. Monitor free disk space and back up this directory separately.
An abrupt power loss before a pending write reaches disk can still lose it.

From `/var/www/fakeweather`, using the application's Python environment:

```sh
nix_venv/bin/python replay_failed.py --list
nix_venv/bin/python replay_failed.py var/failed-records/RECORD-UUID.json
nix_venv/bin/python replay_failed.py --all
```

`--directory /absolute/path` selects a different pending directory for `--list`
and `--all`. Each record is attempted in its own database transaction. Corrupt
files and database failures remain pending, and `--all` continues to the next
record. A nonzero exit code means at least one record failed. Error output names
the exception type without exposing connection details. Successful files move
to `processed/`; retain them for audit or remove them under your retention policy.
Small `.lock` files are expected and may remain. OS locks prevent concurrent
replay of the same pending path and release automatically after a process exits.

The replay command creates `weather_recovery_receipts` in the configured database
if needed. The database user needs permission to create this table, or an
administrator must create it and grant the application user SELECT and INSERT:

```sql
CREATE TABLE weather_recovery_receipts (
    recovery_id uuid PRIMARY KEY,
    inserted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

A receipt and its weather row commit together. Replaying the same recovery UUID
again skips the weather insert, including after a crash between database commit
and file archival. Do not delete receipts while retained recovery files might be
replayed, and do not change recovery IDs. The first failed live write has no
receipt: if PostgreSQL committed it but the connection lost the acknowledgement,
replaying that file can duplicate the original observation. Inspect the stored
timestamp and values against the database before replaying an ambiguous commit.
Separate station retries are also distinct observations under the legacy input
protocol; the spool cannot infer that they represent the same sample.
