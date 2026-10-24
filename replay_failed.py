"""Replay failed weather inserts individually, retaining failures for inspection."""

import argparse
import os
from contextlib import contextmanager
from pathlib import Path

from scripts.failed_records import load_failed_record, spool_directory, sync_directory


@contextmanager
def replay_lock(path):
    """OS advisory lock releases automatically if a replay process crashes."""
    lock_path = path.with_suffix(path.suffix + ".lock")
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(descriptor, "r+b") as stream:
        if os.name == "nt":
            import msvcrt
            if os.fstat(stream.fileno()).st_size == 0:
                stream.write(b"0")
                stream.flush()
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            if os.name == "nt":
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def replay_record(record, recovery_id):
    """Receipt and weather insert commit together; repeat UUIDs do not insert."""
    from scripts import db_ops

    with db_ops.connect() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS weather_recovery_receipts (
                        recovery_id uuid PRIMARY KEY,
                        inserted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cursor.execute("""
                    INSERT INTO weather_recovery_receipts (recovery_id)
                    VALUES (%s) ON CONFLICT (recovery_id) DO NOTHING
                    RETURNING recovery_id
                """, (recovery_id,))
                if cursor.fetchone() is None:
                    return False
                cursor.execute(db_ops.INSERT_RECORD_SQL, record)
                return True


def replay_file(path, *, writer=None):
    path = Path(path).resolve()
    if path.parent.name == "processed":
        raise ValueError("This record is already archived")
    with replay_lock(path):
        recovery_id, record = load_failed_record(path)
        inserted = (writer or replay_record)(record, recovery_id)
        processed = path.parent / "processed"
        processed.mkdir(mode=0o700, exist_ok=True)
        os.replace(path, processed / path.name)
        sync_directory(processed)
        sync_directory(path.parent)
        return inserted


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("record", nargs="?", type=Path, help="one recovery JSON file")
    parser.add_argument("--directory", type=Path, default=spool_directory())
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--list", action="store_true", help="list pending records without connecting")
    mode.add_argument("--all", action="store_true", help="replay all pending records individually")
    args = parser.parse_args(argv)
    if args.record and (args.list or args.all):
        parser.error("choose a record, --list, or --all")
    if not (args.record or args.list or args.all):
        parser.error("choose a record, --list, or --all")
    paths = [args.record] if args.record else sorted(args.directory.glob("*.json"))
    if args.list:
        for path in paths:
            print(path)
        return 0
    failures = 0
    for path in paths:
        try:
            inserted = replay_file(path)
            print(f"{'inserted' if inserted else 'already replayed'}: {path.name}")
        except Exception as error:
            # Database exception text can contain credentials or record contents.
            print(f"retained: {path.name} ({type(error).__name__})")
            failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
