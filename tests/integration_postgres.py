"""Opt-in real PostgreSQL checks, isolated in a newly created UUID schema.

Run explicitly from the project directory:
    nix_venv/bin/python tests/integration_postgres.py

Uses the configured role/database. Requires CREATE SCHEMA permission. It creates
its own weather table and sequence, never copies production sequence defaults,
never changes server configuration, and drops only its generated test schema.
"""

import socket
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

import replay_failed
from scripts import db_ops
from scripts.configs import GlobalConfig
from scripts.create_db import SCHEMA_SQL
from scripts.failed_records import RECORD_FIELDS, load_failed_record, save_failed_record


def sample_record():
    return {
        **dict.fromkeys(RECORD_FIELDS, 1),
        "dateobj": datetime(2026, 10, 24, 8, 1, 2),
        "batt": "integration-test",
        "temp_in": 21.3,
    }


class PostgreSQLRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original_dsn = GlobalConfig.cfg["database"]["connection_str"]
        cls.schema = "fw_test_" + uuid4().hex
        cls.original_connect = psycopg.connect
        # Bootstrap uses an explicitly qualified schema and no weather queries.
        with cls.original_connect(cls.original_dsn, autocommit=True, connect_timeout=5) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(cls.schema)))
        cls.addClassCleanup(cls.drop_test_schema)

        def isolated_connect(*args, **kwargs):
            # db_ops supplies its own options, so retain its timeouts and append
            # isolation here rather than relying on options in the DSN.
            options = kwargs.get("options", "")
            kwargs["options"] = f"{options} -c search_path={cls.schema}"
            return cls.original_connect(*args, **kwargs)

        cls.connection_patch = patch("scripts.db_ops.psycopg.connect", isolated_connect)
        cls.connection_patch.start()
        cls.addClassCleanup(cls.connection_patch.stop)
        with db_ops.connect() as connection:
            if connection.execute("SELECT current_schema() AS name").fetchone()["name"] != cls.schema:
                raise RuntimeError("Refusing to run without the isolated test schema")
            connection.execute(SCHEMA_SQL)
        print(f"Isolated PostgreSQL schema: {cls.schema}", flush=True)

    @classmethod
    def drop_test_schema(cls):
        GlobalConfig.cfg["database"]["connection_str"] = cls.original_dsn
        # Identifier was generated locally, never read from user input or DB.
        if not cls.schema.startswith("fw_test_") or len(cls.schema) != 40:
            raise RuntimeError("Refusing to drop an unexpected schema name")
        with cls.original_connect(cls.original_dsn, autocommit=True, connect_timeout=5) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(cls.schema)))
        print(f"Removed PostgreSQL test schema: {cls.schema}", flush=True)

    def setUp(self):
        self.assertEqual(db_ops.fetch_one("SELECT current_schema() AS name")["name"], self.schema)
        with db_ops.connect() as connection:
            connection.execute("TRUNCATE weather_data")
        self.directory = tempfile.TemporaryDirectory(prefix="fakeweather-integration-")
        self.addCleanup(self.directory.cleanup)

    def count_records(self):
        return db_ops.fetch_one("SELECT count(*) AS count FROM weather_data")["count"]

    def test_constraint_failure_does_not_poison_next_write(self):
        invalid = dict(sample_record(), batt=None)
        with self.assertRaises(psycopg.errors.NotNullViolation):
            db_ops.insert_record(invalid)
        self.assertEqual(self.count_records(), 0)
        db_ops.insert_record(sample_record())
        self.assertEqual(self.count_records(), 1)

    def test_connection_refused_then_configuration_restored(self):
        # Reserve a local TCP port without listening: no service or DB changes.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved:
            reserved.bind(("127.0.0.1", 0))
            refused_dsn = make_conninfo(
                self.original_dsn, host="127.0.0.1", hostaddr="127.0.0.1",
                port=reserved.getsockname()[1], sslmode="disable",
            )
            try:
                GlobalConfig.cfg["database"]["connection_str"] = refused_dsn
                with self.assertRaises(psycopg.OperationalError):
                    db_ops.insert_record(sample_record())
            finally:
                GlobalConfig.cfg["database"]["connection_str"] = self.original_dsn
        db_ops.insert_record(sample_record())
        self.assertEqual(self.count_records(), 1)

    def test_read_sql_error_does_not_poison_writes(self):
        with self.assertRaises(psycopg.errors.DivisionByZero):
            db_ops.fetch_one("SELECT 1 / 0")
        db_ops.insert_record(sample_record())
        self.assertEqual(self.count_records(), 1)

    def test_disk_record_replays_once_and_preserves_time(self):
        path = save_failed_record(sample_record(), RuntimeError(), directory=self.directory.name)
        recovery_id, record = load_failed_record(path)
        self.assertTrue(replay_failed.replay_file(path))
        self.assertFalse(path.exists())
        self.assertTrue((path.parent / "processed" / path.name).exists())
        self.assertEqual(self.count_records(), 1)
        self.assertEqual(
            db_ops.fetch_one("SELECT localdatetime FROM weather_data")["localdatetime"],
            sample_record()["dateobj"],
        )
        self.assertFalse(replay_failed.replay_record(record, recovery_id))
        self.assertEqual(self.count_records(), 1)

    def test_failed_replay_rolls_back_receipt_and_retains_file(self):
        # Ensure the receipt table exists, so rollback can be queried regardless
        # of test ordering. This insert is in the isolated schema as well.
        replay_failed.replay_record(sample_record(), str(uuid4()))
        invalid = dict(sample_record(), batt="x" * 33)
        path = save_failed_record(invalid, RuntimeError(), directory=self.directory.name)
        recovery_id, _ = load_failed_record(path)
        with self.assertRaises(psycopg.errors.StringDataRightTruncation):
            replay_failed.replay_file(path)
        self.assertTrue(path.exists())
        self.assertEqual(self.count_records(), 1)
        receipt = db_ops.fetch_one(
            "SELECT recovery_id FROM weather_recovery_receipts WHERE recovery_id = %s",
            (recovery_id,),
        )
        self.assertIsNone(receipt)
        self.assertTrue(replay_failed.replay_record(sample_record(), recovery_id))
        self.assertEqual(self.count_records(), 2)

    def test_archive_failure_retry_does_not_duplicate_committed_replay(self):
        path = save_failed_record(sample_record(), RuntimeError(), directory=self.directory.name)
        with patch("replay_failed.os.replace", side_effect=OSError("simulated archival failure")):
            with self.assertRaises(OSError):
                replay_failed.replay_file(path)
        self.assertTrue(path.exists())
        self.assertEqual(self.count_records(), 1)
        self.assertFalse(replay_failed.replay_file(path))
        self.assertEqual(self.count_records(), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
