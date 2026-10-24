import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import replay_failed
from scripts.failed_records import RECORD_FIELDS, load_failed_record, save_failed_record


def sample_record():
    return {
        **dict.fromkeys(RECORD_FIELDS, 1),
        "dateobj": datetime(2026, 10, 24, 8, 1, 2, 345678),
        "batt": "0",
        "temp_in": 21.3,
    }


class FailedRecordTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def save(self):
        return save_failed_record(sample_record(), RuntimeError("secret password"), directory=self.directory)

    def test_round_trip_preserves_original_timestamp_and_values(self):
        path = self.save()
        recovery_id, record = load_failed_record(path)
        self.assertEqual(record, sample_record())
        self.assertEqual(path.stem, recovery_id)
        self.assertNotIn("secret password", path.read_text())
        self.assertEqual(list(self.directory.glob("*.tmp")), [])
        if os.name != "nt":
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_credential_and_nonfinite_values_are_rejected(self):
        for bad in ({**sample_record(), "key": "secret"}, {**sample_record(), "temp_in": float("nan")}):
            with self.assertRaises(ValueError):
                save_failed_record(bad, RuntimeError(), directory=self.directory)
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_disk_failure_is_not_reported_as_saved(self):
        with patch("scripts.failed_records.os.fsync", side_effect=OSError("disk error")):
            with self.assertRaises(OSError):
                self.save()
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_success_archives_and_passes_stable_uuid(self):
        path = self.save()
        writer = MagicMock(return_value=True)
        self.assertTrue(replay_failed.replay_file(path, writer=writer))
        writer.assert_called_once_with(sample_record(), path.stem)
        self.assertFalse(path.exists())
        self.assertTrue((self.directory / "processed" / path.name).exists())

    def test_database_failure_retains_pending_file(self):
        path = self.save()
        with self.assertRaises(RuntimeError):
            replay_failed.replay_file(path, writer=MagicMock(side_effect=RuntimeError()))
        self.assertTrue(path.exists())

    def test_archive_failure_can_retry_without_duplicate(self):
        path = self.save()
        seen = set()

        def writer(record, recovery_id):
            if recovery_id in seen:
                return False
            seen.add(recovery_id)
            return True

        with patch("replay_failed.os.replace", side_effect=OSError()):
            with self.assertRaises(OSError):
                replay_failed.replay_file(path, writer=writer)
        self.assertTrue(path.exists())
        self.assertFalse(replay_failed.replay_file(path, writer=writer))
        self.assertEqual(len(seen), 1)

    def test_lock_rejects_concurrent_replay(self):
        path = self.save()
        writer = MagicMock()
        with replay_failed.replay_lock(path):
            with self.assertRaises(OSError):
                replay_failed.replay_file(path, writer=writer)
        writer.assert_not_called()
        self.assertTrue(path.exists())

    def test_all_continues_after_corrupt_record_and_returns_failure(self):
        (self.directory / "000-corrupt.json").write_text("not json")
        path = self.save()
        with patch("replay_failed.replay_record", return_value=True), redirect_stdout(io.StringIO()):
            self.assertEqual(replay_failed.main(["--all", "--directory", str(self.directory)]), 1)
        self.assertTrue((self.directory / "000-corrupt.json").exists())
        self.assertFalse(path.exists())

    def test_list_does_not_connect(self):
        path = self.save()
        with patch("replay_failed.replay_record") as writer, redirect_stdout(io.StringIO()) as output:
            self.assertEqual(replay_failed.main(["--list", "--directory", str(self.directory)]), 0)
        self.assertIn(path.name, output.getvalue())
        writer.assert_not_called()

    def test_invalid_schema_is_retained(self):
        path = self.save()
        envelope = json.loads(path.read_text())
        envelope["record"]["key"] = "must not be replayed"
        path.write_text(json.dumps(envelope))
        with self.assertRaises(ValueError):
            replay_failed.replay_file(path, writer=MagicMock())
        self.assertTrue(path.exists())

    def test_receipt_and_insert_use_same_transaction(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = {"recovery_id": "123"}
        db = SimpleNamespace(connect=MagicMock(), INSERT_RECORD_SQL="INSERT WEATHER")
        db.connect.return_value.__enter__.return_value = connection
        with patch.dict(sys.modules, {"scripts.db_ops": db}), patch("scripts.db_ops", db, create=True):
            self.assertTrue(replay_failed.replay_record(sample_record(), "123"))
            connection.transaction.assert_called_once()
            self.assertEqual(cursor.execute.call_count, 3)
            self.assertEqual(cursor.execute.call_args.args[0], "INSERT WEATHER")
            cursor.reset_mock()
            cursor.fetchone.return_value = None
            self.assertFalse(replay_failed.replay_record(sample_record(), "123"))
            self.assertEqual(cursor.execute.call_count, 2)


if __name__ == "__main__":
    unittest.main()
