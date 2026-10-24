"""Regression checks for connection recovery and write endpoint responses."""
import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

import main
from scripts import db_ops


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        main._successful_writes.clear()

    def connection(self):
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        return connection, cursor

    def test_failed_write_does_not_poison_next_operation(self):
        failed, failed_cursor = self.connection()
        healthy, healthy_cursor = self.connection()
        failed_cursor.execute.side_effect = RuntimeError("connection lost")
        with patch.object(db_ops, "connect", side_effect=[failed, healthy]):
            with self.assertRaisesRegex(RuntimeError, "connection lost"):
                db_ops.insert_record({"example": 1})
            db_ops.insert_record({"example": 2})
        self.assertIs(failed.transaction.return_value.__exit__.call_args.args[0], RuntimeError)
        failed.__exit__.assert_called_once()
        healthy_cursor.execute.assert_called_once_with(db_ops.INSERT_RECORD_SQL, {"example": 2})
        self.assertEqual(healthy.transaction.return_value.__exit__.call_args.args, (None, None, None))
        healthy.__exit__.assert_called_once()

    def test_connect_failure_does_not_prevent_later_connection(self):
        healthy, cursor = self.connection()
        with patch.object(db_ops, "connect", side_effect=[RuntimeError("offline"), healthy]):
            with self.assertRaisesRegex(RuntimeError, "offline"):
                db_ops.insert_record({})
            db_ops.insert_record({})
        cursor.execute.assert_called_once()

    def test_read_closes_connection_and_cursor(self):
        connection, cursor = self.connection()
        cursor.fetchall.return_value = [{"x": 1}]
        with patch.object(db_ops, "connect", return_value=connection):
            self.assertEqual(db_ops.fetch_all("SELECT 1"), [{"x": 1}])
        connection.__exit__.assert_called_once()
        connection.cursor.return_value.__exit__.assert_called_once()
        connection.transaction.assert_not_called()

    def test_commit_failure_propagates(self):
        connection, _ = self.connection()
        connection.transaction.return_value.__exit__.side_effect = RuntimeError("commit lost")
        with patch.object(db_ops, "connect", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "commit lost"):
                db_ops.insert_record({})
        connection.__exit__.assert_called_once()

    def test_write_http_failure_then_identical_retry_succeeds(self):
        parameters = {
            "wid": "regression", "key": "test", "tempin": 215, "humin": 40,
            "temp": 123, "hum": 50, "dewin": 80, "dew": 70, "chill": 100,
            "heatin": 210, "heat": 120, "thw": 110, "bar": 10132,
            "wspd": 15, "wspdhi": 25, "wdir": 180, "wspdavg": 12,
            "wdiravg": 170, "rainrate": 2, "rain": 3, "solarrad": 1500,
            "uvi": 20, "battery": "OK", "date": "2026-10-24", "time": "12:00:00",
        }
        with patch.object(db_ops, "insert_record", side_effect=[RuntimeError("offline"), None]) as insert:
            with TestClient(main.app) as client:
                failed = client.get("/v01/set", params=parameters)
                succeeded = client.get("/v01/set", params=parameters)
                duplicate = client.get("/v01/set", params=parameters)
        self.assertEqual(failed.status_code, 503)
        self.assertEqual(succeeded.status_code, 200)
        self.assertEqual(succeeded.json(), 200)
        self.assertEqual(duplicate.status_code, 200)
        self.assertEqual(insert.call_count, 2)
        record = insert.call_args.args[0]
        self.assertEqual(record["temp_in"], 21.5)
        self.assertEqual(record["baro"], 1013.2)
        self.assertEqual(record["solar_rad"], 150)
        self.assertEqual(record["uvi"], 20)
        self.assertNotIn("key", record)


if __name__ == "__main__":
    unittest.main()
