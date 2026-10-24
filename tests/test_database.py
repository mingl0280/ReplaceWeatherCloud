"""Regression checks for connection recovery and write endpoint responses."""
import asyncio
import inspect
import os
import tempfile
import threading
import unittest
import os
import tempfile

import httpx
import psycopg
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

import main
from scripts import db_ops


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        main._successful_writes.clear()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        environment = patch.dict(os.environ, {"FAKEWEATHER_FAILED_DIR": directory.name})
        environment.start()
        self.addCleanup(environment.stop)

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


class AsyncDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        main._successful_writes.clear()
        main._write_lock = asyncio.Lock()

    async def test_slow_write_does_not_block_homepage(self):
        started = threading.Event()
        release = threading.Event()

        def slow_insert(record):
            started.set()
            if not release.wait(3):
                raise RuntimeError("test release timed out")

        parameters = {
            name: ("test" if param.annotation is str else 100)
            for name, param in inspect.signature(main.set_api).parameters.items()
            if param.default is inspect.Parameter.empty
        }
        transport = httpx.ASGITransport(app=main.app)
        with patch.object(db_ops, "insert_record", side_effect=slow_insert):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                writing = asyncio.create_task(client.get("/v01/set", params=parameters))
                try:
                    self.assertTrue(await asyncio.to_thread(started.wait, 2))
                    response = await asyncio.wait_for(client.get("/"), timeout=1)
                    self.assertEqual(response.status_code, 200)
                    self.assertFalse(writing.done(), "write must still await release")
                finally:
                    release.set()
                    response = await writing
                self.assertEqual(response.status_code, 200)

    async def test_read_error_is_sanitized_503(self):
        transport = httpx.ASGITransport(app=main.app)
        with patch.object(db_ops, "fetch_one", side_effect=psycopg.OperationalError("secret-password")):
            with self.assertLogs(level="ERROR") as logs:
                async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                    response = await client.get("/api/latest")
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("secret-password", response.text)
        self.assertNotIn("secret-password", "".join(logs.output))

    async def test_static_files_are_independent_of_working_directory(self):
        original_directory = os.getcwd()
        transport = httpx.ASGITransport(app=main.app)
        with tempfile.TemporaryDirectory() as directory:
            try:
                os.chdir(directory)
                async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                    for url in ("/", "/detaildata.html", "/static/css/page_custom_styles.css"):
                        response = await client.get(url)
                        self.assertEqual(response.status_code, 200, url)
            finally:
                os.chdir(original_directory)


if __name__ == "__main__":
    unittest.main()
