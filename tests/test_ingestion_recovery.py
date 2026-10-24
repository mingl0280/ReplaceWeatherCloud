import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import main
from scripts import db_ops
from scripts.failed_records import load_failed_record


PARAMETERS = {
    "wid": "test-station", "key": "private-station-key", "tempin": 215,
    "humin": 40, "temp": 123, "hum": 50, "dewin": 80, "dew": 70,
    "chill": 100, "heatin": 210, "heat": 120, "thw": 110, "bar": 10132,
    "wspd": 15, "wspdhi": 25, "wdir": 180, "wspdavg": 12, "wdiravg": 170,
    "rainrate": 2, "rain": 3, "solarrad": 1500, "uvi": 20, "battery": "OK",
    "date": "2026-10-24", "time": "12:00:00",
}


class IngestionRecoveryTests(unittest.TestCase):
    def setUp(self):
        main._successful_writes.clear()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        env = patch.dict(os.environ, {"FAKEWEATHER_FAILED_DIR": self.directory.name})
        env.start()
        self.addCleanup(env.stop)

    def test_failed_record_survives_and_identical_retry_is_not_suppressed(self):
        with patch.object(db_ops, "insert_record", side_effect=[RuntimeError("secret"), None]) as insert:
            with TestClient(main.app) as client:
                failure = client.get("/v01/set", params=PARAMETERS)
                success = client.get("/v01/set", params=PARAMETERS)
                duplicate = client.get("/v01/set", params=PARAMETERS)
        self.assertEqual(failure.status_code, 503)
        self.assertTrue(failure.json()["saved_for_recovery"])
        files = list(Path(self.directory.name).glob("*.json"))
        self.assertEqual(len(files), 1)
        recovery_id, record = load_failed_record(files[0])
        self.assertEqual(recovery_id, failure.json()["recovery_id"])
        self.assertEqual(record, insert.call_args_list[0].args[0])
        self.assertNotIn("private-station-key", files[0].read_text())
        self.assertEqual(success.status_code, 200)
        self.assertEqual(duplicate.status_code, 200)
        self.assertEqual(insert.call_count, 2)

    def test_disk_failure_is_explicit_and_does_not_acknowledge_record(self):
        with patch.object(db_ops, "insert_record", side_effect=RuntimeError()), \
                patch.object(main, "save_failed_record", side_effect=OSError("full")), \
                TestClient(main.app) as client:
            response = client.get("/v01/set", params=PARAMETERS)
        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()["saved_for_recovery"])
        self.assertFalse(main._successful_writes)

    def test_invalid_request_never_enters_database_or_spool(self):
        with patch.object(db_ops, "insert_record") as insert, \
                patch.object(main, "save_failed_record") as save, TestClient(main.app) as client:
            response = client.get("/v01/set", params={**PARAMETERS, "temp": "not-an-integer"})
        self.assertEqual(response.status_code, 422)
        insert.assert_not_called()
        save.assert_not_called()
        self.assertFalse(main._successful_writes)
