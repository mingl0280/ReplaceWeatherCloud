import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import create_db


class SetupTests(unittest.TestCase):
    def test_schema_creation_uses_transaction_context(self):
        with patch.object(create_db.psycopg, "connect") as connect:
            create_db.create_db("test connection")
        connect.assert_called_once_with("test connection")
        connection = connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.execute.assert_called_once_with(create_db.SCHEMA_SQL)
        connect.return_value.__exit__.assert_called_once_with(None, None, None)
        self.assertNotIn("owner to", create_db.SCHEMA_SQL.lower())
        self.assertNotIn("grant", create_db.SCHEMA_SQL.lower())

    def test_schema_failure_reaches_transaction_context(self):
        with patch.object(create_db.psycopg, "connect") as connect:
            connection = connect.return_value.__enter__.return_value
            cursor = connection.cursor.return_value.__enter__.return_value
            cursor.execute.side_effect = RuntimeError("test failure")
            with self.assertRaises(RuntimeError):
                create_db.create_db("test connection")
            self.assertIs(connect.return_value.__exit__.call_args[0][0], RuntimeError)

    def test_legacy_server_entry_point_is_import_safe_and_uses_config(self):
        path = Path(__file__).resolve().parents[1] / "__init__.py"
        spec = importlib.util.spec_from_file_location("legacy_entry_point", path)
        module = importlib.util.module_from_spec(spec)
        with patch("uvicorn.run") as run:
            spec.loader.exec_module(module)
            run.assert_not_called()
            with patch.object(module, "check_config", return_value={"server": {
                "host": "127.0.0.1", "port": 8888, "log_level": "info"
            }}):
                module.main()
            run.assert_called_once_with("main:app", host="127.0.0.1", port=8888, log_level="info")


if __name__ == "__main__":
    unittest.main()
