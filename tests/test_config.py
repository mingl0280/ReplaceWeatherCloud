import os
import tempfile
import unittest
from pathlib import Path

from scripts.configs import Config, PROJECT_ROOT


VALID = """server:
  host: 127.0.0.1
  port: 8000
  log_level: info
database:
  connection_str: postgresql://example/test
"""


class ConfigTests(unittest.TestCase):
    def test_explicit_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text(VALID, encoding="utf-8")
            config = Config(path)
            self.assertTrue(config.init_ok)
            self.assertEqual(config.cfg["server"]["port"], 8000)

    def test_default_path_does_not_follow_working_directory(self):
        original = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            try:
                os.chdir(directory)
                self.assertEqual(Config().path, PROJECT_ROOT / "config.yaml")
            finally:
                os.chdir(original)

    def test_missing_file_gives_instructions_without_creating_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing.yaml"
            with self.assertRaisesRegex(RuntimeError, "Copy default.yaml"):
                Config(path)
            self.assertFalse(path.exists())

    def test_invalid_configuration_does_not_echo_secrets(self):
        contents = ["", "- list", "server: {}", VALID.replace("8000", "70000"),
                    VALID.replace("8000", "true"), VALID.replace("postgresql://example/test", "''"),
                    "server: [private-secret", "!!python/object:private-secret {}"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            for content in contents:
                with self.subTest(content=content):
                    path.write_text(content, encoding="utf-8")
                    with self.assertRaises(RuntimeError) as caught:
                        Config(path)
                    self.assertNotIn("private-secret", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
