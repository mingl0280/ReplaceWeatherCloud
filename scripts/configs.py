"""Load configuration independently of the process working directory."""
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class Config:
    def __init__(self, path=None):
        self.path = Path(path) if path is not None else PROJECT_ROOT / "config.yaml"
        self.init_ok = False
        try:
            with self.path.open(encoding="utf-8") as config_file:
                cfg = yaml.safe_load(config_file)
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"Configuration missing: {self.path}. Copy default.yaml to this path "
                "and configure the server and database before starting."
            ) from exc
        except yaml.YAMLError:
            # Parser diagnostics can contain database credentials from the source.
            raise RuntimeError(f"Invalid YAML in {self.path}; check its syntax.") from None
        if not isinstance(cfg, dict):
            raise RuntimeError(f"Configuration {self.path} must be a mapping.")
        server = cfg.get("server")
        database = cfg.get("database")
        if not isinstance(server, dict) or not isinstance(database, dict):
            raise RuntimeError("Configuration requires server and database mappings.")
        for key in ("host", "log_level"):
            if not isinstance(server.get(key), str) or not server[key].strip():
                raise RuntimeError(f"Configuration server.{key} must be a nonempty string.")
        port = server.get("port")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise RuntimeError("Configuration server.port must be an integer from 1 to 65535.")
        connection_str = database.get("connection_str")
        if not isinstance(connection_str, str) or not connection_str.strip():
            raise RuntimeError("Configuration database.connection_str must be a nonempty string.")
        self.cfg = cfg
        self.init_ok = True


GlobalConfig = Config()


def check_config():
    """Return the configuration already validated at startup."""
    if not GlobalConfig.init_ok:
        raise RuntimeError("Configuration has not been initialized.")
    return GlobalConfig.cfg


legendName = [
    "0.0-0.2 m/s",
    "0.3-1.5 m/s",
    "1.6-3.3 m/s",
    "3.4-5.4 m/s",
    "5.5-7.9 m/s",
    "8.0-10.7 m/s",
    "10.8-13.8 m/s",
    "13.9-17.1 m/s",
    "17.2-20.7 m/s",
    "20.8-24.4 m/s",
    "24.5-28.4 m/s",
    "28.5-32.6 m/s",
    ">32.6 m/s"
]

solar_window_size = 15
rain_window_size = 60
barometer_window_size = 30
