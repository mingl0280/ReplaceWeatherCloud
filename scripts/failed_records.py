"""Private, durable per-record recovery files for failed weather inserts."""

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4


RECORD_FIELDS = frozenset({
    "dateobj", "temp_in", "hum_in", "temp_out", "hum_out", "dew_in",
    "dew_out", "chill_idx", "heatindex", "thw_idx", "baro", "wind_spd",
    "high_wind", "wind_dir", "avg_wind_spd", "avg_wind_dir", "rainrate",
    "raindaily", "solar_rad", "uvi", "batt", "heat",
})


def spool_directory():
    configured = os.environ.get("FAKEWEATHER_FAILED_DIR")
    return Path(configured).resolve() if configured else (
        Path(__file__).resolve().parents[1] / "var" / "failed-records"
    )


def sync_directory(directory):
    """Persist a directory entry on POSIX; Windows has no directory fsync."""
    if os.name != "nt":
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def validate_record(record):
    if not isinstance(record, dict) or set(record) != RECORD_FIELDS:
        raise ValueError("Recovery record has missing or unexpected fields")
    if not isinstance(record["dateobj"], datetime):
        raise ValueError("Recovery record dateobj must be a datetime")
    if not isinstance(record["batt"], str):
        raise ValueError("Recovery battery must be a string")
    for field in RECORD_FIELDS - {"dateobj", "batt"}:
        value = record[field]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"Recovery field {field} must be a finite number")


def save_failed_record(record, error, *, directory=None):
    """Persist normalized values, never URL credentials or exception messages.

    Raises on disk failure so callers cannot falsely report durable recovery.
    """
    validate_record(record)
    directory = Path(directory) if directory is not None else spool_directory()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    record_id = str(uuid4())
    destination = directory / f"{record_id}.json"
    temporary = directory / f".{record_id}.tmp"
    values = dict(record, dateobj=record["dateobj"].isoformat())
    envelope = {
        "version": 1,
        "recovery_id": record_id,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "error_type": type(error).__name__,
        "record": values,
    }
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(envelope, stream, ensure_ascii=False, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        sync_directory(directory)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def load_failed_record(path):
    with Path(path).open(encoding="utf-8") as stream:
        envelope = json.load(stream)
    if envelope.get("version") != 1:
        raise ValueError("Unsupported recovery file version")
    recovery_id = str(UUID(envelope["recovery_id"]))
    record = envelope["record"]
    record["dateobj"] = datetime.fromisoformat(record["dateobj"])
    validate_record(record)
    return recovery_id, record
