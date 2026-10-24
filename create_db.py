"""Compatibility entry point for the schema setup tool."""
from scripts.create_db import create_db, main


if __name__ == "__main__":
    raise SystemExit(main())
