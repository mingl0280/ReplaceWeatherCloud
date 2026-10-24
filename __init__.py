"""Compatibility entry point for starting the weather service."""
import uvicorn

from scripts.configs import check_config


def main():
    server = check_config()["server"]
    uvicorn.run("main:app", host=server["host"], port=server["port"],
                log_level=server["log_level"])


if __name__ == "__main__":
    main()
