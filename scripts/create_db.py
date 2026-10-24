"""Create a fresh schema using the configured database role.

Run python -m scripts.create_db (or python create_db.py) deliberately against a
new database. Existing tables cause rollback; this is not a migration tool.
"""
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg

SCHEMA_SQL = """create table weather_data
            (
                index_id           bigserial      not null primary key,
                localdatetime      timestamp(0)   not null,
                tempindoor         numeric(8, 2)  not null,
                humindoor          integer        not null,
                tempoutdoor        numeric(8, 2)  not null,
                humoutdoor         integer        not null,
                dewindoor          numeric(8, 2)  not null,
                dewoutdoor         numeric(8, 2)  not null,
                "WindChill"        numeric(8, 2)  not null,
                heatindex          numeric(8, 2)  not null,
                temphumidwindindex numeric(8, 2)  not null,
                barometer          numeric(10, 2) not null,
                windspd            numeric(8, 2)  not null,
                highwindspd        numeric(8, 2)  not null,
                winddirection      smallint       not null,
                avgwindspd         numeric(8, 2)  not null,
                avgwinddir         numeric(8, 2)  not null,
                rainrate           numeric(8, 2)  not null,
                raindaily          numeric(8, 2)  not null,
                solarrad           numeric(12, 2) not null,
                uvindex            numeric(8, 2)  not null,
                batterystate       varchar(32)    not null,
                heat               numeric(8, 2)  not null
            );
CREATE INDEX idx_ldt ON weather_data (localdatetime);"""


def create_db(connection_str=None):
    if connection_str is None:
        from scripts.configs import check_config
        connection_str = check_config()["database"]["connection_str"]
    # Psycopg commits on normal context exit and rolls back on any exception.
    with psycopg.connect(connection_str) as connection:
        with connection.cursor() as cursor:
            cursor.execute(SCHEMA_SQL)


def main():
    try:
        create_db()
    except (psycopg.Error, RuntimeError) as exc:
        print(f"Schema creation failed ({type(exc).__name__}); check configuration, "
              "database permissions, and whether weather_data already exists.", file=sys.stderr)
        return 1
    print("Weather schema created successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
