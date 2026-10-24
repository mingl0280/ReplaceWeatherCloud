import psycopg
from fastapi import Query
from psycopg.rows import dict_row
from starlette.concurrency import run_in_threadpool

from scripts.configs import GlobalConfig


async def query_db_by_time(sql_query_str: str,
                           start_timestamp: str = Query(None, alias="startTime"),
                           end_timestamp: str = Query(None, alias="endTime")):
    return await run_in_threadpool(fetch_all, sql_query_str, {
        "sttms": start_timestamp,
        "edtms": end_timestamp,
    })


async def get_raw_wind_by_time(start_timestamp: str = Query(None, alias="startTime"),
                               end_timestamp: str = Query(None, alias="endTime")):
    return await query_db_by_time("SELECT localdatetime AS \"Time\", "
                                  " windspd AS \"Speed\", highwindspd AS \"Gust\", "
                                  " winddirection as \"Direction\" FROM weather_data "
                                  "WHERE localdatetime between (%(sttms)s) and (%(edtms)s) "
                                  "ORDER BY localdatetime DESC",
                                  start_timestamp=start_timestamp,
                                  end_timestamp=end_timestamp)


async def get_raw_rain_by_time(start_timestamp: str = Query(None, alias="startTime"),
                               end_timestamp: str = Query(None, alias="endTime")):
    return await query_db_by_time("SELECT localdatetime AS \"Time\", "
                                  "rainrate as \"Rain\" FROM weather_data  "
                                  "WHERE localdatetime between (%(sttms)s) and (%(edtms)s) "
                                  "ORDER BY localdatetime DESC",
                                  start_timestamp=start_timestamp,
                                  end_timestamp=end_timestamp)


async def get_raw_temp_by_time(start_timestamp: str = Query(None, alias="startTime"),
                               end_timestamp: str = Query(None, alias="endTime")):
    return await query_db_by_time("SELECT localdatetime AS \"Time\", "
                                  " tempoutdoor as \"TempOut\", tempindoor AS \"TempIn\" FROM weather_data "
                                  "WHERE localdatetime BETWEEN (%(sttms)s) AND (%(edtms)s) "
                                  " ORDER BY localdatetime DESC",
                                  start_timestamp=start_timestamp,
                                  end_timestamp=end_timestamp)


async def get_raw_barometer_by_time(start_timestamp: str = Query(None, alias="startTime"),
                                    end_timestamp: str = Query(None, alias="endTime")):
    return await query_db_by_time("SELECT localdatetime AS \"Time\", "
                                  " barometer as \"Baro\", index_id FROM weather_data "
                                  "WHERE localdatetime BETWEEN (%(sttms)s) AND (%(edtms)s) "
                                  "ORDER BY localdatetime DESC",
                                  start_timestamp=start_timestamp,
                                  end_timestamp=end_timestamp)


async def get_raw_solar_by_time(start_timestamp: str = Query(None, alias="startTime"),
                                end_timestamp: str = Query(None, alias="endTime")):
    return await query_db_by_time("SELECT localdatetime AS \"Time\", "
                                  "solarrad as \"Solar\", index_id FROM weather_data "
                                  "WHERE localdatetime BETWEEN (%(sttms)s) AND (%(edtms)s) "
                                  "ORDER BY localdatetime DESC",
                                  start_timestamp=start_timestamp,
                                  end_timestamp=end_timestamp)



def connect():
    """Open a bounded connection owned by one operation, never by the module."""
    return psycopg.connect(
        GlobalConfig.cfg["database"]["connection_str"],
        row_factory=dict_row,
        autocommit=True,
        connect_timeout=5,
        options="-c statement_timeout=15000 -c lock_timeout=5000",
    )


def fetch_all(query, parameters=None):
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            return cursor.fetchall()


def fetch_one(query, parameters=None):
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            return cursor.fetchone()


INSERT_RECORD_SQL = """
    INSERT INTO weather_data
    (localdatetime, tempindoor, humindoor, tempoutdoor, humoutdoor, dewindoor,
     dewoutdoor, "WindChill", heatindex, temphumidwindindex, barometer, windspd,
     highwindspd, winddirection, avgwindspd, avgwinddir, rainrate, raindaily,
     solarrad, uvindex, batterystate, heat)
    VALUES
    (%(dateobj)s, %(temp_in)s, %(hum_in)s, %(temp_out)s, %(hum_out)s, %(dew_in)s,
     %(dew_out)s, %(chill_idx)s, %(heatindex)s, %(thw_idx)s, %(baro)s, %(wind_spd)s,
     %(high_wind)s, %(wind_dir)s, %(avg_wind_spd)s, %(avg_wind_dir)s, %(rainrate)s,
     %(raindaily)s, %(solar_rad)s, %(uvi)s, %(batt)s, %(heat)s)
"""


def insert_record(record):
    """Commit exactly one normalized record; rollback and close on any failure.

    Do not automatically retry: a lost commit acknowledgement is ambiguous.
    """
    with connect() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(INSERT_RECORD_SQL, record)
