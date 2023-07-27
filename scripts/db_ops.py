import psycopg
from fastapi import Query
from psycopg.rows import dict_row

from scripts.configs import GlobalConfig


async def query_db_by_time(sql_query_str: str,
                           start_timestamp: str = Query(None, alias="startTime"),
                           end_timestamp: str = Query(None, alias="endTime")):
    global db_conn
    check_db()

    try:
        result = db_conn.cursor().execute(sql_query_str, {
            "sttms": start_timestamp,
            "edtms": end_timestamp
        }, prepare=True)
    except Exception as e:
        db_conn.close()
        return str(e)

    return result.fetchall()


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


def check_db():
    global db_conn
    if db_conn.broken:
        db_conn.close()
        db_conn = psycopg.connect(GlobalConfig.cfg["database"]["connection_str"], row_factory=dict_row)
        db_conn.prepare_threshold = 0

    if db_conn.closed:
        db_conn = psycopg.connect(GlobalConfig.cfg["database"]["connection_str"], row_factory=dict_row)
        db_conn.prepare_threshold = 0


db_conn = psycopg.connect(GlobalConfig.cfg["database"]["connection_str"], row_factory=dict_row)
