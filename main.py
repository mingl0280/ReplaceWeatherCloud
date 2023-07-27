# MIT License
#
# Copyright (c) 2022 mingl0280@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import shutil
from datetime import datetime as dt
from decimal import Decimal
from os.path import exists
from typing import Optional

import uvicorn
from fastapi import FastAPI, Query
from starlette.responses import FileResponse
from starlette.staticfiles import StaticFiles

import scripts.db_ops
from scripts.configs import check_config, solar_window_size, rain_window_size, barometer_window_size, GlobalConfig
from scripts.db_ops import get_raw_wind_by_time, get_raw_rain_by_time, get_raw_temp_by_time, get_raw_barometer_by_time, \
    get_raw_solar_by_time, check_db, db_conn
from scripts.helper_functions import get_interval_where_str, process_wind_data, \
    process_solar_data, process_rain_data, process_barometer, get_timediff_wind_window_size, process_rose_map, \
    make_times_limited, process_temperature_units

db_conn.prepare_threshold = 0

check_config()

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    return FileResponse('index.html')


@app.get("/detaildata.html")
async def detail_response():
    return FileResponse('detaildata.html')


@app.get("/favicon.ico")
async def icon_ret():
    return FileResponse('favicon.ico')


async def get_wind(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    check_db()
    where_str = get_interval_where_str(prior_days, prior_hrs)

    sql_query_str = \
        "SELECT localdatetime as \"Time\", windspd AS \"Speed\", highwindspd AS \"Gust\", winddirection AS \"Direction\" FROM weather_data " + where_str + " ORDER BY localdatetime DESC"
    result = scripts.db_ops.db_conn.cursor().execute(sql_query_str)

    return result.fetchall()


async def get_raw_baro(prior_days: Optional[int] = Query(None, alias="priorDays"),
                       prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", barometer as \"Baro\", index_id FROM weather_data " + where_str + \
                    " ORDER BY localdatetime DESC"
    result = scripts.db_ops.db_conn.cursor().execute(sql_query_str)
    data = result.fetchall()

    return data


@app.get("/api/solar")
async def get_solar(prior_days: Optional[int] = Query(None, alias="priorDays"),
                    prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    check_db()
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", solarrad as \"Solar\"," \
                    " index_id FROM weather_data " + where_str + \
                    " ORDER BY localdatetime DESC"
    result = scripts.db_ops.db_conn.cursor().execute(sql_query_str)
    data = result.fetchall()
    return_data: list
    try:
        return_data = process_solar_data(data, solar_window_size)
    except:  # on any error
        return_data = []
        scripts.db_ops.db_conn.close()

    return return_data


@app.get("/api/rain")
async def get_rain(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    check_db()
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", rainrate as \"Rain\" FROM weather_data " + where_str + \
                    " ORDER BY localdatetime DESC"
    result = scripts.db_ops.db_conn.cursor().execute(sql_query_str)
    data = result.fetchall()

    return process_rain_data(raw_data=data, sliding_window=rain_window_size)


@app.get("/api/temperature")
async def get_temp(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", tempoutdoor as \"TempOut\", tempindoor AS \"TempIn\"  FROM weather_data {0} ORDER BY localdatetime DESC".format(
        where_str)
    result = scripts.db_ops.db_conn.cursor().execute(sql_query_str)
    data = result.fetchall()
    return_data = []

    for item in data:
        return_data.append([
            item["Time"],
            item["TempOut"],
            item['TempIn']
        ])

    return return_data


@app.get("/api/barometer")
async def get_baro(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs"),
                   altitude: Optional[int] = Query(None, alias="altitude")):
    data = await get_raw_baro(prior_days, prior_hrs)

    return process_barometer(data, barometer_window_size, altitude)


@app.get("/api/windByTime")
async def get_wind_by_time_difference(prior_days: Optional[int] = Query(None, alias="priorDays"),
                                      prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    raw_data = await get_wind(prior_days, prior_hrs)
    window_size = await get_timediff_wind_window_size(prior_days, prior_hrs)
    return process_wind_data(raw_data, window_size)


@app.get("/api/wind/rosemap")
async def get_rosemap_item(speed_type: Optional[int] = Query(0, alias="SpeedType"),
                           prior_days: Optional[int] = Query(None, alias="priorDays"),
                           prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    raw_data = await get_wind(prior_days, prior_hrs)
    return process_rose_map(raw_data, speed_type)


@app.get("/api/ByTime/wind/rosemap")
async def get_rosemap_by_time(speed_type: Optional[int] = Query(0, alias="SpeedType"),
                              start_timestamp: str = Query(None, alias="startTime"),
                              end_timestamp: str = Query(None, alias="endTime")):
    if start_timestamp is None or end_timestamp is None:
        return {"error": "startTime is None or endTime is None", "code": 500}

    start_timestamp, end_timestamp = make_times_limited(start_timestamp, end_timestamp)
    raw_data = await get_raw_wind_by_time(start_timestamp, end_timestamp)
    return process_rose_map(raw_data, speed_type)


@app.get("/api/ByTime/wind")
async def get_wind_by_time(start_timestamp: str = Query(None, alias="startTime"),
                           end_timestamp: str = Query(None, alias="endTime")):
    if start_timestamp is None or end_timestamp is None:
        return {"error": "startTime is None or endTime is None", "code": 500}

    start_timestamp, end_timestamp = make_times_limited(start_timestamp, end_timestamp)

    raw_data = await get_raw_wind_by_time(start_timestamp, end_timestamp)
    window_size = 5
    return process_wind_data(raw_data, window_size)


@app.get("/api/ByTime/rain")
async def get_rain_by_time(start_timestamp: str = Query(None, alias="startTime"),
                           end_timestamp: str = Query(None, alias="endTime")):
    if start_timestamp is None or end_timestamp is None:
        return {"error": "startTime is None or endTime is None", "code": 500}
    start_timestamp, end_timestamp = make_times_limited(start_timestamp, end_timestamp)
    raw_data = await get_raw_rain_by_time(start_timestamp, end_timestamp)
    return process_rain_data(raw_data, sliding_window=rain_window_size)


@app.get("/api/ByTime/temperature")
async def get_temp_by_time(start_timestamp: str = Query(None, alias="startTime"),
                           end_timestamp: str = Query(None, alias="endTime"),
                           unit: Optional[int] = Query(None, alias="unit")):
    if start_timestamp is None or end_timestamp is None:
        return {"error": "startTime is None or endTime is None", "code": 500}

    start_timestamp, end_timestamp = make_times_limited(start_timestamp, end_timestamp)
    raw_data = await get_raw_temp_by_time(start_timestamp, end_timestamp)
    return_data = await process_temperature_units(raw_data, unit)

    return return_data


@app.get("/api/ByTime/solar")
async def get_solar_by_time(start_timestamp: str = Query(None, alias="startTime"),
                            end_timestamp: str = Query(None, alias="endTime")):
    if start_timestamp is None or end_timestamp is None:
        return {"error": "startTime is None or endTime is None", "code": 500}
    start_timestamp, end_timestamp = make_times_limited(start_timestamp, end_timestamp)
    raw_data = await get_raw_solar_by_time(start_timestamp, end_timestamp)
    return process_solar_data(raw_data, sliding_window=solar_window_size)


@app.get("/api/ByTime/barometer")
async def get_barometer_by_time(start_timestamp: str = Query(None, alias="startTime"),
                                end_timestamp: str = Query(None, alias="endTime"),
                                altitude: Optional[int] = Query(None, alias="altitude")):
    if start_timestamp is None or end_timestamp is None:
        return {"error": "startTime is None or endTime is None", "code": 500}

    start_timestamp, end_timestamp = make_times_limited(start_timestamp, end_timestamp)
    raw_data = await get_raw_barometer_by_time(start_timestamp, end_timestamp)
    window_size = barometer_window_size
    return process_barometer(raw_data, window_size, altitude)


@app.get("/v01/set")
async def set_api(wid: str, key: str, tempin: int, humin: int,
                  temp: int, hum: int, dewin: int, dew: int,
                  chill: int, heatin: int, heat: int, thw: int,
                  bar: int, wspd: int, wspdhi: int, wdir: int, wspdavg: int,
                  wdiravg: int, rainrate: int, rain: int, solarrad: int,
                  uvi: int, battery: str, datestr: str = Query(None, alias="date"),
                  timestr: str = Query(None, alias="time")):
    check_db()
    local_tm = dt.now()
    utc_tm = dt.utcnow()
    offset = local_tm - utc_tm
    long_datetime_val = dt.now()
    temp_indoor = tempin / 10
    heat_index_val = heatin / 10
    temp_outdoor = temp / 10
    heat_val = heat / 10
    wind_speed = wspd / 10
    wind_speed_high = wspdhi / 10
    wind_speed_average = wspdavg / 10
    barometer_hpa = bar / 10
    rain_daily_depth = rain / 10
    solar_radiation = solarrad / 10
    chill_index = chill / 10
    temp_humid_wind_index = thw / 10
    rain_rate = rainrate / 10
    dew_point_outdoor = dew / 10
    dew_point_indoor = dewin / 10
    if temp_indoor < -100 or solar_radiation < -100 or temp_outdoor < -100 or heat_index_val < -100 or thw < -1000 or \
            dew_point_outdoor < -100 or dew_point_indoor < -100:
        return 200

    try:
        result = db_conn.cursor().execute("""
            INSERT INTO "weather_data" 
            ("localdatetime", "tempindoor", "humindoor", "tempoutdoor", "humoutdoor", "dewindoor", 
            "dewoutdoor", "WindChill", "heatindex", "temphumidwindindex", "barometer", "windspd", 
            "highwindspd", "winddirection", "avgwindspd", "avgwinddir", "rainrate", "raindaily", 
            "solarrad", "uvindex", "batterystate", "heat" )
             VALUES 
             (%(dateobj)s, %(temp_in)s, %(hum_in)s, %(temp_out)s, %(hum_out)s, %(dew_in)s, 
             %(dew_out)s, %(chill_idx)s, %(heatindex)s, %(thw_idx)s, %(baro)s, %(wind_spd)s, 
             %(high_wind)s, %(wind_dir)s, %(avg_wind_spd)s, %(avg_wind_dir)s, %(rainrate)s, %(raindaily)s , 
             %(solar_rad)s, %(uvi)s, %(batt)s, %(heat)s);
        """, {
            "dateobj": long_datetime_val,
            "temp_in": temp_indoor,
            "hum_in": humin,
            "temp_out": temp_outdoor,
            "hum_out": hum,
            "dew_in": dew_point_indoor,
            "dew_out": dew_point_outdoor,
            "chill_idx": chill_index,
            "heatindex": heat_index_val,
            "thw_idx": temp_humid_wind_index,
            "baro": barometer_hpa,
            "wind_spd": wind_speed,
            "high_wind": wind_speed_high,
            "wind_dir": wdir,
            "avg_wind_spd": wind_speed_average,
            "avg_wind_dir": wdiravg,
            "rainrate": rain_rate,
            "raindaily": rain_daily_depth,
            "solar_rad": solar_radiation,
            "uvi": uvi,
            "batt": battery,
            "heat": heat_val
        })
        print(result)
        db_conn.commit()
    except Exception as ex:
        print(str(ex))
        db_conn.close()
        return 500

    return 200


if __name__ == "__main__":
    if not GlobalConfig.init_ok:
        exit(0)

    uvicorn.run("main:app", host=GlobalConfig.cfg["server"]["host"],
                port=GlobalConfig.cfg["server"]["port"], log_level=GlobalConfig.cfg["server"]["log_level"])


@app.get("/api/latest")
async def latest_info(altitude: Optional[int] = Query(None, alias="altitude")):
    check_db()
    baro_abs_stmt = "weather_data.barometer as \"barometer_abs\""

    if altitude is not None and altitude != 0:
        fix_num = Decimal(altitude) / Decimal(100) * Decimal(12)
        baro_abs_stmt = " round(weather_data.barometer - {},1)as \"barometer_abs\"".format(fix_num)

    sql_stmt = """SELECT
        to_char(weather_data.localdatetime, 'YYYY-MM-DD HH24:MI:SS') AS "Time", 
        weather_data.tempindoor, 
        weather_data.humindoor, 
        weather_data.tempoutdoor, 
        weather_data.humoutdoor, 
        weather_data.dewindoor, 
        weather_data.dewoutdoor, 
        weather_data."WindChill", 
        weather_data.heatindex, 
        weather_data.temphumidwindindex, 
        weather_data.barometer, 
        {},
        ROUND(weather_data.windspd*1.9438444924, 2) as "windspd", 
        ROUND(weather_data.highwindspd*1.9438444924, 2) as "highwindspd", 
        weather_data.winddirection, 
        ROUND(weather_data.avgwindspd,2) as "avgwindspd", 
        weather_data.avgwinddir, 
        weather_data.rainrate, 
        weather_data.raindaily, 
        weather_data.solarrad, 
        weather_data.uvindex / 10 as "uvindex", 
        weather_data.heat
    FROM
        weather_data
    ORDER BY
        weather_data.localdatetime DESC
    LIMIT 1""".format(baro_abs_stmt)

    return db_conn.cursor().execute(sql_stmt).fetchone()
