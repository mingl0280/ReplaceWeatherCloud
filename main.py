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
import asyncio
import hashlib
import logging
import time
from collections import OrderedDict
from datetime import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Optional

import psycopg
import uvicorn
from fastapi import FastAPI, Query, Request, Response
from starlette.responses import JSONResponse
from starlette.responses import FileResponse
from starlette.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

import scripts.db_ops
from scripts.configs import check_config, solar_window_size, rain_window_size, barometer_window_size, GlobalConfig
from scripts.db_ops import get_raw_wind_by_time, get_raw_rain_by_time, get_raw_temp_by_time, get_raw_barometer_by_time, \
    get_raw_solar_by_time
from scripts.helper_functions import get_interval_where_str, process_wind_data, \
    process_solar_data, process_rain_data, process_barometer, get_timediff_wind_window_size, process_rose_map, \
    make_times_limited, process_temperature_units

check_config()

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.exception_handler(psycopg.Error)
async def database_error_handler(request: Request, error: psycopg.Error):
    logging.error("Database operation failed (%s)", type(error).__name__)
    return JSONResponse(status_code=503, content={"error": "Database unavailable"})

@app.get("/")
async def root():
    return FileResponse(BASE_DIR / "index.html")

# Bound station retries without retaining API keys or acknowledging failed writes.
_successful_writes = OrderedDict()
_write_lock = asyncio.Lock()


@app.middleware("http")
async def deduplication_middleware(request: Request, call_next):
    if request.url.path != "/v01/set":
        return await call_next(request)
    client = request.client.host if request.client else ""
    digest = hashlib.sha256(
        (client + ":" + str(request.query_params)).encode("utf-8")
    ).digest()
    async with _write_lock:
        now = time.monotonic()
        while _successful_writes:
            oldest, recorded_at = next(iter(_successful_writes.items()))
            if now - recorded_at < 5:
                break
            _successful_writes.pop(oldest)
        if digest in _successful_writes:
            return Response(status_code=200)
        response = await call_next(request)
        if response.status_code == 200:
            _successful_writes[digest] = time.monotonic()
            while len(_successful_writes) > 1024:
                _successful_writes.popitem(last=False)
        return response


@app.get("/detaildata.html")
async def detail_response():
    return FileResponse(BASE_DIR / "detaildata.html")


@app.get("/favicon.ico")
async def icon_ret():
    return FileResponse(BASE_DIR / "favicon.ico")


async def get_wind(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    where_str = get_interval_where_str(prior_days, prior_hrs)

    sql_query_str = \
        "SELECT localdatetime as \"Time\", windspd AS \"Speed\", highwindspd AS \"Gust\", winddirection AS \"Direction\" FROM weather_data " + where_str + " ORDER BY localdatetime DESC"
    return await run_in_threadpool(scripts.db_ops.fetch_all, sql_query_str)


async def get_raw_baro(prior_days: Optional[int] = Query(None, alias="priorDays"),
                       prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", barometer as \"Baro\", index_id FROM weather_data " + where_str + \
                    " ORDER BY localdatetime DESC"
    data = await run_in_threadpool(scripts.db_ops.fetch_all, sql_query_str)

    return data


@app.get("/api/solar")
async def get_solar(prior_days: Optional[int] = Query(None, alias="priorDays"),
                    prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", solarrad as \"Solar\"," \
                    " index_id FROM weather_data " + where_str + \
                    " ORDER BY localdatetime DESC"
    data = await run_in_threadpool(scripts.db_ops.fetch_all, sql_query_str)
    return process_solar_data(data, solar_window_size)


@app.get("/api/rain")
async def get_rain(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", rainrate as \"Rain\" FROM weather_data " + where_str + \
                    " ORDER BY localdatetime DESC"
    data = await run_in_threadpool(scripts.db_ops.fetch_all, sql_query_str)

    return process_rain_data(raw_data=data, sliding_window=rain_window_size)


@app.get("/api/temperature")
async def get_temp(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", tempoutdoor as \"TempOut\", tempindoor AS \"TempIn\"  FROM weather_data {0} ORDER BY localdatetime DESC".format(
        where_str)
    data = await run_in_threadpool(scripts.db_ops.fetch_all, sql_query_str)
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

    # handle error
    if temp_indoor < -100 or solar_radiation < -100 or temp_outdoor < -100 or heat_index_val < -100 or thw < -1000 or \
            dew_point_outdoor < -100 or dew_point_indoor < -100:
        return 200

    record = {
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
    }

    try:
        await run_in_threadpool(scripts.db_ops.insert_record, record)
    except Exception as error:
        logging.error("Weather record insert failed (%s)", type(error).__name__)
        return JSONResponse(status_code=503, content={"error": "Database write failed"})

    return 200


@app.get("/api/latest")
async def latest_info(altitude: Optional[int] = Query(None, alias="altitude")):
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

    return await run_in_threadpool(scripts.db_ops.fetch_one, sql_stmt)


if __name__ == "__main__":
    if not GlobalConfig.init_ok:
        exit(0)

    uvicorn.run(app, host=GlobalConfig.cfg["server"]["host"],
                port=GlobalConfig.cfg["server"]["port"], log_level=GlobalConfig.cfg["server"]["log_level"])


