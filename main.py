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
import itertools
import logging
import math
import shutil
import time
from datetime import datetime as dt
from decimal import Decimal
from os.path import exists
from time import mktime
from typing import Optional
from collections import deque

import psycopg
import uvicorn
import yaml
from fastapi import FastAPI, Query
from starlette.responses import FileResponse
from starlette.staticfiles import StaticFiles
from psycopg.rows import dict_row

if not exists("config.yaml"):
    shutil.copy("default.yaml", "config.yaml")
    print("Please edit config.yaml to represent your current configuration!")
    print("App will now quit.")
    exit(0)
else:
    with open("config.yaml", 'r', encoding='utf-8') as in_file:
        yaml_content = in_file.read()
        cfg_items = yaml.load(yaml_content)

db_conn = psycopg.connect(cfg_items["database"]["connection_str"], row_factory=dict_row)

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")


def check_db():
    global db_conn
    if db_conn.broken:
        db_conn.close()
        db_conn = psycopg.connect(cfg_items["database"]["connection_str"])


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


@app.get("/")
async def root():
    return FileResponse('index.html')


@app.get("/favicon.ico")
async def icon_ret():
    return FileResponse('favicon.ico')


class KalmanFilter:
    def __init__(self):
        self.last_p = Decimal(0.02)
        self.current_p = Decimal(0)
        self.out = Decimal(0)
        self.k_gain = Decimal(0)
        self.q = Decimal(0.075)
        self.r = Decimal(0.6)

    def __kalman_filter(self, data_point: Decimal):
        self.current_p = Decimal(self.last_p) + Decimal(self.q)
        self.k_gain = self.current_p / (self.current_p + self.r)
        self.out = self.out + self.k_gain * (data_point - self.out)
        self.last_p = (1 - self.k_gain) * self.current_p
        return self.out

    def flush_data(self, data, col):
        for data_point in data:
            self.__kalman_filter(data_point[col])

    def calc_new_data(self, data):
        return self.__kalman_filter(data)

    def set_q(self, value: Decimal):
        self.q = value

    def set_default_q(self):
        self.q = Decimal(0.075)


def sliding_average(data, col_num, window_size, round_digits):
    tmp_arr = []
    index = 0
    half_size = math.floor(window_size / 2)
    for x in range(0, math.floor(window_size / 2)):
        tmp_arr.append(data[0][col_num])

    for item in data:
        tmp_arr.append(item[col_num])
        index += 1

    for x in range(0, math.floor(window_size / 2)):
        tmp_arr.append(data[-1][col_num])

    dq = deque(tmp_arr[:window_size])

    s = sum(dq)
    arr_avg = []

    for i in range(half_size, len(tmp_arr)):
        s += tmp_arr[i] - dq.popleft()
        dq.append(tmp_arr[i])
        arr_avg.append(round(s / window_size, round_digits))

    ret_data = data

    for i in range(0, index-1):
        ret_data[i][col_num] = arr_avg[i+half_size]

    return ret_data


class RoseMapDirItem:
    type: str
    data: []
    coordinateSystem: str
    name: str
    stack: str
    # itemStyle: {}
    # roundCap: bool


def get_dir_from_angle(angle: int):
    if angle > 348.75 or angle <= 11.25:
        return 0  # N
    if 11.25 < angle <= 33.75:
        return 1  # NNE
    if 33.75 < angle <= 56.25:
        return 2  # NE
    if 56.25 < angle <= 78.75:
        return 3  # ENE
    if 78.75 < angle <= 101.25:
        return 4  # E
    if 101.25 < angle <= 123.75:
        return 5  # ESE
    if 123.75 < angle <= 146.25:
        return 6  # SE
    if 146.25 < angle <= 168.75:
        return 7  # SSE
    if 168.75 < angle <= 191.25:
        return 8  # S
    if 191.25 < angle <= 213.75:
        return 9  # SSW
    if 213.75 < angle <= 236.25:
        return 10  # SW
    if 236.25 < angle <= 258.75:
        return 11  # WSW
    if 258.75 < angle <= 281.25:
        return 12  # W
    if 281.25 < angle <= 303.75:
        return 13  # WNW
    if 303.75 < angle <= 326.25:
        return 14  # NW
    if 326.25 < angle <= 348.75:
        return 15  # NNW


def get_interval_where_str(prior_days, prior_hrs):
    interval_str = ""
    if prior_days is not None:
        interval_str = " " + str(prior_days) + " DAYS"
    else:
        if prior_hrs is not None:
            interval_str = interval_str + " " + str(prior_hrs) + " HOURS"

    if interval_str != "":
        where_str = " WHERE localdatetime > CURRENT_TIMESTAMP - INTERVAL '{intv_str}'"
        where_str = where_str.format_map({"intv_str": interval_str})
    else:
        where_str = ""
    return where_str


@app.get("/api/wind")
async def get_wind(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    global db_conn
    check_db()
    where_str = get_interval_where_str(prior_days, prior_hrs)

    sql_query_str = \
        "SELECT localdatetime AS \"Time\", " \
        " windspd AS \"Speed\", highwindspd AS \"Gust\", " \
        " winddirection as \"Direction\" FROM weather_data " + where_str + " ORDER BY localdatetime DESC"
    result = db_conn.cursor().execute(sql_query_str)

    return result.fetchall()


@app.get("/api/solar")
async def get_solar(prior_days: Optional[int] = Query(None, alias="priorDays"),
                    prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    global db_conn
    check_db()
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", solarrad as \"Solar\" FROM weather_data " + where_str + \
                    " ORDER BY localdatetime DESC"
    result = db_conn.cursor().execute(sql_query_str)
    data = result.fetchall()
    return_data = []
    for item in data:
        try:
            return_data.append([
                item["Time"],
                item["Solar"],
                item["Solar"]
            ])
        except:
            db_conn.close()
            db_conn = psycopg.connect(cfg_items["database"]["connection_str"])

    return_data = sliding_average(return_data, 2, 15, 1)

    return return_data


@app.get("/api/rain")
async def get_rain(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    global db_conn
    check_db()
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", rainrate as \"Rain\" FROM weather_data " + where_str + \
                    " ORDER BY localdatetime DESC"
    result = db_conn.cursor().execute(sql_query_str)
    data = result.fetchall()
    return_data = []

    for item in data:
        return_data.append([
            item["Time"],
            item["Rain"]
        ])

    return return_data


@app.get("/api/temperature")
async def get_temp(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    global db_conn
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", tempoutdoor as \"TempOut\", tempindoor as \"TempIn\" FROM weather_data " + where_str + \
                    " ORDER BY localdatetime DESC"
    result = db_conn.cursor().execute(sql_query_str)
    data = result.fetchall()
    return_data = []

    for item in data:
        return_data.append([
            item["Time"],
            item["TempOut"],
            item['TempIn']
        ])

    return return_data


#@app.get("/api/raw_barometer")
async def get_raw_baro(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    global db_conn
    where_str = get_interval_where_str(prior_days, prior_hrs)
    sql_query_str = "SELECT localdatetime AS \"Time\", barometer as \"Baro\" FROM weather_data " + where_str + \
                    " ORDER BY localdatetime DESC"
    result = db_conn.cursor().execute(sql_query_str)
    data = result.fetchall()

    return data


@app.get("/api/barometer")
async def get_baro(prior_days: Optional[int] = Query(None, alias="priorDays"),
                   prior_hrs: Optional[int] = Query(None, alias="priorHrs"),
                   altitude: Optional[int] = Query(None, alias="altitude")):

    data = await get_raw_baro(prior_days, prior_hrs)

    window_size = 30

    interm_data = []

    fix_alt = False

    if altitude is None or altitude == 0:
        fix_alt = False
    else:
        fix_alt = True

    for item in data:
        if fix_alt:
            baro = altitude_fix(item["Baro"], altitude)
            interm_data.append([
                item["Time"],
                baro,
                baro
            ])
        else:
            interm_data.append([
                item["Time"],
                item["Baro"],
                item["Baro"]
            ])

    return_data = sliding_average(interm_data, 1, window_size, 2)

    return return_data


def altitude_fix(relative_pressure: Decimal, altitude: int):
    approx_fix = Decimal(altitude) / Decimal(100) * Decimal(12)
    abs_pressure = relative_pressure - approx_fix
    return abs_pressure


@app.get("/api/windByTime")
async def get_wind_by_time(prior_days: Optional[int] = Query(None, alias="priorDays"),
                           prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    raw_data = await get_wind(prior_days, prior_hrs)
    data_remap = []
    window_size = 5
    if (prior_days == 0 or prior_days is None) and prior_hrs <= 3:
        window_size = 5
    else:
        if (prior_days == 0 or prior_days is None) and 2 < prior_hrs < 10:
            window_size = 15
        else:
            if prior_days > 2:
                window_size = 240
            else:
                window_size = 30
    # if prior_days == 0 or prior_days is None:
    #     prior_hrs = prior_hrs + 1
    #     cut_count = 60
    # else:
    #     prior_days = prior_days + 1
    #     cut_count = 24 * 60
    #
    # raw_more_data = await get_wind(prior_days, prior_hrs)
    # raw_more_data = raw_more_data[:cut_count]
    #speed_filter = KalmanFilter()
    #gust_filter = KalmanFilter()
    #if (prior_days == 0 or prior_days is None) and prior_hrs <= 3:
    #    speed_filter.set_q(Decimal(0.8))
    #    gust_filter.set_q(Decimal(0.8))
    #else:
    #    if (prior_days == 0 or prior_days is None) and 2 < prior_hrs < 10:
    #        speed_filter.set_default_q()
    #        gust_filter.set_default_q()
    #
    #    else:
    #        speed_filter.set_q(Decimal(0.0001))
    #        gust_filter.set_q(Decimal(0.0001))

    #speed_filter.flush_data(raw_more_data, "Speed")
    #gust_filter.flush_data(raw_more_data, "Gust")
    for data_item in raw_data:
        data_remap.append([
            data_item["Time"],
            round(data_item["Speed"] * Decimal(1.9438444924), 2),
            round((270 - data_item["Direction"]) * 3.141592654 / 180, 3),
            round(data_item["Gust"] * Decimal(1.9438444924), 2),
            round(data_item["Gust"] * Decimal(1.9438444924), 2),
            round(data_item["Speed"] * Decimal(1.9438444924), 2),
            # round(speed_filter.calc_new_data(data_item["Speed"]) * Decimal(1.9438444924), 2),
            # round((270 - data_item["Direction"]) * 3.141592654 / 180, 3),
            # round(data_item["Gust"] * Decimal(1.9438444924), 2),
            # round(gust_filter.calc_new_data(data_item["Gust"]) * Decimal(1.9438444924), 2),
            # round(data_item["Speed"] * Decimal(1.9438444924), 2),
            get_dir_from_angle(data_item["Direction"]),
            data_item["Direction"]
        ])
    data_remap = sliding_average(data_remap, 1, window_size, 2)
    data_remap = sliding_average(data_remap, 4, window_size, 2)
    return data_remap


@app.get("/api/wind/rosemap")
async def get_rosemap_item(speed_type: Optional[int] = Query(0, alias="SpeedType"),
                           prior_days: Optional[int] = Query(None, alias="priorDays"),
                           prior_hrs: Optional[int] = Query(None, alias="priorHrs")):
    raw_data = await get_wind(prior_days, prior_hrs)
    map_spd_categories = []
    real_spd_type_str = "Speed" if speed_type == 0 else "Gust"
    for serial_name in legendName:
        item = RoseMapDirItem()
        item.name = serial_name
        item.type = 'bar'
        item.coordinateSystem = 'polar'
        item.stack = 'a'
        item.data = []
        for i in range(16):
            item.data.append(0)

        map_spd_categories.append(item)

    for data_item in raw_data:
        data_dir_idx = get_dir_from_angle(data_item["Direction"])
        if data_item[real_spd_type_str] <= 0.2:
            map_spd_categories[0].data[data_dir_idx] += 1
        if 0.2 < data_item[real_spd_type_str] <= 1.5:
            map_spd_categories[1].data[data_dir_idx] += 1
        if 1.5 < data_item[real_spd_type_str] <= 3.3:
            map_spd_categories[2].data[data_dir_idx] += 1
        if 3.3 < data_item[real_spd_type_str] <= 5.4:
            map_spd_categories[3].data[data_dir_idx] += 1
        if 5.4 < data_item[real_spd_type_str] <= 7.9:
            map_spd_categories[4].data[data_dir_idx] += 1
        if 7.9 < data_item[real_spd_type_str] <= 10.7:
            map_spd_categories[5].data[data_dir_idx] += 1
        if 10.7 < data_item[real_spd_type_str] <= 13.8:
            map_spd_categories[6].data[data_dir_idx] += 1
        if 13.8 < data_item[real_spd_type_str] <= 17.1:
            map_spd_categories[7].data[data_dir_idx] += 1
        if 17.1 < data_item[real_spd_type_str] <= 20.7:
            map_spd_categories[8].data[data_dir_idx] += 1
        if 20.7 < data_item[real_spd_type_str] <= 24.4:
            map_spd_categories[9].data[data_dir_idx] += 1
        if 24.4 < data_item[real_spd_type_str] <= 28.6:
            map_spd_categories[10].data[data_dir_idx] += 1
        if 28.6 < data_item[real_spd_type_str] <= 32.6:
            map_spd_categories[11].data[data_dir_idx] += 1
        if 32.6 < data_item[real_spd_type_str]:
            map_spd_categories[12].data[data_dir_idx] += 1

    return map_spd_categories


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


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
    except:
        return 500

    return 200


if __name__ == "__main__":
    if exists("config.yaml"):
        with open("config.yaml", 'r', encoding='utf-8') as in_file:
            yaml_content = in_file.read()
            cfg_items = yaml.load(yaml_content, Loader=yaml.FullLoader)
    else:
        shutil.copy("default.yaml", "config.yaml")
        print("Please edit config.yaml to represent your current configuration!")
        print("App will now quit.")
        exit(0)

    uvicorn.run("main:app", host=cfg_items["server"]["host"],
                port=cfg_items["server"]["port"], log_level=cfg_items["server"]["log_level"])
