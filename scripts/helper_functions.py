from datetime import datetime as dt, timedelta
from decimal import Decimal

from fastapi import HTTPException

from scripts.configs import legendName
from scripts.RoseMapDirItem import RoseMapDirItem


def sliding_average(data, col_num, window_size, round_digits):
    """Smooth every row, repeating endpoint values outside the data range.

    Even windows contain one more preceding than following value. Return
    copied rows so callers can retain their raw observations.
    """
    if not isinstance(window_size, int) or window_size < 1:
        raise ValueError("window_size must be a positive integer")
    if not data:
        return []
    left = window_size // 2
    right = window_size - left - 1
    values = [row[col_num] for row in data]
    padded = [values[0]] * left + values + [values[-1]] * right
    total = sum(padded[:window_size])
    result = []
    for index, row in enumerate(data):
        if index:
            total += padded[index + window_size - 1] - padded[index - 1]
        smoothed = list(row)
        smoothed[col_num] = round(total / window_size, round_digits)
        result.append(smoothed)
    return result


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


def process_wind_data(raw_data, window_size: int):
    data_remap = []
    for data_item in raw_data:
        data_remap.append([
            data_item["Time"],
            round(data_item["Speed"] * Decimal("1.9438444924"), 2),
            round((270 - data_item["Direction"]) * 3.141592654 / 180, 3),
            round(data_item["Gust"] * Decimal("1.9438444924"), 2),
            round(data_item["Gust"] * Decimal("1.9438444924"), 2),
            round(data_item["Speed"] * Decimal("1.9438444924"), 2),
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


def process_solar_data(raw_data, sliding_window) -> list:
    return_data = []
    for item in raw_data:
        return_data.append([
            item["Time"],
            item["Solar"],
            item["Solar"]
        ])

    return sliding_average(return_data, 2, sliding_window, 1)


def process_rain_data(raw_data, sliding_window) -> list:
    return_data = []
    for item in raw_data:
        return_data.append([
            item["Time"],
            item["Rain"],
            item["Rain"]
        ])

    return sliding_average(return_data, 2, sliding_window, 2)


def process_barometer(raw_data, window_size, altitude):
    interm_data = []
    fix_alt = False

    if altitude is None or altitude == 0:
        pass
    else:
        fix_alt = True

    for item in raw_data:
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


async def get_timediff_wind_window_size(prior_days, prior_hrs):
    if prior_days:
        return 240 if prior_days > 2 else 30
    if prior_hrs is None or prior_hrs <= 3:
        return 5
    return 15 if prior_hrs < 10 else 30


def process_rose_map(raw_data, speed_type):
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


def make_times_limited(start_timestamp, end_timestamp):
    try:
        start_time_obj = dt.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
        end_time_obj = dt.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=422, detail="startTime and endTime must use YYYY-MM-DD HH:MM:SS"
        ) from exc
    if end_time_obj < start_time_obj:
        raise HTTPException(status_code=422, detail="endTime must not precede startTime")
    time_diff = end_time_obj - start_time_obj
    if time_diff > timedelta(hours=48):
        end_time_obj = start_time_obj + timedelta(hours=48)
        end_timestamp = end_time_obj.strftime('%Y-%m-%d %H:%M:%S')

    return start_timestamp, end_timestamp


async def process_temperature_units(raw_data, unit):
    return_data = []
    # Ax+B conversion
    conversion_A = Decimal("1")
    conversion_B = Decimal("0")
    if unit == 1:
        conversion_A = Decimal("1.8")
        conversion_B = Decimal("32")
    if unit == 2:
        conversion_B = Decimal("273.15")
    for item in raw_data:
        return_data.append([
            item["Time"],
            round(item["TempOut"] * conversion_A + conversion_B, 2 if conversion_A == 1 else 1),
            round(item['TempIn'] * conversion_A + conversion_B, 2 if conversion_A == 1 else 1)
        ])
    return return_data
