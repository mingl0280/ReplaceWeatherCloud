import asyncio
import unittest
from decimal import Decimal

from fastapi import HTTPException

from scripts.helper_functions import (
    get_timediff_wind_window_size,
    make_times_limited,
    process_barometer,
    process_rain_data,
    process_solar_data,
    process_temperature_units,
    process_wind_data,
    sliding_average,
)


class SmoothingTests(unittest.TestCase):
    def test_centered_average_includes_last_row_without_mutating_input(self):
        rows = [["a", Decimal("0")], ["b", Decimal("3")], ["c", Decimal("9")]]
        result = sliding_average(rows, 1, 3, 2)
        self.assertEqual([row[1] for row in result], [Decimal("1"), Decimal("4"), Decimal("7")])
        self.assertEqual([row[1] for row in rows], [Decimal("0"), Decimal("3"), Decimal("9")])
        self.assertEqual([row[0] for row in result], ["a", "b", "c"])

    def test_even_window_uses_two_previous_and_one_following(self):
        rows = [[0], [4], [12]]
        self.assertEqual(sliding_average(rows, 0, 4, 2), [[1], [4], [7]])

    def test_window_larger_than_data_and_single_row(self):
        self.assertEqual(sliding_average([[2]], 0, 60, 2), [[2]])
        self.assertEqual(sliding_average([[0], [10]], 0, 5, 2), [[4], [6]])
        self.assertEqual(sliding_average([[2], [3]], 0, 1, 2), [[2], [3]])

    def test_empty_chart_results_are_valid(self):
        self.assertEqual(process_wind_data([], 5), [])
        self.assertEqual(process_rain_data([], 60), [])
        self.assertEqual(process_solar_data([], 15), [])
        self.assertEqual(process_barometer([], 30, None), [])

    def test_invalid_window_rejected(self):
        for size in (0, -1, 2.5):
            with self.subTest(size=size), self.assertRaises(ValueError):
                sliding_average([[1]], 0, size, 2)


class ParameterTests(unittest.TestCase):
    def test_optional_wind_parameters_and_boundaries(self):
        cases = [
            (None, None, 5), (0, None, 5), (None, 1, 5),
            (None, 3, 5), (None, 4, 15), (None, 9, 15),
            (None, 10, 30), (None, 12, 30), (1, None, 30),
            (2, None, 30), (3, None, 240), (3, 1, 240),
        ]
        for days, hours, expected in cases:
            with self.subTest(days=days, hours=hours):
                self.assertEqual(asyncio.run(get_timediff_wind_window_size(days, hours)), expected)

    def test_valid_time_format_and_48_hour_limit_preserved(self):
        start = "2026-10-01 12:00:00"
        self.assertEqual(make_times_limited(start, start), (start, start))
        end = "2026-10-03 12:00:00"
        self.assertEqual(make_times_limited(start, end), (start, end))
        self.assertEqual(make_times_limited(start, "2026-10-10 12:00:00"), (start, end))

    def test_bad_or_reversed_time_returns_422(self):
        for start, end in [
            ("undefined", "undefined"),
            (None, "2026-10-01 12:00:00"),
            ("2026-10-02 12:00:00", "2026-10-01 12:00:00"),
        ]:
            with self.subTest(start=start, end=end), self.assertRaises(HTTPException) as caught:
                make_times_limited(start, end)
            self.assertEqual(caught.exception.status_code, 422)

    def test_temperature_conversion_decimal_rounding(self):
        rows = [{"Time": "now", "TempOut": Decimal("0"), "TempIn": Decimal("25.55")}]
        self.assertEqual(asyncio.run(process_temperature_units(rows, 2)),
                         [["now", Decimal("273.15"), Decimal("298.70")]])
        self.assertEqual(asyncio.run(process_temperature_units(rows, 1)),
                         [["now", Decimal("32.0"), Decimal("78.0")]])
        self.assertEqual(asyncio.run(process_temperature_units(rows, None)),
                         [["now", Decimal("0"), Decimal("25.55")]])


if __name__ == "__main__":
    unittest.main()
