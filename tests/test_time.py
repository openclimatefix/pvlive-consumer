import pytest
from freezegun import freeze_time

from pvliveconsumer.time import check_uk_london_hour


def test_time_none():
    check_uk_london_hour(hour=None)


@freeze_time("2022-01-01 10:15")
def test_time_winter():
    check_uk_london_hour(hour=10)


@freeze_time("2022-01-01 10:15")
def test_time_winter_error():
    with pytest.raises(Exception):
        check_uk_london_hour(hour=12)


@freeze_time("2022-07-01 13:30")
def test_time_summer():
    check_uk_london_hour(hour=14)


@freeze_time("2022-07-01 14:30")
def test_time_summer_error():
    with pytest.raises(Exception):
        check_uk_london_hour(hour=16)
