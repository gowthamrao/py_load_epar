import datetime
from unittest.mock import MagicMock

from py_load_epar.etl.extract import extract_data


def test_extract_data_returns_all_records_without_hwm():
    """
    Test that extract_data yields all mock records when no high_water_mark is provided.
    """
    settings = MagicMock()
    records = list(extract_data(settings, high_water_mark=None))
    assert len(records) == 3
    assert records[0]["epar_id"] == "EMA/123456"


def test_extract_data_filters_based_on_hwm():
    """
    Test that extract_data correctly filters records based on the high_water_mark.
    """
    settings = MagicMock()
    # This HWM should filter out the first record (2023-05-20)
    hwm = datetime.date(2023, 5, 20)

    records = list(extract_data(settings, high_water_mark=hwm))

    # Only the records with a later date should be returned
    assert len(records) == 2
    assert records[0]["epar_id"] == "EMA/789012"
    assert records[1]["epar_id"] == "EMA/345678"


def test_extract_data_with_hwm_that_filters_all():
    """
    Test that extract_data returns no records if the HWM is after all record dates.
    """
    settings = MagicMock()
    hwm = datetime.date(2024, 1, 1)

    records = list(extract_data(settings, high_water_mark=hwm))

    assert len(records) == 0
