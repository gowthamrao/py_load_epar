import logging
from typing import List, Dict, Any

from py_load_epar.etl.transform import transform_and_validate
from py_load_epar.models import EparIndex


def test_transform_and_validate_success():
    """
    Test that valid raw records are successfully transformed and validated.
    """
    raw_records: List[Dict[str, Any]] = [
        {
            "epar_id": "EMA/1", "medicine_name": "TestMed A", "authorization_status": "Authorised",
            "last_update_date_source": "2023-01-01"
        },
        {
            "epar_id": "EMA/2", "medicine_name": "TestMed B", "authorization_status": "Authorised",
            "last_update_date_source": "2023-01-02", "therapeutic_area": "Testing"
        },
    ]

    validated_models = list(transform_and_validate(iter(raw_records)))

    assert len(validated_models) == 2
    assert isinstance(validated_models[0], EparIndex)
    assert validated_models[0].epar_id == "EMA/1"
    assert validated_models[1].therapeutic_area == "Testing"


def test_transform_and_validate_quarantines_invalid_records(caplog):
    """
    Test that invalid records are skipped and a warning is logged.
    """
    raw_records: List[Dict[str, Any]] = [
        {
            "epar_id": "EMA/1", "medicine_name": "TestMed A", "authorization_status": "Authorised",
            "last_update_date_source": "2023-01-01"
        },
        {
            # Invalid because last_update_date_source is missing
            "epar_id": "EMA/2", "medicine_name": "TestMed B", "authorization_status": "Authorised",
        },
        {
            "epar_id": "EMA/3", "medicine_name": "TestMed C", "authorization_status": "Authorised",
            "last_update_date_source": "2023-01-03"
        },
    ]

    with caplog.at_level(logging.WARNING):
        validated_models = list(transform_and_validate(iter(raw_records)))

    # Check that only valid models are yielded
    assert len(validated_models) == 2
    assert validated_models[0].epar_id == "EMA/1"
    assert validated_models[1].epar_id == "EMA/3"

    # Check that the failure was logged
    assert len(caplog.records) == 1
    assert "failed validation" in caplog.text
    assert "Record ID: EMA/2" in caplog.text
    assert "last_update_date_source" in caplog.text # Field error
    assert "Field required" in caplog.text
