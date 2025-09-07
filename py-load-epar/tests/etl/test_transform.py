import logging
from typing import Any, Dict, List
from unittest.mock import MagicMock

from py_load_epar.etl.transform import transform_and_validate
from py_load_epar.models import EparIndex
from py_load_epar.spor_api.client import SporApiClient
from py_load_epar.spor_api.models import SporOmsOrganisation, SporSmsSubstance


def test_transform_and_validate_success():
    """
    Test that valid raw records are successfully transformed and validated.
    """
    # Arrange
    raw_records: List[Dict[str, Any]] = [
        {
            "medicine_name": "TestMed A",
            "marketing_authorization_holder_raw": "Pharma A",
            "authorization_status": "Authorised",
            "last_update_date_source": "2023-01-01",
        },
        {
            "medicine_name": "TestMed B",
            "marketing_authorization_holder_raw": "Pharma B",
            "authorization_status": "Authorised",
            "last_update_date_source": "2023-01-02",
            "therapeutic_area": "Testing",
        },
    ]
    mock_spor_client = MagicMock(spec=SporApiClient)
    mock_spor_client.search_organisation.return_value = None
    mock_spor_client.search_substance.return_value = None

    # Act
    results = list(transform_and_validate(iter(raw_records), mock_spor_client, 1))
    validated_models = [item[0] for item in results]

    # Assert
    assert len(validated_models) == 2
    assert isinstance(validated_models[0], EparIndex)
    assert validated_models[0].medicine_name == "TestMed A"
    assert validated_models[1].therapeutic_area == "Testing"


def test_transform_and_validate_quarantines_invalid_records(caplog):
    """
    Test that invalid records are skipped and a warning is logged.
    """
    # Arrange
    raw_records: List[Dict[str, Any]] = [
        {
            "medicine_name": "TestMed A",
            "marketing_authorization_holder_raw": "Pharma A",
            "authorization_status": "Authorised",
            "last_update_date_source": "2023-01-01",
        },
        {
            # Invalid because last_update_date_source is missing
            "medicine_name": "TestMed B",
            "marketing_authorization_holder_raw": "Pharma B",
            "authorization_status": "Authorised",
        },
        {
            "medicine_name": "TestMed C",
            "marketing_authorization_holder_raw": "Pharma C",
            "authorization_status": "Authorised",
            "last_update_date_source": "2023-01-03",
        },
    ]
    mock_spor_client = MagicMock(spec=SporApiClient)
    mock_spor_client.search_organisation.return_value = None
    mock_spor_client.search_substance.return_value = None

    # Act
    with caplog.at_level(logging.WARNING):
        results = list(transform_and_validate(iter(raw_records), mock_spor_client, 1))
    validated_models = [item[0] for item in results]

    # Assert
    assert len(validated_models) == 2
    assert validated_models[0].medicine_name == "TestMed A"
    assert validated_models[1].medicine_name == "TestMed C"

    assert len(caplog.records) == 1
    assert "failed validation" in caplog.text
    assert "'medicine_name': 'TestMed B'" in caplog.text
    assert "last_update_date_source" in caplog.text
    assert "Field required" in caplog.text


def test_transform_and_validate_enrichment():
    """
    Test that the transform function correctly calls the SPOR client and
    enriches the data.
    """
    # Arrange
    raw_records: List[Dict[str, Any]] = [
        {
            "medicine_name": "EnrichMed",
            "marketing_authorization_holder_raw": "Rich Pharma Inc.",
            "active_substance_raw": "SubstanceX, SubstanceY",
            "authorization_status": "Authorised",
            "last_update_date_source": "2023-01-01",
        }
    ]
    mock_org = SporOmsOrganisation(orgId="ORG-123", name="Rich Pharma Inc.")
    mock_substance = SporSmsSubstance(smsId="SUB-456", name="SubstanceX")

    mock_spor_client = MagicMock(spec=SporApiClient)
    mock_spor_client.search_organisation.return_value = mock_org
    mock_spor_client.search_substance.return_value = mock_substance

    # Act
    results = list(transform_and_validate(iter(raw_records), mock_spor_client, 1))
    epar_record, substance_links = results[0]

    # Assert
    # Check that the client was called with the correct parameters
    mock_spor_client.search_organisation.assert_called_once_with("Rich Pharma Inc.")
    assert mock_spor_client.search_substance.call_count == 2
    mock_spor_client.search_substance.assert_any_call("SubstanceX")
    mock_spor_client.search_substance.assert_any_call("SubstanceY")

    # Check that the EPAR record was enriched
    assert epar_record.mah_oms_id == "ORG-123"

    # Check that the substance link record was created
    assert len(substance_links) == 2  # Both calls return the same mock
    assert substance_links[0].spor_substance_id == "SUB-456"
    assert substance_links[0].epar_id == epar_record.epar_id
