import datetime
from unittest.mock import patch

import pytest

from py_load_epar.config import Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl
from py_load_epar.spor_api.models import SporOmsOrganisation, SporSmsSubstance

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def test_etl_populates_master_data(
    db_settings: Settings, postgres_adapter: PostgresAdapter
):
    """
    Tests the end-to-end ETL flow, verifying that master data tables
    (organizations, substances) are correctly populated before the main
    epar_index table, ensuring foreign key constraints are met.
    """
    # 1. Arrange: Mock settings and external dependencies
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    settings.etl.batch_size = 10  # Keep batch size reasonable for testing

    # Mock data that will be "returned" by the extractor
    mock_raw_data = [
        {
            "product_number": "TEST/001",
            "medicine_name": "Test Med 1",
            "marketing_authorization_holder_raw": "Test Pharma Inc.",
            "active_substance_raw": "Substance A, Substance B",
            "last_update_date_source": datetime.date(2024, 1, 1),
            "authorization_status": "Authorised",
            "source_url": "http://fake-url.com/1",
            "therapeutic_area": "Testing",
        },
        {
            "product_number": "TEST/002",
            "medicine_name": "Test Med 2",
            "marketing_authorization_holder_raw": "Another Corp.",
            "active_substance_raw": "Substance C",
            "last_update_date_source": datetime.date(2024, 1, 2),
            "authorization_status": "Authorised",
            "source_url": "http://fake-url.com/2",
            "therapeutic_area": "Testing",
        },
        {
            "product_number": "TEST/003",
            "medicine_name": "Test Med 3",
            "marketing_authorization_holder_raw": "Test Pharma Inc.", # Same org as #1
            "active_substance_raw": "Substance D",
            "last_update_date_source": datetime.date(2024, 1, 3),
            "authorization_status": "Authorised",
            "source_url": "http://fake-url.com/3",
            "therapeutic_area": "Testing",
        },
    ]

    # Mock responses that will be "returned" by the SPOR API client
    mock_org_1 = SporOmsOrganisation(orgId="OMS-12345", name="Test Pharma Inc.")
    mock_org_2 = SporOmsOrganisation(orgId="OMS-67890", name="Another Corp.")
    mock_sub_a = SporSmsSubstance(smsId="SMS-A", name="Substance A")
    mock_sub_b = SporSmsSubstance(smsId="SMS-B", name="Substance B")
    mock_sub_c = SporSmsSubstance(smsId="SMS-C", name="Substance C")
    mock_sub_d = SporSmsSubstance(smsId="SMS-D", name="Substance D")

    # Use a dictionary to mock the search function's behavior
    org_search_map = {
        "Test Pharma Inc.": mock_org_1,
        "Another Corp.": mock_org_2,
    }
    substance_search_map = {
        "Substance A": mock_sub_a,
        "Substance B": mock_sub_b,
        "Substance C": mock_sub_c,
        "Substance D": mock_sub_d,
    }

    # Patch the external dependencies
    with patch(
        "py_load_epar.etl.orchestrator.extract_data", return_value=iter(mock_raw_data)
    ) as mock_extract, patch(
        "py_load_epar.etl.orchestrator.SporApiClient"
    ) as mock_spor_client_class, patch(
        "py_load_epar.etl.orchestrator._process_documents", return_value=0
    ) as mock_process_docs:

        # Configure the mock SPOR client instance
        mock_spor_client = mock_spor_client_class.return_value
        mock_spor_client.search_organisation.side_effect = (
            lambda name: org_search_map.get(name)
        )
        mock_spor_client.search_substance.side_effect = (
            lambda name: substance_search_map.get(name)
        )

        # 2. Act: Run the main ETL function
        run_etl(settings)

        # 3. Assert: Verify the state of the database
        mock_extract.assert_called_once()
        mock_process_docs.assert_called() # Check that it was called, even if it did nothing

        with postgres_adapter.conn.cursor() as cursor:
            # Assert organizations table
            cursor.execute("SELECT oms_id, organization_name FROM organizations ORDER BY oms_id")
            org_results = cursor.fetchall()
            assert org_results == [
                ("OMS-12345", "Test Pharma Inc."),
                ("OMS-67890", "Another Corp."),
            ]

            # Assert substances table
            cursor.execute("SELECT spor_substance_id, substance_name FROM substances ORDER BY spor_substance_id")
            sub_results = cursor.fetchall()
            assert sub_results == [
                ("SMS-A", "Substance A"),
                ("SMS-B", "Substance B"),
                ("SMS-C", "Substance C"),
                ("SMS-D", "Substance D"),
            ]

            # Assert epar_index table and foreign keys
            cursor.execute("SELECT COUNT(*) FROM epar_index")
            assert cursor.fetchone()[0] == 3
            cursor.execute("SELECT epar_id, mah_oms_id FROM epar_index ORDER BY epar_id")
            fk_results = cursor.fetchall()
            assert fk_results == [
                ("TEST/001", "OMS-12345"),
                ("TEST/002", "OMS-67890"),
                ("TEST/003", "OMS-12345"),
            ]

            # Assert link table
            cursor.execute("SELECT COUNT(*) FROM epar_substance_link")
            assert cursor.fetchone()[0] == 4
            cursor.execute("SELECT epar_id, spor_substance_id FROM epar_substance_link ORDER BY epar_id, spor_substance_id")
            link_results = cursor.fetchall()
            assert link_results == [
                ("TEST/001", "SMS-A"),
                ("TEST/001", "SMS-B"),
                ("TEST/002", "SMS-C"),
                ("TEST/003", "SMS-D"),
            ]

            # Assert pipeline execution log
            cursor.execute("SELECT status, records_processed FROM pipeline_execution")
            exec_log = cursor.fetchone()
            assert exec_log[0] == "SUCCESS"
            assert exec_log[1] == 3
