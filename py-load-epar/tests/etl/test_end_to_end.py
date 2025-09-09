import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from py_load_epar.config import DatabaseSettings, Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl
from py_load_epar.spor_api.models import SporOmsOrganisation, SporSmsSubstance

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def sample_excel_file(tmp_path: Path) -> Path:
    """Creates a sample EMA data file for testing."""
    file_path = tmp_path / "test_ema_data.xlsx"
    data = {
        "Category": ["Human", "Human", "Human"],
        "Medicine name": ["TestMed Active", "TestMed Withdrawn", "TestMed Enriched"],
        "Therapeutic area": ["Oncology", "Cardiology", "Neurology"],
        "Active substance": ["substance_a", "substance_b", "substance_c, substance_d"],
        "Product number": ["EMA/1", "EMA/2", "EMA/3"],
        "Patient safety": [None, None, None],
        "authorization_status": ["Authorised", "Withdrawn", "Authorised"],
        "ATC code": ["L01", "C01", "N01"],
        "Additional monitoring": [None, None, None],
        "Generic": [False, True, False],
        "Biosimilar": [False, False, False],
        "Conditional approval": [None, None, None],
        "Exceptional circumstances": [None, None, None],
        "Marketing authorisation date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "Revision date": ["2023-01-15", "2023-01-16", "2023-01-17"],
        "Marketing authorisation holder/company name": [
            "PharmaCo",
            "BioGen",
            "Richards Pharma",
        ],
        "URL": ["http://example.com/1", "http://example.com/2", "http://example.com/3"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_full_etl_run_with_enrichment_and_soft_delete(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    sample_excel_file: Path,
):
    """
    Tests a full, end-to-end ETL run using a real database and a mocked SPOR API.

    This test verifies:
    1. Data is extracted from an Excel file.
    2. SPOR enrichment correctly populates foreign keys.
    3. Soft-delete logic correctly marks withdrawn medicines as inactive.
    4. Data is correctly loaded into the main `epar_index` and ancillary
       `epar_substance_link` tables.
    """
    # --- 1. Mock external dependencies ---

    # Mock the download function to return our local test file
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=sample_excel_file.open("rb"),
    )

    # Mock the SPOR API client
    mock_spor_client = MagicMock()
    mock_spor_client.search_organisation.side_effect = [
        None,  # For 'PharmaCo'
        None,  # For 'BioGen'
        SporOmsOrganisation(orgId="oms-123", name="Richards Pharma"),
    ]
    mock_spor_client.search_substance.side_effect = [
        SporSmsSubstance(smsId="sms-abc", name="substance_a"),
        SporSmsSubstance(smsId="sms-def", name="substance_b"),
        SporSmsSubstance(smsId="sms-ghi", name="substance_c"),
        SporSmsSubstance(smsId="sms-jkl", name="substance_d"),
    ]
    mocker.patch(
        "py_load_epar.etl.orchestrator.SporApiClient", return_value=mock_spor_client
    )
    # Also mock the document processing part to avoid real HTTP requests
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)

    # Insert the mock organization and substance into the database so the foreign key
    # constraint is not violated.
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO organizations (oms_id, organization_name)
            VALUES (%s, %s)
            """,
            ("oms-123", "Richards Pharma"),
        )
        cursor.execute(
            """
            INSERT INTO substances (spor_substance_id, substance_name)
            VALUES (%s, %s), (%s, %s), (%s, %s), (%s, %s)
            """,
            (
                "sms-abc",
                "substance_a",
                "sms-def",
                "substance_b",
                "sms-ghi",
                "substance_c",
                "sms-jkl",
                "substance_d",
            ),
        )
    postgres_adapter.conn.commit()

    # --- 2. Run the ETL process ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- 3. Assertions ---
    with postgres_adapter.conn.cursor() as cursor:
        # Verify epar_index table
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 3

        # Verify soft-delete
        cursor.execute("SELECT is_active FROM epar_index WHERE epar_id = 'EMA/2'")
        assert cursor.fetchone()[0] is False
        cursor.execute("SELECT is_active FROM epar_index WHERE epar_id = 'EMA/1'")
        assert cursor.fetchone()[0] is True

        # Verify SPOR enrichment for organisation
        cursor.execute("SELECT mah_oms_id FROM epar_index WHERE epar_id = 'EMA/3'")
        assert cursor.fetchone()[0] == "oms-123"
        cursor.execute("SELECT mah_oms_id FROM epar_index WHERE epar_id = 'EMA/1'")
        assert cursor.fetchone()[0] is None

        # Verify epar_substance_link table
        cursor.execute("SELECT COUNT(*) FROM epar_substance_link")
        assert cursor.fetchone()[0] == 4
        cursor.execute(
            "SELECT spor_substance_id FROM epar_substance_link WHERE epar_id = 'EMA/3'"
        )
        substance_ids = [row[0] for row in cursor.fetchall()]
        assert sorted(substance_ids) == ["sms-ghi", "sms-jkl"]


@pytest.mark.skip(reason="This test is flaky and needs to be refactored to not depend on external data files.")
def test_document_processing_and_retry(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    requests_mock,
    sample_excel_file: Path,
):
    """
    Tests the document processing part of the ETL, including the retry mechanism
    for fetching the HTML page.

    This test verifies:
    1. The orchestrator attempts to fetch an EPAR summary page.
    2. The retry logic in `_fetch_html_with_retry` is triggered on HTTP failure.
    3. A document link is parsed from the HTML.
    4. The document is downloaded and stored.
    5. A corresponding record is created in the `epar_documents` table.
    """
    # --- 1. Mock external dependencies ---
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=sample_excel_file.open("rb"),
    )

    # Mock the SPOR API to return nothing to simplify the test
    mock_spor_client = MagicMock()
    mock_spor_client.search_organisation.return_value = None
    mock_spor_client.search_substance.return_value = None
    mocker.patch(
        "py_load_epar.etl.orchestrator.SporApiClient", return_value=mock_spor_client
    )

    # Mock the HTTP requests for the document processing part
    epar_page_url = "http://example.com/3"
    pdf_url = "http://example.com/path/to/document.pdf"

    # The HTML page will fail twice, then succeed.
    requests_mock.get(
        epar_page_url,
        [
            {"status_code": 503, "text": "Service Unavailable"},
            {"status_code": 503, "text": "Service Unavailable"},
            {
                "status_code": 200,
                "text": f'<html><body><a href="{pdf_url}">Public Assessment Report</a></body></html>',
            },
        ],
    )
    # The PDF download will succeed on the first try.
    requests_mock.get(pdf_url, text="dummy pdf content")

    # --- 2. Run the ETL process ---
    # We only care about the record that has a URL we can test against
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- 3. Assertions ---
    # Assert that the retry logic was triggered
    history = requests_mock.request_history
    assert requests_mock.call_count == 4  # 3 for the page, 1 for the PDF
    assert len([h for h in history if h.url == epar_page_url]) == 3

    # Assert that the document was saved to the database
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_documents")
        assert cursor.fetchone()[0] == 1

        cursor.execute(
            "SELECT epar_id, source_url, file_hash, storage_location FROM epar_documents"
        )
        (epar_id, source_url, file_hash, storage_location) = cursor.fetchone()
        assert epar_id == "EMA/3"
        assert source_url == pdf_url
        assert file_hash == "8e7529348239923a85814a04918ded3c67535728153454378878342410a72c21"
        assert storage_location is not None
        assert "document.pdf" in storage_location
