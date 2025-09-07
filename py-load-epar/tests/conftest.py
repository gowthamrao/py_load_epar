import pytest
import openpyxl
from pathlib import Path
import datetime

@pytest.fixture(scope="session", autouse=True)
def create_sample_excel_file():
    """
    A session-scoped fixture that creates the sample Excel file needed for tests.
    This runs automatically for every test session.
    """
    test_data_dir = Path(__file__).parent / "test_data"
    test_data_dir.mkdir(exist_ok=True)
    file_path = test_data_dir / "sample_ema_data.xlsx"

    # Create a new workbook and select the active sheet
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Worksheet"

    # Define the headers
    headers = [
        "Category", "Medicine name", "Therapeutic area",
        "INN / common name", "Active substance", "Product number",
        "Patient safety", "Authorisation status", "ATC code",
        "Additional monitoring", "Generic", "Biosimilar",
        "Conditional marketing authorisation", "Exceptional circumstances",
        "Accelerated assessment", "Orphan medicine",
        "Marketing authorisation date", "Date of opinion",
        "Marketing authorisation holder/company name",
        "Pharmacotherapeutic group",
        "Date of withdrawal of marketing authorisation", "First published",
        "Revision date", "URL",
    ]
    ws.append(headers)

    # Define the data rows
    data = [
        [
            "Human", "TestMed1", "Endocrinology", "testmed-a", "Testmed-A",
            "EMA/H/C/000001", "", "Authorised", "A10AB01", "", "No", "No",
            "No", "No", "No", "No", datetime.datetime(2024, 1, 25, 0, 0),
            datetime.datetime(2023, 11, 10, 0, 0), "Test Pharma 1", "Drugs used in diabetes",
            "", datetime.datetime(2024, 1, 25, 0, 0), datetime.datetime(2024, 1, 25, 0, 0), "http://example.com/doc1.pdf",
        ],
        [
            "Human", "TestMed2", "Oncology", "testmed-b", "Testmed-B",
            "EMA/H/C/000002", "", "Authorised", "L01AX01", "", "No", "No",
            "No", "No", "No", "Yes", datetime.datetime(2024, 2, 15, 0, 0),
            datetime.datetime(2023, 12, 15, 0, 0), "Test Pharma 2", "Antineoplastic agents",
            "", datetime.datetime(2024, 2, 15, 0, 0), datetime.datetime(2024, 2, 15, 0, 0), "http://example.com/doc2.pdf",
        ],
    ]
    for row in data:
        ws.append(row)

    wb.save(file_path)
    # The fixture yields control to the test session
    yield
    # Teardown: remove the file after the session
    # I will leave the file for now to check if it's created correctly
    # file_path.unlink()
    # try:
    #     test_data_dir.rmdir()
    # except OSError:
    #     pass
