import datetime
from pathlib import Path

import openpyxl
import pytest
from testcontainers.postgres import PostgresContainer

from py_load_epar.config import DatabaseSettings, Settings
from py_load_epar.db.postgres import PostgresAdapter


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
        "Category",
        "Medicine name",
        "Therapeutic area",
        "INN / common name",
        "Active substance",
        "Product number",
        "Patient safety",
        "Authorisation status",
        "ATC code",
        "Additional monitoring",
        "Generic",
        "Biosimilar",
        "Conditional marketing authorisation",
        "Exceptional circumstances",
        "Accelerated assessment",
        "Orphan medicine",
        "Marketing authorisation date",
        "Date of opinion",
        "Marketing authorisation holder/company name",
        "Pharmacotherapeutic group",
        "Date of withdrawal of marketing authorisation",
        "First published",
        "Revision date",
        "URL",
    ]
    ws.append(headers)

    # Define the data rows
    data = [
        [
            "Human",
            "TestMed1",
            "Endocrinology",
            "testmed-a",
            "Testmed-A",
            "EMA/H/C/000001",
            "",
            "Authorised",
            "A10AB01",
            "",
            "No",
            "No",
            "No",
            "No",
            "No",
            "No",
            datetime.datetime(2024, 1, 25, 0, 0),
            datetime.datetime(2023, 11, 10, 0, 0),
            "Test Pharma 1",
            "Drugs used in diabetes",
            "",
            datetime.datetime(2024, 1, 25, 0, 0),
            datetime.datetime(2024, 1, 25, 0, 0),
            "http://example.com/doc1.pdf",
        ],
        [
            "Human",
            "TestMed2",
            "Oncology",
            "testmed-b",
            "Testmed-B",
            "EMA/H/C/000002",
            "",
            "Authorised",
            "L01AX01",
            "",
            "No",
            "No",
            "No",
            "No",
            "No",
            "Yes",
            datetime.datetime(2024, 2, 15, 0, 0),
            datetime.datetime(2023, 12, 15, 0, 0),
            "Test Pharma 2",
            "Antineoplastic agents",
            "",
            datetime.datetime(2024, 2, 15, 0, 0),
            datetime.datetime(2024, 2, 15, 0, 0),
            "http://example.com/doc2.pdf",
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


@pytest.fixture(scope="module")
def postgres_container():
    """Fixture to start and stop a PostgreSQL test container."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="function")
def db_settings(postgres_container: PostgresContainer) -> Settings:
    """Fixture to create a DatabaseSettings object from the test container."""
    return Settings(
        db=DatabaseSettings(
            host=postgres_container.get_container_host_ip(),
            port=postgres_container.get_exposed_port(5432),
            user=postgres_container.username,
            password=postgres_container.password,
            dbname=postgres_container.dbname,
        )
    )


@pytest.fixture(scope="function")  # Use function scope to get a clean db for each test
def postgres_adapter(db_settings: Settings) -> PostgresAdapter:
    """
    Fixture to create a PostgresAdapter instance connected to a clean test container.
    It creates the schema and yields the adapter.
    """
    adapter = PostgresAdapter(db_settings.db)
    adapter.connect()

    # Create schema for each test function
    with open("src/py_load_epar/db/schema.sql") as f:
        with adapter.conn.cursor() as cursor:
            cursor.execute(f.read())
    adapter.conn.commit()

    yield adapter

    # Teardown: drop all tables to ensure a clean state for the next test
    with adapter.conn.cursor() as cursor:
        cursor.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    adapter.conn.commit()
    adapter.close()
