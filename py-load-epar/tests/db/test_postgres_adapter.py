import datetime
from unittest.mock import patch

import pytest
from pydantic import BaseModel
from testcontainers.postgres import PostgresContainer

from py_load_epar.config import Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl
from py_load_epar.models import EparIndex

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


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


@pytest.fixture
def sample_data():
    """Fixture to provide sample EparIndex data for testing."""
    return [
        EparIndex(
            epar_id="EMA/1",
            medicine_name="TestMed A",
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 1),
        ),
        EparIndex(
            epar_id="EMA/2",
            medicine_name="TestMed B",
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 2),
        ),
    ]


def test_connection(postgres_adapter: PostgresAdapter):
    """Test that the adapter can connect to the database."""
    assert postgres_adapter.conn is not None
    assert not postgres_adapter.conn.closed


def test_full_load_strategy(postgres_adapter: PostgresAdapter, sample_data):
    """Test the FULL load strategy, which should truncate and reload."""
    target_table = "epar_index"

    # First load
    staging_table = postgres_adapter.prepare_load("FULL", target_table)
    assert staging_table == target_table
    postgres_adapter.bulk_load_batch(iter(sample_data), staging_table, EparIndex)
    postgres_adapter.finalize("FULL", target_table)

    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        assert cursor.fetchone()[0] == 2

    # Second load with different data
    new_data = [
        EparIndex(
            epar_id="EMA/3",
            medicine_name="TestMed C",
            authorization_status="Withdrawn",
            last_update_date_source=datetime.date(2023, 2, 1),
        )
    ]
    postgres_adapter.prepare_load("FULL", target_table)
    postgres_adapter.bulk_load_batch(iter(new_data), target_table, EparIndex)
    postgres_adapter.finalize("FULL", target_table)

    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        assert cursor.fetchone()[0] == 1
        cursor.execute(
            f"SELECT medicine_name FROM {target_table} WHERE epar_id = 'EMA/3'"
        )
        assert cursor.fetchone()[0] == "TestMed C"
        cursor.execute(f"SELECT COUNT(*) FROM {target_table} WHERE epar_id = 'EMA/1'")
        assert cursor.fetchone()[0] == 0


def test_delta_load_strategy(postgres_adapter: PostgresAdapter, sample_data):
    """
    Test the DELTA load strategy, which should insert new and update existing
    records.
    """
    target_table = "epar_index"

    # Initial load
    staging_table_1 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(iter(sample_data), staging_table_1, EparIndex)
    postgres_adapter.finalize(
        "DELTA",
        target_table,
        staging_table_1,
        EparIndex,
        primary_key_columns=["epar_id"],
    )

    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        assert cursor.fetchone()[0] == 2

    # Delta load with one new and one updated record
    delta_data = [
        EparIndex(
            epar_id="EMA/2",
            medicine_name="TestMed B Updated",
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 2),
        ),  # Update
        EparIndex(
            epar_id="EMA/3",
            medicine_name="TestMed C New",
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 2, 1),
        ),  # Insert
    ]
    staging_table_2 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(iter(delta_data), staging_table_2, EparIndex)
    postgres_adapter.finalize(
        "DELTA",
        target_table,
        staging_table_2,
        EparIndex,
        primary_key_columns=["epar_id"],
    )

    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        assert cursor.fetchone()[0] == 3  # 2 initial + 1 new
        cursor.execute(
            f"SELECT medicine_name FROM {target_table} WHERE epar_id = 'EMA/2'"
        )
        assert cursor.fetchone()[0] == "TestMed B Updated"
        cursor.execute(
            f"SELECT medicine_name FROM {target_table} WHERE epar_id = 'EMA/3'"
        )
        assert cursor.fetchone()[0] == "TestMed C New"


# Define a model where the PK is not the first column
class NonStandardPkModel(BaseModel):
    some_data: str
    item_id: str  # Primary Key
    more_data: int


def test_delta_load_with_non_standard_pk(postgres_adapter: PostgresAdapter):
    """
    Test that DELTA load works correctly when the primary key is not the first
    column in the model, verifying the fix for the hardcoded PK assumption.
    """
    target_table = "non_standard_pk_table"
    pk_columns = ["item_id"]

    # Create the test table manually
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE {target_table} (
                some_data VARCHAR(50),
                item_id VARCHAR(50) PRIMARY KEY,
                more_data INTEGER
            );
            """
        )
    postgres_adapter.conn.commit()

    # Initial data load
    initial_data = [
        NonStandardPkModel(some_data="A", item_id="pk1", more_data=100),
        NonStandardPkModel(some_data="B", item_id="pk2", more_data=200),
    ]
    staging_1 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(iter(initial_data), staging_1, NonStandardPkModel)
    postgres_adapter.finalize(
        "DELTA", target_table, staging_1, NonStandardPkModel, pk_columns
    )

    # Delta load with an update and an insert
    delta_data = [
        NonStandardPkModel(some_data="B_updated", item_id="pk2", more_data=250),
        NonStandardPkModel(some_data="C", item_id="pk3", more_data=300),
    ]
    staging_2 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(iter(delta_data), staging_2, NonStandardPkModel)
    postgres_adapter.finalize(
        "DELTA", target_table, staging_2, NonStandardPkModel, pk_columns
    )

    # Assertions
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        assert cursor.fetchone()[0] == 3

        cursor.execute(f"SELECT some_data, more_data FROM {target_table} WHERE item_id = 'pk2'")
        assert cursor.fetchone() == ("B_updated", 250)

        cursor.execute(f"SELECT some_data, more_data FROM {target_table} WHERE item_id = 'pk3'")
        assert cursor.fetchone() == ("C", 300)


def test_rollback_on_failure(postgres_adapter: PostgresAdapter, sample_data):
    """Test that the transaction is rolled back if finalize is not called."""
    target_table = "epar_index"

    staging_table = postgres_adapter.prepare_load("FULL", target_table)
    postgres_adapter.bulk_load_batch(iter(sample_data), staging_table, EparIndex)

    # Instead of finalizing, we roll back
    postgres_adapter.rollback()

    # Re-establish connection to check state, as rollback might close it
    postgres_adapter.connect()
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        assert cursor.fetchone()[0] == 0


def test_cdc_delta_load_scenario(db_settings: Settings, postgres_adapter: PostgresAdapter):
    """
    Tests the end-to-end CDC (Change Data Capture) logic over multiple runs.
    """
    # Mock settings for the test
    settings = db_settings
    settings.etl.load_strategy = "DELTA"

    # --- RUN 1: Initial load ---
    run1_data = [
        {
            "medicine_name": "TestMed A",
            "marketing_authorization_holder_raw": "Pharma Inc.",
            "last_update_date_source": datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "medicine_name": "TestMed B",
            "marketing_authorization_holder_raw": "Pharma Inc.",
            "last_update_date_source": datetime.datetime(2023, 1, 2, 12, 0, 0, tzinfo=datetime.timezone.utc),
        },
    ]
    run1_hwm = datetime.datetime(2023, 1, 2, 12, 0, 0, tzinfo=datetime.timezone.utc)

    # We patch the extract and SPOR client to isolate the test to the orchestration and DB logic
    with patch("py_load_epar.etl.orchestrator.extract_data", return_value=(iter(run1_data), run1_hwm)), \
         patch("py_load_epar.etl.orchestrator.SporApiClient"):
        run_etl(settings)

    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 2
        cursor.execute("SELECT MAX(high_water_mark) FROM pipeline_execution WHERE status = 'SUCCESS'")
        assert cursor.fetchone()[0] == run1_hwm

    # --- RUN 2: New and updated records ---
    run2_data = [
        {
            "medicine_name": "TestMed B",  # Same as before, should be updated
            "marketing_authorization_holder_raw": "Pharma Inc.",
            "last_update_date_source": datetime.datetime(2023, 1, 3, 12, 0, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "medicine_name": "TestMed C",  # New record
            "marketing_authorization_holder_raw": "Pharma Inc.",
            "last_update_date_source": datetime.datetime(2023, 1, 4, 12, 0, 0, tzinfo=datetime.timezone.utc),
        },
    ]
    run2_hwm = datetime.datetime(2023, 1, 4, 12, 0, 0, tzinfo=datetime.timezone.utc)

    with patch("py_load_epar.etl.orchestrator.extract_data", return_value=(iter(run2_data), run2_hwm)), \
         patch("py_load_epar.etl.orchestrator.SporApiClient"):
        run_etl(settings)

    with postgres_adapter.conn.cursor() as cursor:
        # Should have 3 records now (2 from run 1, 1 new from run 2)
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 3
        # The high water mark for the latest successful run should be updated
        cursor.execute("SELECT MAX(high_water_mark) FROM pipeline_execution WHERE status = 'SUCCESS'")
        assert cursor.fetchone()[0] == run2_hwm

    # --- RUN 3: No new records ---
    # The extract function will be filtered by the HWM from run 2, so it should yield no data.
    with patch("py_load_epar.etl.orchestrator.extract_data", return_value=(iter([]), run2_hwm)), \
         patch("py_load_epar.etl.orchestrator.SporApiClient"):
        run_etl(settings)

    with postgres_adapter.conn.cursor() as cursor:
        # Count should still be 3
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 3
        # HWM should remain the same as the latest data seen
        cursor.execute("SELECT MAX(high_water_mark) FROM pipeline_execution WHERE status = 'SUCCESS'")
        assert cursor.fetchone()[0] == run2_hwm
        # Check there are two successful executions logged
        cursor.execute("SELECT COUNT(*) FROM pipeline_execution WHERE status = 'SUCCESS'")
        assert cursor.fetchone()[0] == 3
