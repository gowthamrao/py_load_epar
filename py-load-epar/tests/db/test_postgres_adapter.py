import datetime
import pytest
from testcontainers.postgresql import PostgresContainer

from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.config import DatabaseSettings
from py_load_epar.models import EparIndex

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container():
    """Fixture to start and stop a PostgreSQL test container."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def db_settings(postgres_container: PostgresContainer) -> DatabaseSettings:
    """Fixture to create a DatabaseSettings object from the test container."""
    return DatabaseSettings(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.username,
        password=postgres_container.password,
        dbname=postgres_container.dbname,
    )


@pytest.fixture(scope="function") # Use function scope to get a clean db for each test
def postgres_adapter(db_settings: DatabaseSettings) -> PostgresAdapter:
    """
    Fixture to create a PostgresAdapter instance connected to a clean test container.
    It creates the schema and yields the adapter.
    """
    adapter = PostgresAdapter(db_settings)
    adapter.connect()

    # Create schema for each test function
    with open("py-load-epar/src/py_load_epar/db/schema.sql") as f:
        with adapter.conn.cursor() as cursor:
            cursor.execute(f.read())
    adapter.conn.commit()

    yield adapter

    # Teardown: close the connection
    adapter.close()


@pytest.fixture
def sample_data():
    """Fixture to provide sample EparIndex data for testing."""
    return [
        EparIndex(
            epar_id="EMA/1", medicine_name="TestMed A", authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 1)
        ),
        EparIndex(
            epar_id="EMA/2", medicine_name="TestMed B", authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 2)
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
    new_data = [EparIndex(epar_id="EMA/3", medicine_name="TestMed C", authorization_status="Withdrawn", last_update_date_source=datetime.date(2023, 2, 1))]
    postgres_adapter.prepare_load("FULL", target_table)
    postgres_adapter.bulk_load_batch(iter(new_data), target_table, EparIndex)
    postgres_adapter.finalize("FULL", target_table)

    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        assert cursor.fetchone()[0] == 1
        cursor.execute(f"SELECT medicine_name FROM {target_table} WHERE epar_id = 'EMA/3'")
        assert cursor.fetchone()[0] == "TestMed C"
        cursor.execute(f"SELECT COUNT(*) FROM {target_table} WHERE epar_id = 'EMA/1'")
        assert cursor.fetchone()[0] == 0


def test_delta_load_strategy(postgres_adapter: PostgresAdapter, sample_data):
    """Test the DELTA load strategy, which should insert new and update existing records."""
    target_table = "epar_index"

    # Initial load
    staging_table_1 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(iter(sample_data), staging_table_1, EparIndex)
    postgres_adapter.finalize("DELTA", target_table, staging_table_1, EparIndex)

    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        assert cursor.fetchone()[0] == 2

    # Delta load with one new and one updated record
    delta_data = [
        EparIndex(epar_id="EMA/2", medicine_name="TestMed B Updated", authorization_status="Authorised", last_update_date_source=datetime.date(2023, 1, 2)), # Update
        EparIndex(epar_id="EMA/3", medicine_name="TestMed C New", authorization_status="Authorised", last_update_date_source=datetime.date(2023, 2, 1)),     # Insert
    ]
    staging_table_2 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(iter(delta_data), staging_table_2, EparIndex)
    postgres_adapter.finalize("DELTA", target_table, staging_table_2, EparIndex)

    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        assert cursor.fetchone()[0] == 3 # 2 initial + 1 new
        cursor.execute(f"SELECT medicine_name FROM {target_table} WHERE epar_id = 'EMA/2'")
        assert cursor.fetchone()[0] == "TestMed B Updated"
        cursor.execute(f"SELECT medicine_name FROM {target_table} WHERE epar_id = 'EMA/3'")
        assert cursor.fetchone()[0] == "TestMed C New"

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
