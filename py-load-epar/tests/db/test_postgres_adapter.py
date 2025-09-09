import datetime
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from py_load_epar.config import DatabaseSettings, Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl
from py_load_epar.models import EparIndex

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


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
    model = EparIndex
    columns = list(model.model_fields.keys())

    # First load
    data_iterator_1 = (
        tuple(record.model_dump(include=columns).values()) for record in sample_data
    )
    staging_table = postgres_adapter.prepare_load("FULL", target_table)
    assert staging_table == target_table
    postgres_adapter.bulk_load_batch(data_iterator_1, staging_table, columns)
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
    data_iterator_2 = (
        tuple(record.model_dump(include=columns).values()) for record in new_data
    )
    postgres_adapter.prepare_load("FULL", target_table)
    postgres_adapter.bulk_load_batch(data_iterator_2, target_table, columns)
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
    model = EparIndex
    columns = list(model.model_fields.keys())

    # Initial load
    data_iterator_1 = (
        tuple(record.model_dump(include=columns).values()) for record in sample_data
    )
    staging_table_1 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(data_iterator_1, staging_table_1, columns)
    postgres_adapter.finalize(
        "DELTA",
        target_table,
        staging_table_1,
        model,
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
    data_iterator_2 = (
        tuple(record.model_dump(include=columns).values()) for record in delta_data
    )
    staging_table_2 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(data_iterator_2, staging_table_2, columns)
    postgres_adapter.finalize(
        "DELTA",
        target_table,
        staging_table_2,
        model,
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


def test_delta_load_soft_delete(postgres_adapter: PostgresAdapter):
    """
    Test that the DELTA load strategy correctly soft-deletes records that
    are no longer present in the source data.
    """
    target_table = "epar_index"
    model = EparIndex
    columns = list(model.model_fields.keys())
    pk_columns = ["epar_id"]

    # Initial load of 3 records
    initial_data = [
        EparIndex(
            epar_id="EMA/1",
            medicine_name="Active Med A",
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 1),
        ),
        EparIndex(
            epar_id="EMA/2",
            medicine_name="Active Med B",
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 2),
        ),
        EparIndex(
            epar_id="EMA/3",
            medicine_name="To be Deactivated",
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 3),
        ),
    ]
    data_iterator_1 = (
        tuple(record.model_dump(include=columns).values()) for record in initial_data
    )
    staging_1 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(data_iterator_1, staging_1, columns)
    postgres_adapter.finalize("DELTA", target_table, staging_1, model, pk_columns)

    # Verify initial state
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table} WHERE is_active = TRUE")
        assert cursor.fetchone()[0] == 3

    # Delta load where EMA/3 is now missing
    delta_data = [
        EparIndex(
            epar_id="EMA/1",
            medicine_name="Active Med A",
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 1),
        ),
        EparIndex(
            epar_id="EMA/2",
            medicine_name="Active Med B Updated", # Update this one
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 4),
        ),
        EparIndex(
            epar_id="EMA/4", # Add a new one
            medicine_name="New Med D",
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 5),
        ),
    ]
    data_iterator_2 = (
        tuple(record.model_dump(include=columns).values()) for record in delta_data
    )
    staging_2 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(data_iterator_2, staging_2, columns)
    postgres_adapter.finalize("DELTA", target_table, staging_2, model, pk_columns)

    # Verify final state
    with postgres_adapter.conn.cursor() as cursor:
        # Check counts
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        assert cursor.fetchone()[0] == 4 # 3 initial + 1 new
        cursor.execute(f"SELECT COUNT(*) FROM {target_table} WHERE is_active = TRUE")
        assert cursor.fetchone()[0] == 3 # EMA/1, EMA/2, EMA/4

        # Check soft-deleted record
        cursor.execute(f"SELECT is_active FROM {target_table} WHERE epar_id = 'EMA/3'")
        assert cursor.fetchone()[0] is False

        # Check updated record
        cursor.execute(f"SELECT medicine_name FROM {target_table} WHERE epar_id = 'EMA/2'")
        assert cursor.fetchone()[0] == "Active Med B Updated"

        # Check untouched record
        cursor.execute(f"SELECT is_active FROM {target_table} WHERE epar_id = 'EMA/1'")
        assert cursor.fetchone()[0] is True

        # Check new record
        cursor.execute(f"SELECT is_active FROM {target_table} WHERE epar_id = 'EMA/4'")
        assert cursor.fetchone()[0] is True


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
    model = NonStandardPkModel
    columns = list(model.model_fields.keys())
    data_iterator_1 = (
        tuple(record.model_dump(include=columns).values()) for record in initial_data
    )
    staging_1 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(data_iterator_1, staging_1, columns)
    postgres_adapter.finalize(
        "DELTA", target_table, staging_1, model, pk_columns
    )

    # Delta load with an update and an insert
    delta_data = [
        NonStandardPkModel(some_data="B_updated", item_id="pk2", more_data=250),
        NonStandardPkModel(some_data="C", item_id="pk3", more_data=300),
    ]
    data_iterator_2 = (
        tuple(record.model_dump(include=columns).values()) for record in delta_data
    )
    staging_2 = postgres_adapter.prepare_load("DELTA", target_table)
    postgres_adapter.bulk_load_batch(data_iterator_2, staging_2, columns)
    postgres_adapter.finalize(
        "DELTA", target_table, staging_2, model, pk_columns
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
    model = EparIndex
    columns = list(model.model_fields.keys())
    data_iterator = (
        tuple(record.model_dump(include=columns).values()) for record in sample_data
    )

    staging_table = postgres_adapter.prepare_load("FULL", target_table)
    postgres_adapter.bulk_load_batch(data_iterator, staging_table, columns)

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
            "product_number": "EMA/1",
            "medicine_name": "TestMed A",
            "marketing_authorization_holder_raw": "Pharma Inc.",
            "last_update_date_source": datetime.date(2023, 1, 1),
            "authorization_status": "Authorised",
        },
        {
            "product_number": "EMA/2",
            "medicine_name": "TestMed B",
            "marketing_authorization_holder_raw": "Pharma Inc.",
            "last_update_date_source": datetime.date(2023, 1, 2),
            "authorization_status": "Authorised",
        },
    ]
    run1_hwm = datetime.datetime(2023, 1, 2, 12, 0, 0, tzinfo=datetime.timezone.utc)

    # We patch the extract and SPOR client to isolate the test to the orchestration and DB logic
    with patch("py_load_epar.etl.orchestrator.extract_data", return_value=iter(run1_data)), patch(
        "py_load_epar.etl.orchestrator.SporApiClient"
    ) as mock_spor_client_class:
        mock_spor_client_instance = mock_spor_client_class.return_value
        mock_spor_client_instance.search_organisation.return_value = None
        mock_spor_client_instance.search_substance.return_value = None
        run_etl(settings)

    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 2
        cursor.execute("SELECT MAX(high_water_mark) FROM pipeline_execution WHERE status = 'SUCCESS'")
        assert cursor.fetchone()[0].date() == run1_hwm.date()

    # --- RUN 2: New and updated records ---
    run2_data = [
        {
            "product_number": "EMA/2",
            "medicine_name": "TestMed B",  # Same as before, should be updated
            "marketing_authorization_holder_raw": "Pharma Inc.",
            "last_update_date_source": datetime.date(2023, 1, 3),
            "authorization_status": "Authorised",
        },
        {
            "product_number": "EMA/3",
            "medicine_name": "TestMed C",  # New record
            "marketing_authorization_holder_raw": "Pharma Inc.",
            "last_update_date_source": datetime.date(2023, 1, 4),
            "authorization_status": "Authorised",
        },
    ]
    run2_hwm = datetime.datetime(2023, 1, 4, 12, 0, 0, tzinfo=datetime.timezone.utc)

    with patch("py_load_epar.etl.orchestrator.extract_data", return_value=iter(run2_data)), patch(
        "py_load_epar.etl.orchestrator.SporApiClient"
    ) as mock_spor_client_class:
        mock_spor_client_instance = mock_spor_client_class.return_value
        mock_spor_client_instance.search_organisation.return_value = None
        mock_spor_client_instance.search_substance.return_value = None
        run_etl(settings)

    with postgres_adapter.conn.cursor() as cursor:
        # Should have 3 records now (2 from run 1, 1 new from run 2)
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 3
        # The high water mark for the latest successful run should be updated
        cursor.execute("SELECT MAX(high_water_mark) FROM pipeline_execution WHERE status = 'SUCCESS'")
        assert cursor.fetchone()[0].date() == run2_hwm.date()

    # --- RUN 3: No new records ---
    # The extract function will be filtered by the HWM from run 2, so it should yield no data.
    with patch("py_load_epar.etl.orchestrator.extract_data", return_value=iter([])), patch(
        "py_load_epar.etl.orchestrator.SporApiClient"
    ):
        run_etl(settings)

    with postgres_adapter.conn.cursor() as cursor:
        # Count should still be 3
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 3
        # HWM should remain the same as the latest data seen
        cursor.execute("SELECT MAX(high_water_mark) FROM pipeline_execution WHERE status = 'SUCCESS'")
        assert cursor.fetchone()[0].date() == run2_hwm.date()
        # Check there are two successful executions logged
        cursor.execute("SELECT COUNT(*) FROM pipeline_execution WHERE status = 'SUCCESS'")
        assert cursor.fetchone()[0] == 3
