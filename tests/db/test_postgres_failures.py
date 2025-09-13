import pytest
from unittest.mock import MagicMock, patch
from py_load_epar.db.postgres import PostgresAdapter, StreamingIteratorIO
from py_load_epar.config import DatabaseSettings
import io

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def disconnected_adapter(db_settings: DatabaseSettings) -> PostgresAdapter:
    """Provides a PostgresAdapter instance that has not been connected."""
    # We need to access the 'db' attribute of the main settings object
    return PostgresAdapter(db_settings.db)


def test_connect_with_extra_params(postgres_adapter: PostgresAdapter):
    """
    Tests that the connect method can accept and use additional connection parameters,
    covering the `if connection_params:` block.
    """
    # The main postgres_adapter fixture already connects. We can close it and reconnect.
    postgres_adapter.close()
    assert postgres_adapter.conn is None or postgres_adapter.conn.closed != 0

    # Reconnect with an extra, valid parameter (e.g., connect_timeout)
    postgres_adapter.connect(connection_params={"connect_timeout": 10})
    assert postgres_adapter.conn is not None
    assert not postgres_adapter.conn.closed
    # We can't easily assert the timeout was used without a slow server,
    # but successful connection confirms the code path was taken without error.


def test_prepare_load_with_invalid_strategy(postgres_adapter: PostgresAdapter):
    """
    Tests that prepare_load raises a ValueError for an unknown load strategy.
    """
    with pytest.raises(ValueError, match="Unknown load strategy: INVALID"):
        postgres_adapter.prepare_load("INVALID", "any_table")


@pytest.mark.parametrize(
    "method_name,method_args",
    [
        ("prepare_load", ("FULL", "a_table")),
        ("bulk_load_batch", (iter([]), "a_table", ["col1"])),
        ("finalize", ("FULL", "a_table")),
        ("get_latest_high_water_mark", ()),
        ("log_pipeline_start", ("DELTA",)),
        ("log_pipeline_success", (1, 100)),
        ("log_pipeline_failure", (1,)),
    ],
)
def test_methods_raise_connection_error_when_not_connected(
    disconnected_adapter: PostgresAdapter, method_name: str, method_args: tuple
):
    """
    Tests that key methods raise ConnectionError if the adapter is not connected.
    """
    method_to_test = getattr(disconnected_adapter, method_name)
    with pytest.raises(ConnectionError, match="Database connection is not established"):
        method_to_test(*method_args)


def test_finalize_delta_with_missing_params(postgres_adapter: PostgresAdapter):
    """
    Tests that finalize raises a ValueError if required params for DELTA are missing.
    """
    with pytest.raises(
        ValueError,
        match="For 'DELTA' strategy, 'staging_table', 'pydantic_model', and 'primary_key_columns' must be provided.",
    ):
        postgres_adapter.finalize(load_strategy="DELTA", target_table="any_table")


def test_soft_delete_with_incomplete_settings(postgres_adapter: PostgresAdapter, caplog):
    """
    Tests that _perform_soft_delete logs a warning if settings are incomplete.
    """
    mock_cursor = MagicMock()
    # Call the protected method directly for this unit-style test
    postgres_adapter._perform_soft_delete(
        cursor=mock_cursor,
        target_table="t",
        staging_table="s",
        primary_key_columns=["id"],
        settings={"column": "is_active"},  # Incomplete settings
    )
    assert "Soft delete settings are incomplete. Skipping." in caplog.text
    mock_cursor.execute.assert_not_called()


def test_streaming_iterator_read_all():
    """
    Tests the read(size=-1) case in the StreamingIteratorIO helper class.
    """
    data = [b"hello", b"world", b"this is a test"]
    iterator = iter(data)
    stream = StreamingIteratorIO(iterator)

    result = stream.read(-1)

    assert result == b"helloworldthis is a test"
    # After reading all, the internal buffer should be empty
    assert stream.read() == b""
