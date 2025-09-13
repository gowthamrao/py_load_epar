import datetime
from unittest.mock import MagicMock, patch

import pytest
import psycopg2
from psycopg2.extensions import connection as PgConnection

from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.models import EparIndex
from py_load_epar.config import DatabaseSettings

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class MockPostgresAdapter(PostgresAdapter):
    def __init__(self, settings: DatabaseSettings, mock_connection: PgConnection):
        super().__init__(settings)
        self._mock_connection = mock_connection

    def _get_connection(self, **kwargs) -> PgConnection:
        return self._mock_connection


def test_bulk_load_rollback_on_connection_error(db_settings):
    """
    Test that the transaction is rolled back correctly if the database
    connection is lost during a bulk load operation.
    """
    # Create a mock connection and cursor
    mock_cursor = MagicMock()
    mock_cursor.copy_expert.side_effect = psycopg2.OperationalError(
        "Simulated connection failure"
    )

    mock_connection = MagicMock(spec=PgConnection)
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Use the mock adapter
    adapter = MockPostgresAdapter(db_settings.db, mock_connection)
    adapter.conn = mock_connection  # Manually set the connection

    target_table = "epar_index"
    model = EparIndex
    columns = list(model.model_fields.keys())

    sample_data = [
        EparIndex(
            epar_id="EMA/1",
            medicine_name="TestMed A",
            authorization_status="Authorised",
            last_update_date_source=datetime.date(2023, 1, 1),
            therapeutic_area="Testing",
        ),
    ]
    data_iterator = (
        tuple(record.model_dump(include=columns).values()) for record in sample_data
    )

    # This should raise the OperationalError
    with pytest.raises(psycopg2.OperationalError):
        adapter.bulk_load_batch(data_iterator, target_table, columns)

    # Assert that rollback was called on the connection
    mock_connection.rollback.assert_called_once()
