from unittest.mock import MagicMock, patch

import pytest

from py_load_epar.etl.orchestrator import run_etl
from py_load_epar.config import Settings


@patch('py_load_epar.etl.orchestrator.get_db_adapter')
@patch('py_load_epar.etl.orchestrator.extract_data')
@patch('py_load_epar.etl.orchestrator.transform_and_validate')
def test_run_etl_successful_flow(mock_transform, mock_extract, mock_get_adapter):
    """
    Test the happy path of the ETL orchestrator.
    Ensures all components are called in the correct order and the transaction is finalized.
    """
    # Arrange
    settings = Settings()
    mock_adapter = MagicMock()
    mock_get_adapter.return_value = mock_adapter

    mock_extract.return_value = iter([{'id': 1}])
    mock_transform.return_value = iter([MagicMock(), MagicMock()]) # 2 records in 1 batch

    # Act
    run_etl(settings)

    # Assert
    mock_get_adapter.assert_called_once_with(settings)
    mock_adapter.connect.assert_called_once()

    mock_extract.assert_called_once()
    mock_transform.assert_called_once()

    mock_adapter.prepare_load.assert_called_once_with(
        load_strategy=settings.etl.load_strategy,
        target_table="epar_index"
    )
    mock_adapter.bulk_load_batch.assert_called_once()
    mock_adapter.finalize.assert_called_once()

    assert not mock_adapter.rollback.called
    mock_adapter.close.assert_called_once()


@patch('py_load_epar.etl.orchestrator.get_db_adapter')
@patch('py_load_epar.etl.orchestrator.extract_data')
@patch('py_load_epar.etl.orchestrator.transform_and_validate')
def test_run_etl_rolls_back_on_failure(mock_transform, mock_extract, mock_get_adapter):
    """
    Test that the orchestrator calls rollback() on the adapter when an error occurs.
    """
    # Arrange
    settings = Settings()
    mock_adapter = MagicMock()
    mock_get_adapter.return_value = mock_adapter

    # Simulate a failure during the bulk load
    error_message = "DB error"
    mock_adapter.bulk_load_batch.side_effect = Exception(error_message)

    mock_extract.return_value = iter([{'id': 1}])
    mock_transform.return_value = iter([MagicMock()])

    # Act & Assert
    with pytest.raises(Exception, match=error_message):
        run_etl(settings)

    # Assert that rollback was called and finalize was not
    mock_adapter.rollback.assert_called_once()
    assert not mock_adapter.finalize.called

    # The 'finally' block should still close the connection
    mock_adapter.close.assert_called_once()
