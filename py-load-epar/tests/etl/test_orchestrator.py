from unittest.mock import MagicMock, patch

import pytest
from pathlib import Path

from py_load_epar.config import Settings
from py_load_epar.etl.orchestrator import run_etl
from py_load_epar.models import EparIndex


@patch("py_load_epar.etl.orchestrator.get_db_adapter")
@patch("py_load_epar.etl.orchestrator.extract_data")
@patch("py_load_epar.etl.orchestrator.transform_and_validate")
@patch("py_load_epar.etl.orchestrator._process_documents")
def test_run_etl_successful_flow(
    mock_process_docs, mock_transform, mock_extract, mock_get_adapter
):
    """
    Test the happy path of the ETL orchestrator, ensuring all main components
    and the document processing step are called.
    """
    # Arrange
    settings = Settings()
    settings.etl.document_storage_path = "/tmp/docs"
    mock_adapter = MagicMock()
    mock_get_adapter.return_value = mock_adapter

    mock_extract.return_value = iter([{"id": 1}])
    # Simulate two validated records being returned by the transform step
    validated_records = [MagicMock(spec=EparIndex), MagicMock(spec=EparIndex)]
    mock_transform.return_value = iter(validated_records)

    # Act
    run_etl(settings)

    # Assert Core ETL
    mock_get_adapter.assert_called_once_with(settings)
    mock_adapter.connect.assert_called_once()
    mock_extract.assert_called_once()
    mock_transform.assert_called_once()
    mock_adapter.prepare_load.assert_called_once_with(
        load_strategy=settings.etl.load_strategy, target_table="epar_index"
    )
    mock_adapter.bulk_load_batch.assert_called_once()
    mock_adapter.finalize.assert_called_once()
    assert not mock_adapter.rollback.called
    mock_adapter.close.assert_called_once()

    # Assert Document Processing
    mock_process_docs.assert_called_once()
    # Check that it was called with the adapter, the records, and the correct path
    call_args, _ = mock_process_docs.call_args
    assert call_args[0] is mock_adapter
    assert call_args[1] == validated_records
    assert call_args[2] == Path(settings.etl.document_storage_path)


@patch("py_load_epar.etl.orchestrator.get_db_adapter")
@patch("py_load_epar.etl.orchestrator.extract_data")
def test_run_etl_rolls_back_on_failure(mock_extract, mock_get_adapter):
    """
    Test that the orchestrator calls rollback() on the adapter when an error occurs.
    """
    # Arrange
    settings = Settings()
    mock_adapter = MagicMock()
    mock_get_adapter.return_value = mock_adapter

    # Simulate a failure during the transform step
    error_message = "Validation error"
    with patch(
        "py_load_epar.etl.orchestrator.transform_and_validate",
        side_effect=Exception(error_message),
    ):
        # Act & Assert
        with pytest.raises(Exception, match=error_message):
            run_etl(settings)

    # Assert that rollback was called and finalize was not
    mock_adapter.rollback.assert_called_once()
    assert not mock_adapter.finalize.called
    mock_adapter.close.assert_called_once()
