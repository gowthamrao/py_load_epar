from unittest.mock import MagicMock, patch

import pytest
from pathlib import Path

from py_load_epar.config import Settings
from py_load_epar.etl.orchestrator import run_etl
from py_load_epar.models import EparIndex, EparSubstanceLink
from py_load_epar.spor_api.client import SporApiClient


@patch("py_load_epar.etl.orchestrator.SporApiClient")
@patch("py_load_epar.etl.orchestrator.get_db_adapter")
@patch("py_load_epar.etl.orchestrator.extract_data")
@patch("py_load_epar.etl.orchestrator.transform_and_validate")
@patch("py_load_epar.etl.orchestrator._process_substance_links")
@patch("py_load_epar.etl.orchestrator._process_documents")
def test_run_etl_successful_flow(
    mock_process_docs,
    mock_process_links,
    mock_transform,
    mock_extract,
    mock_get_adapter,
    mock_spor_client_class,
):
    """
    Test the happy path of the ETL orchestrator, ensuring all main components
    and ancillary data processing steps are called.
    """
    # Arrange
    settings = Settings()
    settings.etl.document_storage_path = "/tmp/docs"
    mock_adapter = MagicMock()
    mock_get_adapter.return_value = mock_adapter
    mock_spor_client_instance = MagicMock(spec=SporApiClient)
    mock_spor_client_class.return_value = mock_spor_client_instance

    mock_extract.return_value = (iter([{"id": 1}]), None)

    # Simulate two validated records being returned by the transform step
    epar_records = [MagicMock(spec=EparIndex), MagicMock(spec=EparIndex)]
    substance_links = [MagicMock(spec=EparSubstanceLink)]
    mock_transform.return_value = iter([(epar_records[0], substance_links), (epar_records[1], [])])
    mock_adapter.log_pipeline_start.return_value = 123  # Mock execution_id

    # Act
    run_etl(settings)

    # Assert Core ETL
    mock_spor_client_class.assert_called_once_with(settings.spor_api)
    mock_get_adapter.assert_called_once_with(settings)
    mock_adapter.connect.assert_called_once()
    mock_extract.assert_called_once()
    mock_transform.assert_called_once_with(
        mock_extract.return_value[0], mock_spor_client_instance, 123
    )
    mock_adapter.prepare_load.assert_called_once_with(
        load_strategy=settings.etl.load_strategy, target_table="epar_index"
    )
    mock_adapter.bulk_load_batch.assert_called_once()
    mock_adapter.finalize.assert_called_once()
    assert not mock_adapter.rollback.called
    mock_adapter.close.assert_called_once()

    # Assert Ancillary Data Processing
    mock_process_links.assert_called_once_with(mock_adapter, substance_links)
    mock_process_docs.assert_called_once()
    call_args, _ = mock_process_docs.call_args
    assert call_args[0] is mock_adapter
    assert call_args[1] == epar_records
    assert call_args[2] == Path(settings.etl.document_storage_path)


@patch("py_load_epar.etl.orchestrator.SporApiClient")
@patch("py_load_epar.etl.orchestrator.get_db_adapter")
@patch("py_load_epar.etl.orchestrator.extract_data")
def test_run_etl_rolls_back_on_failure(
    mock_extract, mock_get_adapter, mock_spor_client_class
):
    """
    Test that the orchestrator calls rollback() on the adapter when an error occurs.
    """
    # Arrange
    settings = Settings()
    mock_adapter = MagicMock()
    mock_get_adapter.return_value = mock_adapter

    mock_extract.return_value = (iter([{}]), None)
    mock_adapter.log_pipeline_start.return_value = 123

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
    mock_adapter.log_pipeline_failure.assert_called_once_with(123)
    mock_adapter.rollback.assert_called_once()
    assert not mock_adapter.finalize.called
    mock_adapter.close.assert_called_once()
