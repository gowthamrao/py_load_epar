import datetime
from unittest.mock import MagicMock, patch

import pytest

from py_load_epar.config import Settings
from py_load_epar.etl.orchestrator import run_etl
from py_load_epar.models import EparIndex, EparSubstanceLink
from py_load_epar.spor_api.client import SporApiClient
from py_load_epar.storage.interfaces import IStorage


@patch("py_load_epar.etl.orchestrator.StorageFactory")
@patch("py_load_epar.etl.orchestrator.SporApiClient")
@patch("py_load_epar.etl.orchestrator.get_db_adapter")
@patch("py_load_epar.etl.orchestrator.extract_data")
@patch("py_load_epar.etl.orchestrator.transform_and_validate")
@patch("py_load_epar.etl.orchestrator._process_organizations")
@patch("py_load_epar.etl.orchestrator._process_substances")
@patch("py_load_epar.etl.orchestrator._process_substance_links")
@patch("py_load_epar.etl.orchestrator._process_documents")
def test_run_etl_successful_flow(
    mock_process_docs,
    mock_process_links,
    mock_process_substances,
    mock_process_orgs,
    mock_transform,
    mock_extract,
    mock_get_adapter,
    mock_spor_client_class,
    mock_storage_factory,
):
    """
    Test the happy path of the ETL orchestrator, ensuring all main components
    and ancillary data processing steps are called.
    """
    # Arrange
    settings = Settings()
    settings.etl.batch_size = 1 # Process one record at a time
    mock_adapter = MagicMock()
    mock_adapter.get_latest_high_water_mark.return_value = None
    mock_get_adapter.return_value = mock_adapter

    mock_spor_client_instance = MagicMock(spec=SporApiClient)
    mock_spor_client_class.return_value = mock_spor_client_instance

    mock_storage_instance = MagicMock(spec=IStorage)
    mock_storage_factory.return_value.get_storage.return_value = mock_storage_instance

    mock_raw_records_iterator = iter([{"id": 1}, {"id": 2}])
    mock_extract.return_value = mock_raw_records_iterator

    record1 = MagicMock(spec=EparIndex)
    record1.last_update_date_source = datetime.date(2024, 1, 1)
    record1.source_url = "http://example.com/doc1"
    record2 = MagicMock(spec=EparIndex)
    record2.last_update_date_source = datetime.date(2024, 1, 2)
    record2.source_url = None
    substance_links = [MagicMock(spec=EparSubstanceLink)]

    # Mock transform to return the new 4-tuple format
    mock_transform.return_value = iter([
        (record1, substance_links, ["org1"], ["sub1"]),
        (record2, [], [], []),
    ])
    mock_adapter.log_pipeline_start.return_value = 123

    # Act
    run_etl(settings)

    # Assert
    mock_storage_factory.assert_called_once_with(settings.storage)
    mock_spor_client_class.assert_called_once_with(settings.spor_api)
    mock_get_adapter.assert_called_once_with(settings)
    mock_adapter.connect.assert_called_once()
    mock_extract.assert_called_once()
    mock_transform.assert_called_once_with(
        mock_raw_records_iterator, mock_spor_client_instance, 123
    )
    # Check that all processing functions were called with the correct data
    assert mock_process_orgs.call_count == 2
    mock_process_orgs.assert_any_call(mock_adapter, ["org1"])
    assert mock_process_substances.call_count == 2
    mock_process_substances.assert_any_call(mock_adapter, ["sub1"])
    mock_process_links.assert_called_once_with(mock_adapter, substance_links)
    # _process_documents is now only called with records that have a valid URL
    mock_process_docs.assert_called_once_with(
        adapter=mock_adapter,
        processed_records=[record1],  # record2 has a None URL
        storage=mock_storage_instance,
    )
    mock_adapter.close.assert_called_once()


@patch("py_load_epar.etl.orchestrator.StorageFactory")
@patch("py_load_epar.etl.orchestrator.SporApiClient")
@patch("py_load_epar.etl.orchestrator.get_db_adapter")
@patch("py_load_epar.etl.orchestrator.extract_data")
def test_run_etl_rolls_back_on_failure(
    mock_extract, mock_get_adapter, mock_spor_client_class, mock_storage_factory
):
    """
    Test that the orchestrator calls rollback() on the adapter when an error occurs.
    """
    # Arrange
    settings = Settings()
    mock_adapter = MagicMock()
    mock_get_adapter.return_value = mock_adapter

    mock_extract.return_value = iter([{}])
    mock_adapter.log_pipeline_start.return_value = 123

    error_message = "Validation error"
    with patch(
        "py_load_epar.etl.orchestrator.transform_and_validate",
        side_effect=Exception(error_message),
    ):
        # Act & Assert
        with pytest.raises(Exception, match=error_message):
            run_etl(settings)

    mock_adapter.log_pipeline_failure.assert_called_once_with(123)
    mock_adapter.rollback.assert_called_once()
    assert not mock_adapter.finalize.called
    mock_adapter.close.assert_called_once()
