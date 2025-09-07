import io
import logging

import pytest
import requests
import requests_mock
from py_load_epar.etl.downloader import download_file, parse_excel_data

# Configure logging for tests
logging.basicConfig(level=logging.INFO)


def test_download_file_success(requests_mock):
    """
    Tests that download_file successfully downloads content on a 200 OK response.
    """
    url = "http://test.com/data.xlsx"
    mock_content = b"dummy excel content"
    requests_mock.get(url, content=mock_content, status_code=200)

    result_buffer = download_file(url)

    assert isinstance(result_buffer, io.BytesIO)
    assert result_buffer.read() == mock_content


def test_download_file_http_error(requests_mock):
    """
    Tests that download_file raises a RequestException on an HTTP error (e.g., 404).
    """
    url = "http://test.com/notfound"
    requests_mock.get(url, status_code=404, reason="Not Found")

    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        download_file(url)

    assert "404 Client Error: Not Found" in str(excinfo.value)


def test_parse_excel_data():
    """
    Tests that parse_excel_data correctly parses an in-memory Excel file.
    """
    # 1. Create an in-memory Excel file
    output = io.BytesIO()
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Test Data"

    # 2. Add data
    header = ["ID", "Medicine Name", "Value"]
    rows = [
        (1, "TestMed A", 100),
        (2, "TestMed B", 200),
    ]
    sheet.append(header)
    for row in rows:
        sheet.append(row)

    # 3. Save to buffer
    workbook.save(output)
    output.seek(0)  # Rewind the buffer to the beginning

    # 4. Parse the data
    parsed_records = parse_excel_data(output)

    # 5. Check the results
    assert hasattr(parsed_records, '__iter__')  # Check if it's an iterator
    result_list = list(parsed_records)

    assert len(result_list) == 2
    assert result_list[0] == {"ID": 1, "Medicine Name": "TestMed A", "Value": 100}
    assert result_list[1] == {"ID": 2, "Medicine Name": "TestMed B", "Value": 200}
