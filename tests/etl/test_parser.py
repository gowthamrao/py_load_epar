from pathlib import Path

import pytest

from py_load_epar.etl.parser import parse_ema_excel_file

import pandas as pd

# (Kept the original test for non-existent file)

@pytest.fixture
def valid_excel_file(tmp_path: Path) -> Path:
    """Creates a valid Excel file for parser testing with canonical headers."""
    file_path = tmp_path / "parser_test_data.xlsx"
    data = {
        "Category": ["Human", "Human"],
        "Medicine name": ["TestMed1", "TestMed2"],
        "Therapeutic area": ["Oncology", "Cardiology"],
        "Active substance": ["Testmed-A", "Testmed-B"],
        "Product number": ["EMA/1", "EMA/2"],
        "Patient safety": [None, None],
        "Authorization status": ["Authorised", "Withdrawn"], # Canonical 'z' spelling
        "ATC code": ["L01", "C01"],
        "URL": ["http://example.com/1", "http://example.com/2"],
        "Marketing authorisation holder/company name": ["Test Pharma 1", "Test Pharma 2"],
        "Revision date": ["2023-01-15", "2023-01-16"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_parse_ema_excel_file_returns_iterator(valid_excel_file: Path):
    """Test that the parser returns an iterator."""
    from collections.abc import Iterator

    result = parse_ema_excel_file(valid_excel_file)
    assert isinstance(result, Iterator)


def test_parse_ema_excel_file_yields_correct_data(valid_excel_file: Path):
    """
    Tests that the parser successfully reads a valid Excel file and yields
    the correct data as dictionaries with snake_cased keys.
    """
    records = list(parse_ema_excel_file(valid_excel_file))

    assert len(records) == 2
    first_record = records[0]
    assert isinstance(first_record, dict)

    # Check for a few snake_cased headers
    assert "medicine_name" in first_record
    assert "product_number" in first_record
    assert "authorization_status" in first_record # Check for canonical 'z' spelling
    assert "u_r_l" in first_record

    # Check values
    assert first_record["medicine_name"] == "TestMed1"
    assert first_record["authorization_status"] == "Authorised"
    assert records[1]["authorization_status"] == "Withdrawn"


def test_parse_non_existent_file_raises_error():
    """
    Tests that the parser raises an exception when the file does not exist.
    We expect openpyxl to raise a FileNotFoundError.
    """
    non_existent_path = Path("this_file_does_not_exist.xlsx")
    with pytest.raises(FileNotFoundError):
        # We must consume the iterator to trigger the file open operation
        list(parse_ema_excel_file(non_existent_path))
