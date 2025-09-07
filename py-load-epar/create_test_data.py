import openpyxl
from pathlib import Path

# Define the file path
test_data_dir = Path("tests/test_data")
test_data_dir.mkdir(exist_ok=True)
file_path = test_data_dir / "sample_ema_data.xlsx"

# Create a new workbook and select the active sheet
wb = openpyxl.Workbook()
ws = wb.active
ws.title = "Worksheet"

# Define the headers
headers = [
    "Category",
    "Medicine name",
    "Therapeutic area",
    "International non-proprietary name (INN) / common name",
    "Active substance",
    "Product number",
    "Patient safety",
    "Authorisation status",
    "ATC code",
    "Additional monitoring",
    "Generic",
    "Biosimilar",
    "Conditional marketing authorisation",
    "Exceptional circumstances",
    "Accelerated assessment",
    "Orphan medicine",
    "Marketing authorisation date",
    "Date of opinion",
    "Marketing authorisation holder/company name",
    "Pharmacotherapeutic group",
    "Date of withdrawal of marketing authorisation",
    "First published",
    "Revision date",
    "URL",
]

# Append headers to the sheet
ws.append(headers)

# Define the data rows
data = [
    [
        "Human",
        "TestMed1",
        "Endocrinology",
        "testmed-a",
        "Testmed-A",
        "EMA/H/C/000001",
        "",
        "Authorised",
        "A10AB01",
        "",
        "No",
        "No",
        "No",
        "No",
        "No",
        "No",
        "2024-01-25 00:00:00",
        "2023-11-10 00:00:00",
        "Test Pharma 1",
        "Drugs used in diabetes",
        "",
        "2024-01-25",
        "2024-01-25",
        "http://example.com/doc1.pdf",
    ],
    [
        "Human",
        "TestMed2",
        "Oncology",
        "testmed-b",
        "Testmed-B",
        "EMA/H/C/000002",
        "",
        "Authorised",
        "L01AX01",
        "",
        "No",
        "No",
        "No",
        "No",
        "No",
        "Yes",
        "2024-02-15 00:00:00",
        "2023-12-15 00:00:00",
        "Test Pharma 2",
        "Antineoplastic agents",
        "",
        "2024-02-15",
        "2024-02-15",
        "http://example.com/doc2.pdf",
    ],
]

# Append data rows to the sheet
for row in data:
    ws.append(row)

# Save the workbook
wb.save(file_path)

print(f"Test data file created at: {file_path}")
