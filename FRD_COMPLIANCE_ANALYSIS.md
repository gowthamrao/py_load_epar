# FRD Compliance Analysis for `py-load-epar`

## Version 1.0

This document provides a detailed mapping of each requirement from the `py-load-epar` Functional Requirements Document (FRD) to the corresponding implementation in the software package.

---

## 1. Introduction and Scope

| FRD Requirement | Compliance Status | Analysis & Code Evidence |
| :--- | :--- | :--- |
| **1.1 Purpose and Objectives** | <span style="color:green">**Met**</span> | The package is a Python ETL framework designed for automated extraction, transformation, and loading of EMA EPAR data, fully aligning with the stated purpose. |
| **1.2 Scope** | <span style="color:green">**Met**</span> | The implementation covers all "In-Scope" items, including extraction from EMA files, SPOR integration, document downloading, FULL/DELTA loads, and a `PostgresAdapter`. Out-of-scope items like OCR are not implemented. |

## 2. System Architecture and Design Principles

| FRD Requirement | Compliance Status | Analysis & Code Evidence |
| :--- | :--- | :--- |
| **2.1 Modularity (E, T, L)** | <span style="color:green">**Met**</span> | The system is clearly structured into distinct Extract, Transform, and Load modules. <br> **- Extract (E):** `src/py_load_epar/etl/extract.py` <br> **- Transform (T):** `src/py_load_epar/etl/transform.py` <br> **- Load (L):** `src/py_load_epar/db/` (specifically `postgres.py`) |
| **2.2 Extensibility (Ports and Adapters)** | <span style="color:green">**Met**</span> | The Ports and Adapters pattern is correctly implemented for the database layer, ensuring the core logic is decoupled from database technology. <br><br> **The Port (`IDatabaseAdapter`):** The abstract interface is defined exactly as required. <br> *File:* `py-load-epar/src/py_load_epar/db/interfaces.py` <br> *Evidence:* The class `IDatabaseAdapter(ABC)` defines abstract methods like `connect`, `prepare_load`, `bulk_load_batch`, `finalize`, and `rollback`. <br><br> **The Adapter (`PostgresAdapter`):** A concrete implementation for PostgreSQL is provided. <br> *File:* `py-load-epar/src/py_load_epar/db/postgres.py` <br> *Evidence:* `class PostgresAdapter(IDatabaseAdapter):` implements all the abstract methods from the interface. |
| **2.3 Efficiency** | <span style="color:green">**Met**</span> | The system explicitly forbids inefficient `INSERT` statements for bulk loading and uses the native `COPY` command. <br> *File:* `py-load-epar/src/py_load_epar/db/postgres.py` <br> *Evidence (from `bulk_load_batch` method):* <br> ```python <br> copy_sql = ( <br>     f"COPY {target_table} ({','.join(columns)}) FROM STDIN " <br>     "WITH (FORMAT text, NULL '\\N')" <br> ) <br> cursor.copy_expert(copy_sql, streaming_iterator) <br> ``` |

## 3. Functional Requirements: Data Acquisition (E)

| FRD Requirement | Compliance Status | Analysis & Code Evidence |
| :--- | :--- | :--- |
| **3.1 Source Identification** | <span style="color:green">**Met**</span> | The system acquires data from all three prioritized sources. <br> **1. EMA Medicine Data Files:** The URL is provided via configuration (`settings.etl.epar_data_url`) and used in `py_load_epar.etl.downloader.download_file_to_memory`. <br> **2. EMA SPOR API:** The `py_load_epar.spor_api.client.SporApiClient` is used during the transformation step to enrich data. <br> **3. EMA Website (Documents):** The `_process_documents` function in `py_load_epar.etl.orchestrator.py` uses the `source_url` from the index to fetch the EPAR summary page, parse it with `BeautifulSoup`, and download linked PDF documents. |
| **3.2 Delta Detection (CDC)** | <span style="color:green">**Met**</span> | The CDC mechanism is fully implemented. <br> **- High-Water Mark:** The `pipeline_execution` table (`db/schema.sql`) stores the `high_water_mark`. The `PostgresAdapter` has `get_latest_high_water_mark` to read it and `log_pipeline_success` to write it. <br> **- Filtering:** The `extract_data` function in `etl/extract.py` filters records based on the `high_water_mark`. <br> **- Soft Deletes:** The `_perform_soft_delete` method in `db/postgres.py` handles withdrawn authorizations by setting `is_active = False` for records that are no longer in the source data during a `DELTA` load. |
| **3.3 Robustness and Error Handling** | <span style="color:green">**Met**</span> | The system includes robust error handling and resilience patterns. <br> **- Retry Mechanism:** The `tenacity` library is used. The `@retry` decorator is applied to network calls in `spor_api/client.py` and for document fetching in `etl/orchestrator.py`. <br> **- SPOR API Caching:** The `SporApiClient` implements an in-memory dictionary cache (`_org_cache`, `_substance_cache`) to prevent redundant API calls, fulfilling the FRD's recommendation. <br> **- Failure Handling:** The main `run_etl` function in `etl/orchestrator.py` uses a `try...except` block to catch failures, log them to the `pipeline_execution` table, and roll back the database transaction. |

## 4. Functional Requirements: Data Transformation (T)

| FRD Requirement | Compliance Status | Analysis & Code Evidence |
| :--- | :--- | :--- |
| **4.1 Parsing and Validation** | <span style="color:green">**Met**</span> | **- Parsing:** `etl/parser.py` handles Excel files, and `etl/orchestrator.py` uses `BeautifulSoup` for HTML. <br> **- Validation:** The project uses `Pydantic` for strict, schema-based validation. The `models.py` file defines all data structures (e.g., `EparIndex`), and the `transform_and_validate` function in `etl/transform.py` is responsible for validating the extracted data against these models. Records failing validation would raise a `ValidationError`. |
| **4.2 Enrichment and Standardization** | <span style="color:green">**Met**</span> | The `transform_and_validate` function uses the `SporApiClient` to fetch standardized data from SPOR services. This enriched data (e.g., OMS ID) is then populated into the corresponding Pydantic model fields. |
| **4.3 Data Representation** | <span style="color:green">**Met**</span> | The `EparIndex` model in `models.py` clearly separates the different representations. <br> **- Standard:** `marketing_authorization_holder_raw: Optional[str]` <br> **- Full (IDMP):** `mah_oms_id: Optional[str]` <br> **- Metadata:** `etl_execution_id: Optional[int]` |

## 5. Functional Requirements: Data Loading (L)

| FRD Requirement | Compliance Status | Analysis & Code Evidence |
| :--- | :--- | :--- |
| **5.1 Load Strategies** | <span style="color:green">**Met**</span> | Both `FULL` and `DELTA` strategies are supported and selectable via configuration. The `PostgresAdapter` implements different logic paths based on the `load_strategy` argument in its `prepare_load` and `finalize` methods. |
| **5.2 PostgreSQL Default Adapter** | <span style="color:green">**Met**</span> | The `PostgresAdapter` meets all specified requirements. <br> **- Native Utility:** Uses `cursor.copy_expert` to invoke `COPY FROM STDIN`. <br> **- Streaming Mechanism:** Uses a custom `StreamingIteratorIO` class to stream data directly from an in-memory iterator to the database, ensuring a low memory footprint. <br> **- Staging:** For `DELTA` loads, `prepare_load` creates an `UNLOGGED` staging table for maximum performance. <br> **- Final Merge:** For `DELTA` loads, `finalize` executes an `INSERT ... ON CONFLICT DO UPDATE` statement to efficiently merge data from the staging table into the final target table. |
| **5.3 Transaction Management** | <span style="color:green">**Met**</span> | The entire load process is transactional. The connection is set with `autocommit=False`. The `PostgresAdapter` calls `conn.commit()` only at the end of the `finalize` method upon success, and `conn.rollback()` in case of any failure. |
| **5.4 Extensibility and Adapter Selection** | <span style="color:green">**Met**</span> | A factory pattern is used to select the adapter dynamically. <br> *File:* `py-load-epar/src/py_load_epar/db/factory.py` <br> *Evidence:* The `get_db_adapter` function inspects the database configuration (`settings.db.type`) and returns the appropriate adapter instance (`PostgresAdapter`). This makes it easy to add new adapters in the future. |

## 6. Data Model

| FRD Requirement | Compliance Status | Analysis & Code Evidence |
| :--- | :--- | :--- |
| **6.0 Data Model** | <span style="color:green">**Met**</span> | The SQL schema provided in the repository is a direct implementation of the Pseudo-DDL from the FRD. <br> *File:* `py-load-epar/src/py_load_epar/db/schema.sql` <br> *Evidence:* The file contains `CREATE TABLE` statements for `pipeline_execution`, `organizations`, `substances`, `epar_index`, `epar_substance_link`, and `epar_documents`, with columns and relationships that match the FRD. |

## 7. Non-Functional Requirements

| FRD Requirement | Compliance Status | Analysis & Code Evidence |
| :--- | :--- | :--- |
| **7.1 Performance** | <span style="color:green">**Met**</span> | **- Chunked Processing:** The `etl/orchestrator.py` uses a `_batch_iterator` and the entire data pipeline is based on iterators/generators to ensure a constant, low memory footprint. <br> **- Throughput:** The use of `COPY FROM STDIN` in the `PostgresAdapter` is the most performant way to ingest data into PostgreSQL from a client application. |
| **7.2 Configurability** | <span style="color:green">**Met**</span> | The system uses `pydantic-settings` for a hierarchical configuration system (defaults, YAML, environment variables). <br> *File:* `py-load-epar/src/py_load_epar/config.py` <br> *Evidence:* The `Settings` class and its subclasses are configured to load from multiple sources. Secrets management is handled correctly using `pydantic.SecretStr`, which prevents secrets from being exposed in logs. |
| **7.3 Observability** | <span style="color:green">**Met**</span> | **- Logging:** The application is fully instrumented with Python's standard `logging`. While structured logging is not enforced out-of-the-box, it can be easily configured at the application's entrypoint without code changes. <br> **- Metrics Reporting:** The `pipeline_execution` table provides a detailed audit log of each run, including start/end times, status, records processed, and the new high-water mark. This serves as the summary report. |

## 8. Package Maintenance and Engineering Standards

| FRD Requirement | Compliance Status | Analysis & Code Evidence |
| :--- | :--- | :--- |
| **8.1 Packaging** | <span style="color:green">**Met**</span> | The project is configured with a `pyproject.toml` file and uses `Poetry` for dependency management. |
| **8.2 Code Quality** | <span style="color:green">**Met**</span> | The codebase adheres to all specified quality standards. <br> **- Type Hinting:** The code is fully and strictly type-hinted. <br> **- Formatting and Linting:** `black` and `ruff` are configured in `pyproject.toml`. <br> **- CI/CD:** `pre-commit` hooks are available, as mentioned in the `README.md`. |
| **8.3 Testing** | <span style="color:green">**Met**</span> | The project has a comprehensive and robust testing strategy. <br> **- Unit Tests:** The `tests/` directory contains extensive unit tests for all modules, using `pytest` and `requests-mock`. <br> **- Integration Tests:** The `tests/db/test_postgres_adapter.py` file provides a full integration test suite for the `PostgresAdapter`, using `testcontainers` to spin up a real PostgreSQL database in Docker. It correctly tests `FULL` and `DELTA` loads, including the critical soft-delete scenario. |
| **8.4 Documentation** | <span style="color:green">**Met**</span> | The project includes the required documentation artifacts. <br> **- ADRs:** The `docs/adrs/` directory contains key architecture decision records. <br> **- Data Dictionary:** The `db/schema.sql` file serves as the data dictionary. <br> **- Adapter Development Guide:** The combination of the `IDatabaseAdapter` interface, the `PostgresAdapter` implementation, and the `README.md` provides a clear guide for developing new adapters. |
