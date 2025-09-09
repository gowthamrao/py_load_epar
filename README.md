# py-load-epar

`py-load-epar` is a Python ETL framework for ingesting European Medicines Agency (EMA) European Public Assessment Report (EPAR) data into a relational database. It is designed to be robust, scalable, and extensible.

This project was built based on the functional requirements outlined in the provided FRD.

## Features

- **Extract, Transform, Load (ETL):** A modular pipeline for processing EPAR data.
- **Extensible Database Support:** Uses a "Ports and Adapters" pattern. A `PostgresAdapter` is provided, and other adapters can be easily added.
- **High-Performance Loading:** Utilizes native database bulk loading utilities (e.g., `COPY` for PostgreSQL) for maximum efficiency.
- **Data Validation:** Leverages `Pydantic` for strict data validation and schema enforcement.
- **Configurable:** Settings can be managed via a `config.yaml` file and overridden with environment variables.
- **Robust Testing:** Includes a full suite of unit tests and `testcontainers`-based integration tests.

## Project Structure

```
.
├── pyproject.toml      # Poetry configuration, dependencies
├── README.md           # This file
├── docs/adrs/          # Architecture Decision Records
└── src/
    └── py_load_epar/
        ├── config.py       # Configuration management using Pydantic
        ├── models.py       # Pydantic data models
        ├── db/             # Database layer (Ports and Adapters)
        │   ├── interfaces.py
        │   ├── postgres.py
        │   ├── factory.py
        │   └── schema.sql
        ├── etl/            # Core ETL logic
        │   ├── extract.py
        │   ├── transform.py
        │   └── orchestrator.py
        └── __main__.py     # Entrypoint for running the ETL
```

## Setup

This project uses [Poetry](https://python-poetry.org/) for dependency management.

1.  **Install Poetry:** Follow the instructions on the official website.
2.  **Install Dependencies:** Navigate to the project root (`py-load-epar`) and run:
    ```bash
    poetry install
    ```
    This will create a virtual environment and install all required dependencies. To install development dependencies as well, run:
    ```bash
    poetry install --with dev
    ```
3.  **Set up Pre-commit Hooks (Optional but Recommended):**
    ```bash
    poetry run pre-commit install
    ```

## Configuration

The application is configured through a hierarchy of defaults, a YAML file, and environment variables.

1.  **Create a `config.yaml` file:** You can create a `config.yaml` in the project root to override default settings.

    ```yaml
    # config.yaml (example)
    db:
      host: "localhost"
      port: 5432
      user: "myuser"
      dbname: "mydatabase"

    etl:
      load_strategy: "DELTA"
      batch_size: 5000
    ```

2.  **Environment Variables:** Any setting can be overridden by environment variables. Secrets like the database password **must** be provided this way. The variables are prefixed and use a `__` delimiter for nested keys.

    ```bash
    # Example: Set the database password
    export PY_LOAD_EPAR_DB_PASSWORD="your_secret_password"

    # Example: Override the load strategy
    export PY_LOAD_EPAR_ETL_LOAD_STRATEGY="FULL"
    ```

## Running the ETL

The ETL process can be triggered by running the main orchestrator function. An entrypoint will be added to run it from the command line.

```bash
# Ensure your environment is configured (e.g., DB password)
poetry run python -m py_load_epar
```

## Running Tests

The project has a comprehensive test suite.

1.  **Run all tests:**
    ```bash
    poetry run pytest
    ```
2.  **Run only unit tests:**
    ```bash
    poetry run pytest -m "not integration"
    ```
3.  **Run only integration tests (requires Docker):**
    ```bash
    poetry run pytest -m "integration"
    ```
