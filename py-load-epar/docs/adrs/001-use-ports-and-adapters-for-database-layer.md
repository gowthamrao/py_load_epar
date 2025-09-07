# 1. Use Ports and Adapters for the Database Layer

Date: 2025-09-07

## Status

Accepted

## Context

The Functional Requirements Document (FRD) for `py-load-epar` specifies that the system must be extensible and cloud-agnostic, allowing for seamless integration with various target database technologies (e.g., PostgreSQL, Amazon Redshift, Snowflake). The core ETL logic must remain independent of the database-specific implementation details.

## Decision

We will implement the **Ports and Adapters** (also known as Hexagonal Architecture) pattern for the database layer.

This will be achieved by:
1.  **Defining a "Port":** A Python Abstract Base Class (ABC) named `IDatabaseAdapter` will define a standard interface for all database operations required by the ETL process (e.g., `connect`, `prepare_load`, `bulk_load_batch`, `finalize`, `rollback`).
2.  **Implementing "Adapters":** For each target database technology, a concrete class will be created that implements the `IDatabaseAdapter` interface. The initial project scope requires a `PostgresAdapter`. This adapter will encapsulate all the PostgreSQL-specific logic, such as using the `psycopg2` library and the `COPY` command.

A `DatabaseAdapterFactory` will be responsible for instantiating the correct adapter based on the application's configuration.

## Consequences

### Positive

-   **Decoupling:** The core application logic is completely decoupled from the database technology. The orchestrator interacts only with the `IDatabaseAdapter` interface, not with any concrete implementation.
-   **Extensibility:** Adding support for a new database (e.g., Snowflake) becomes a well-defined task: simply create a new `SnowflakeAdapter` that implements the required methods. No changes are needed in the core ETL logic.
-   **Testability:** The core logic can be unit-tested by providing a mock adapter. The adapters themselves can be tested in isolation via integration tests against real (containerized) database instances. This aligns perfectly with the testing strategy.
-   **Clarity of Concerns:** The separation is clear. The `etl` module knows *what* needs to be done, and the `db` adapters know *how* to do it for a specific database.

### Negative

-   **Increased Indirection:** This pattern introduces a layer of abstraction, which can add a small amount of complexity compared to a direct, hardcoded implementation.
-   **Interface Maintenance:** The `IDatabaseAdapter` interface must be carefully designed to be generic enough to accommodate different database paradigms while still being specific enough to be useful. Changes to the interface will affect all existing adapters.
