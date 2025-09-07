# 2. Use Pydantic for Data Validation and Settings Management

Date: 2025-09-07

## Status

Accepted

## Context

The FRD requires that all incoming data be strictly validated against predefined schemas to ensure type safety and data quality. It also requires a hierarchical configuration system (defaults, YAML, environment variables) where secrets are handled securely.

## Decision

We will use the **Pydantic** library for both data validation and application settings management.

1.  **Data Validation:** We will define Pydantic `BaseModel` classes for each key data entity (e.g., `EparIndex`). The `Transform` module will use these models to parse and validate the raw data extracted from the source. Pydantic's `ValidationError` will be used to catch and quarantine invalid records.
2.  **Settings Management:** We will use Pydantic's `BaseSettings` (via the `pydantic-settings` package) to manage application configuration. A nested structure of settings models (`DatabaseSettings`, `EtlSettings`, etc.) will provide a clean, typed, and documented configuration schema. This library natively handles loading from environment variables, which satisfies the requirement for secrets management. We will supplement this with a manual loader for a `config.yaml` file to provide the full hierarchical configuration.

## Consequences

### Positive

-   **Robustness & Data Integrity:** Pydantic provides immediate, clear, and strict validation of data structures. This is crucial for maintaining data quality in the target database.
-   **Developer Experience:** It provides a declarative, expressive, and easy-to-use syntax for defining complex data schemas. The error messages from `ValidationError` are highly detailed, making debugging easy.
-   **Single Tool for Two Jobs:** Pydantic elegantly solves both the data validation and configuration management problems, reducing the number of third-party dependencies and cognitive overhead.
-   **Automatic Type Coercion:** Pydantic intelligently coerces incoming data into the correct Python types (e.g., string to `datetime.date`), simplifying the transformation logic.
-   **IDE Support:** As Pydantic models are just Python classes, they provide excellent autocompletion and static analysis support in modern IDEs.

### Negative

-   **Dependency:** It adds a significant third-party dependency to the project. However, given its widespread adoption and stability, this is a low-risk decision.
-   **Performance:** There is a minor performance overhead associated with data validation compared to not validating at all. This is a necessary trade-off for ensuring data quality and is negligible compared to the I/O-bound operations of the ETL process (network requests, database writes).
