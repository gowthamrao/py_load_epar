# 3. Use Testcontainers for Database Integration Testing

Date: 2025-09-07

## Status

Accepted

## Context

The FRD mandates that integration tests are mandatory for all `IDatabaseAdapter` implementations. These tests must validate the native bulk loading mechanism, transaction management, and load strategies. The testing environment must be reproducible and realistic.

## Decision

We will use the **Testcontainers** library for Python to conduct integration tests against our database adapters.

Specifically, for the `PostgresAdapter`, the test suite will use `testcontainers.postgresql.PostgresContainer` to programmatically start and stop an ephemeral PostgreSQL database instance in a Docker container for each test run (or test module). The tests will connect to this containerized database, execute the test logic (e.g., loading data), and then the container will be automatically destroyed.

## Consequences

### Positive

-   **High Fidelity Testing:** Tests are run against a real PostgreSQL database, not a mock or an in-memory substitute (like SQLite). This ensures that the adapter's logic, including the use of native SQL features like `COPY` and `INSERT ON CONFLICT`, is tested accurately.
-   **Reproducibility:** The test environment is defined in code and is identical on every run, whether on a developer's local machine or in a CI/CD pipeline. This eliminates "works on my machine" problems.
-   **Test Isolation:** Each test function can get its own clean, isolated database instance. This prevents tests from interfering with each other and ensures there is no shared, mutable state between test runs.
-   **No External Dependencies:** Developers do not need to manually install or manage a local PostgreSQL server for testing. They only need a working Docker installation.
-   **Extensibility:** This approach works for any database that has a Docker image, making it easy to write integration tests for future adapters (e.g., for MySQL, Redshift Local, etc.).

### Negative

-   **Dependency on Docker:** The machine running the tests must have Docker installed and running. This is a reasonable requirement for modern development and CI/CD environments.
-   **Slower Test Execution:** Integration tests using Testcontainers are inherently slower than unit tests because they involve the overhead of starting a Docker container and a real database service. To mitigate this, we will clearly separate unit and integration tests (using `pytest` markers), allowing developers to run only the fast unit tests during most of the development cycle.
-   **Resource Usage:** Running Docker containers consumes more system resources (CPU, RAM) than running simple unit tests.
