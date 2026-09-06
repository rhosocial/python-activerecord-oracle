# rhosocial-activerecord Oracle Backend Documentation

> **AI Learning Assistant**: Key concepts in this documentation are marked with AI Prompt. When you encounter concepts you don't understand, you can ask the AI assistant directly.
>
> **Example:** "How does the Oracle backend handle transactions? How does it differ from SQLite?"

## Table of Contents

1. **[Introduction](introduction/README.md)**
    *   **[Oracle Backend Overview](introduction/README.md)**: Architecture, sync/async parity, and quick example
    *   **[Relationship with Core Library](introduction/relationship.md)**: How the backend integrates with rhosocial-activerecord
    *   **[Supported Versions](introduction/supported_versions.md)**: Oracle, Python, and dependency version matrix

2. **[Installation & Configuration](installation_and_configuration/README.md)**
    *   **[Installation Guide](installation_and_configuration/installation.md)**: pip installation and driver setup
    *   **[Connection Configuration](installation_and_configuration/configuration.md)**: Host, port, database, credentials, and advanced options
    *   **[SSL/TLS Configuration](installation_and_configuration/ssl.md)**: Secure connection settings
    *   **[Connection Management](installation_and_configuration/pool.md)**: Single-connection lifecycle, pool support, and FastAPI patterns
    *   **[Character Set / Encoding](installation_and_configuration/charset.md)**: Encoding configuration and best practices

3. **[Oracle Specific Features](backend_specific_features/README.md)**
    *   **[Field Types](backend_specific_features/field_types.md)**: Oracle-specific data types, type constants, and usage patterns
    *   **[Dialect Expressions](backend_specific_features/dialect.md)**: Oracle-specific SQL syntax, operators, functions, and DDL extensions
    *   **[Indexing](backend_specific_features/indexing.md)**: Index types, creation, and optimization strategies
    *   **[EXPLAIN](backend_specific_features/explain.md)**: Query execution plan analysis
    *   **[Introspection](backend_specific_features/introspection.md)**: Database metadata queries and schema inspection
    *   **[Partitioning](backend_specific_features/partition.md)**: Table partitioning strategies and lifecycle management

4. **[DDL Operations](ddl/README.md)**
    *   **[DDL Overview](ddl/README.md)**: Schema management, supported operations, and backend-specific DDL extensions

5. **[Transaction Support](transaction_support/README.md)**
    *   **[Transaction Overview](transaction_support/README.md)**: Transaction manager API and savepoints
    *   **[Isolation Levels](transaction_support/isolation_level.md)**: Oracle-specific isolation semantics
    *   **[Savepoint](transaction_support/savepoint.md)**: Nested transactions and conditional rollback
    *   **[Deadlock Handling](transaction_support/deadlock.md)**: Oracle deadlock detection, error codes, and retry strategies

6. **[Type Adapters](type_adapters/README.md)**
    *   **[Type Mapping](type_adapters/mapping.md)**: Oracle to Python type conversion table
    *   **[Custom Adapters](type_adapters/custom.md)**: Extending type support with custom adapters
    *   **[Timezone Handling](type_adapters/timezone.md)**: Timestamp and timezone configuration

7. **[Testing](testing/README.md)**
    *   **[Test Configuration](testing/configuration.md)**: Oracle-specific test setup
    *   **[Local Testing](testing/local.md)**: Docker-based local test environment

8. **[Troubleshooting](troubleshooting/README.md)**
    *   **[Connection Errors](troubleshooting/connection.md)**: Error codes, diagnostics, and automatic recovery
    *   **[Performance Issues](troubleshooting/performance.md)**: Slow query analysis and optimization

9. **[Scenarios](scenarios/README.md)**
    *   **[Parallel Workers](scenarios/parallel_workers.md)**: Oracle-specific concurrency characteristics and patterns

10. **[Customization](customization/README.md)**
    *   **[Custom Expressions](customization/custom_expressions.md)**: Creating new expression classes for Oracle-specific SQL
    *   **[Custom Data Types](customization/custom_types.md)**: Defining new DataType subclasses for custom column types
    *   **[Custom Type Adapters](customization/custom_adapters.md)**: Registering custom converters between Python and Oracle values

11. **[Command-Line Interface](cli/README.md)**
    *   **[CLI Overview](cli/README.md)**: Commands for querying, introspecting, and managing Oracle

> **Core Library Documentation**: For the complete ActiveRecord framework documentation (modeling, querying, relationships, performance, worker pools), refer to [rhosocial-activerecord documentation](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US).
