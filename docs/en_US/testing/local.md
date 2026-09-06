# Local Oracle Testing

## Overview

This section describes how to set up a local Oracle testing environment.

## Running Oracle with Docker

Use the official Oracle Free images (e.g., `gvenzl/oracle-free` provides single-container Free edition with fast startup):

```bash
# Run Oracle Free container
docker run -d \
  --name oracle-test \
  -e ORACLE_PASSWORD=test \
  -e APP_USER=test \
  -e APP_USER_PASSWORD=test \
  -p 1521:1521 \
  gvenzl/oracle-free:23-slim

# Wait for Oracle to be ready
docker exec oracle-test bash -c 'until echo "SELECT 1 FROM dual" | sqlplus system/test@localhost:1521/FREEPDB1; do sleep 2; done'
```

## Using Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  oracle:
    image: gvenzl/oracle-free:23-slim
    environment:
      ORACLE_PASSWORD: test
      APP_USER: test
      APP_USER_PASSWORD: test
    ports:
      - "1521:1521"
    volumes:
      - oracle_data:/opt/oracle/oradata

volumes:
  oracle_data:
```

```bash
docker-compose up -d
```

> **Note**: Oracle containers are heavyweight and can take 1-3 minutes to become ready on first startup. Use the `sqlplus` health check above or a wait loop in CI before running tests.

## Service Name vs SID

Oracle Free databases typically use a service name (e.g., `FREEPDB1`). Use `OracleConnectionConfig` with the service name:

```python
config = OracleConnectionConfig(
    host='localhost',
    port=1521,
    database='FREEPDB1',   # Service name
    username='test',
    password='test',
)
```

## Running Tests

```bash
# Set environment variables
export ORACLE_HOST=localhost
export ORACLE_PORT=1521
export ORACLE_DATABASE=FREEPDB1
export ORACLE_USER=test
export ORACLE_PASSWORD=test

# Run tests
pytest tests/
```

💡 *AI Prompt:* "What is the difference between Docker and Docker Compose?"

