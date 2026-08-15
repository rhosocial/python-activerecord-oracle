#!/usr/bin/env bash
# ===========================================================================
# demo_async.sh — async migration execution (--async) for Oracle
#
# Scenarios:
#   - apply UP with --async
#   - dry-run with --async
#   - rollback DOWN with --async
#
# Requires: oracledb (async thin mode)
#
# Usage:
#   cd python-activerecord-oracle
#   PYTHONPATH=src \
#     bash src/rhosocial/activerecord/backend/impl/oracle/examples/named_migrations/demo_async.sh
# ===========================================================================
set -euo pipefail

if [ -d "./src" ]; then
    export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"
fi

MODULE="rhosocial.activerecord.backend.impl.oracle.examples.named_migrations"
# AsyncNamedMigration subclass — required by AsyncMigrationRunner (selected via --async).
FQN="${MODULE}.async_migrations.V001CreateUsersAsync"
STORE="./demo_oracle_async_mig.json"
VENV_PYTHON="${DEMO_VENV_PYTHON:-python3}"
PYTHON="$VENV_PYTHON -m rhosocial.activerecord.backend.impl.oracle"

# Connection info — safe defaults (localhost / empty password); override via env.
ORACLE_HOST="${ORACLE_HOST:-localhost}"
ORACLE_PORT="${ORACLE_PORT:-1521}"
ORACLE_SERVICE="${ORACLE_SERVICE:-XEPDB1}"
ORACLE_USER="${ORACLE_USER:-system}"
ORACLE_PASSWORD="${ORACLE_PASSWORD:-}"
CONN_ARGS="--host $ORACLE_HOST --port $ORACLE_PORT --service $ORACLE_SERVICE --user $ORACLE_USER --password $ORACLE_PASSWORD"

rm -f "$STORE"
echo "=== Async Migration (--async) for Oracle ==="
echo

echo "[1] Async dry-run (preview SQL, no changes):"
$PYTHON named-migration "$FQN" $CONN_ARGS \
    --direction up --dry-run --async
echo

echo "[2] Async apply UP:"
$PYTHON named-migration "$FQN" $CONN_ARGS \
    --direction up --async --record-store "$STORE"
echo

echo "[3] Async rollback DOWN:"
$PYTHON named-migration "$FQN" $CONN_ARGS \
    --direction down --async --record-store "$STORE"
echo

rm -f "$STORE"
echo "=== Async Migration Demo Complete for Oracle ==="
