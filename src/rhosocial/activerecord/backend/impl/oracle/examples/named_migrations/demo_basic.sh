#!/usr/bin/env bash
# ===========================================================================
# demo_basic.sh — single migration basic operations (Oracle)
#
# Scenarios:
#   - apply / rollback a single migration
#   - dry-run preview
#   - duplicate execution protection
#
# Usage:
#   cd python-activerecord-oracle
#   DEMO_VENV_PYTHON=.venv3.12/bin/python \
#     PYTHONPATH=src \
#     bash src/rhosocial/activerecord/backend/impl/oracle/examples/named_migrations/demo_basic.sh
# ===========================================================================
set -euo pipefail

if [ -d "./src" ]; then
    export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"
fi

MODULE="rhosocial.activerecord.backend.impl.oracle.examples.named_migrations"
FQN="${MODULE}.migrations.V001CreateUsers"
STORE="./demo_oracle_basic_mig.json"
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
echo "=== Single Migration Basic Operations (Oracle) ==="
echo

echo "[1] List all migrations in the module:"
$PYTHON named-migration "${MODULE}.migrations" --list -o table
echo

echo "[2] Describe V001CreateUsers (--describe):"
$PYTHON named-migration "$FQN" --describe
echo

echo "[3] Dry-run preview (no actual changes):"
$PYTHON named-migration "$FQN" $CONN_ARGS --direction up --dry-run
echo

echo "[4] Apply UP (create users table):"
$PYTHON named-migration "$FQN" $CONN_ARGS --direction up --record-store "$STORE"
echo

echo "[5] Record store contents:"
cat "$STORE"
echo
echo

echo "[6] Duplicate UP (should be rejected):"
$PYTHON named-migration "$FQN" $CONN_ARGS --direction up --record-store "$STORE" 2>&1 || true
echo

echo "[7] Apply DOWN (drop users table):"
$PYTHON named-migration "$FQN" $CONN_ARGS --direction down --record-store "$STORE"
echo

echo "[8] Record store after rollback:"
cat "$STORE"
echo

rm -f "$STORE"
echo "=== Single Migration Basic Operations Complete (Oracle) ==="
