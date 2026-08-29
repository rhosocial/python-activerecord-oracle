# src/rhosocial/activerecord/backend/impl/oracle/examples/cli/named_query_demo.py
"""
Named Query CLI demo script.

Demonstrates how to invoke named queries (Named Query) via Oracle CLI.

Usage:
    cd src/rhosocial/activerecord/backend/impl/oracle/examples
    python3 cli/named_query_demo.py

Or use CLI directly:
    python -m rhosocial.activerecord.backend.impl.oracle named-expression \
        rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions.get_order \
        --param order_id=1
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import subprocess
import sys
from pathlib import Path

# Setup: ensure the package is importable from source (if not installed)
project_root = Path(__file__).resolve().parents[8]
src_dir = project_root / "src"
sys.path.insert(0, str(src_dir))

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
"""
This section demonstrates typical Named Query CLI usage.

### 1. List all named queries in a module

```bash
python -m rhosocial.activerecord.backend.impl.oracle named-expression \
    rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions \
    --list
```

### 2. View single query signature and parameters

```bash
python -m rhosocial.activerecord.backend.impl.oracle named-expression \
    rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions.get_order \
    --describe
```

### 3. Dry-run: render SQL without executing

```bash
python -m rhosocial.activerecord.backend.impl.oracle named-expression \
    rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions.get_order \
    --dry-run \
    --param order_id=1
```

### 4. Execute a named query

```bash
python -m rhosocial.activerecord.backend.impl.oracle named-expression \
    rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions.get_order \
    --param order_id=1
```

### 5. Execute EXPLAIN plan

```bash
python -m rhosocial.activerecord.backend.impl.oracle named-expression \
    rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions.get_order \
    --explain \
    --param order_id=1
```
"""


def run_cli_command(args):
    """Execute CLI command and print output."""
    cmd = [sys.executable, "-m", "rhosocial.activerecord.backend.impl.oracle"] + args
    print(f"\n{'=' * 60}")
    print(f"Running: {' '.join(cmd)}")
    print("=" * 60)
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
    print(result.stdout)
    if result.stderr:
        print(f"STDERR: {result.stderr}")
    return result.returncode


def run_python_snippet(code):
    """Run a plain Python snippet (schema preparation helper).

    The subprocess runs with cwd=project_root so the module is importable
    from source via PYTHONPATH.
    """
    print(f"\n{'=' * 60}")
    print("Running: python -c <prepare_demo_schema>")
    print("=" * 60)
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        cwd=str(project_root),
    )
    print(result.stdout)
    if result.stderr:
        print(f"STDERR: {result.stderr}")
    return result.returncode


def prepare_demo_schema():
    """Create and seed the demo tables via the prepare_orders_demo helper.

    The Oracle CLI's query subcommand executes a single statement only (no
    --executescript), so the demo uses the order_expressions module's own
    prepare_orders_demo helper to create the schema and seed data. This is
    the Oracle equivalent of the SQLite demo's query --executescript step.
    """
    code = (
        "import os;"
        "from rhosocial.activerecord.backend.impl.oracle import OracleBackend, OracleConnectionConfig;"
        "from rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions "
        "import prepare_orders_demo;"
        "c = OracleConnectionConfig(host=os.getenv('ORACLE_HOST', '127.0.0.1'), "
        "port=int(os.getenv('ORACLE_PORT', '1521')), username=os.getenv('ORACLE_USER', 'system'), "
        "password=os.getenv('ORACLE_PASSWORD', 'Password1!'), "
        "service_name=os.getenv('ORACLE_SERVICE', 'FREEPDB1'));"
        "b = OracleBackend(connection_config=c);"
        "b.connect();"
        "prepare_orders_demo(b);"
        "b.disconnect()"
    )
    run_python_snippet(code)


def main():
    print("Named Query CLI Demo")
    print("=" * 60)

    # 1. List all named queries in a module
    print("\n【1】List all named queries in module")
    run_cli_command(
        [
            "named-expression",
            "rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions",
            "--list",
        ]
    )

    # 2. View single query signature
    print("\n【2】View single query signature and parameters")
    run_cli_command(
        [
            "named-expression",
            "rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions.get_order",
            "--describe",
        ]
    )

    # 3. Dry-run: render SQL only
    print("\n【3】Dry-run: render SQL, don't execute")
    run_cli_command(
        [
            "named-expression",
            "rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions.get_order",
            "--dry-run",
            "--param",
            "order_id=1",
        ]
    )

    # 4. Execute named query
    # The named expression only builds SQL; the tables must exist in the
    # target database. Pre-populate the schema via the query subcommand.
    print("\n【4】Execute named query")
    prepare_demo_schema()
    run_cli_command(
        [
            "named-expression",
            "rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions.get_order",
            "--param",
            "order_id=1",
        ]
    )

    # 5. View another query
    print("\n【5】View check_inventory query")
    run_cli_command(
        [
            "named-expression",
            "rhosocial.activerecord.backend.impl.oracle.examples.named_expressions.order_expressions.check_inventory",
            "--describe",
        ]
    )

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
# No cleanup needed - CLI commands are self-contained