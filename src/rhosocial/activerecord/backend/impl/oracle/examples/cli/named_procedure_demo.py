# src/rhosocial/activerecord/backend/impl/oracle/examples/cli/named_procedure_demo.py
"""
Named Procedure CLI demo script.

Demonstrates how to invoke named procedures (Named Procedure) via Oracle CLI.

Usage:
    cd src/rhosocial/activerecord/backend/impl/oracle/examples
    python3 cli/named_procedure_demo.py

Or use CLI directly:
    python -m rhosocial.activerecord.backend.impl.oracle named-procedure \
        rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow.OrderProcessingProcedure \
        --param order_id=1 \
        --param user_id=100 \
        --param amount=99.99
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
This section demonstrates typical Named Procedure CLI usage.

### 1. List all named procedures in a module

```bash
python -m rhosocial.activerecord.backend.impl.oracle named-procedure \
    rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow \
    --list
```

### 2. View single procedure signature and parameters

```bash
python -m rhosocial.activerecord.backend.impl.oracle named-procedure \
    rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow.OrderProcessingProcedure \
    --describe
```

### 3. Dry-run: render each step's SQL without executing

```bash
python -m rhosocial.activerecord.backend.impl.oracle named-procedure \
    rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow.OrderProcessingProcedure \
    --dry-run \
    --param order_id=1 \
    --param user_id=100 \
    --param amount=99.99
```

### 4. Execute named procedure (AUTO transaction mode)

```bash
python -m rhosocial.activerecord.backend.impl.oracle named-procedure \
    rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow.OrderProcessingProcedure \
    --param order_id=1 \
    --param user_id=100 \
    --param amount=99.99 \
    --transaction auto
```

### 5. Execute named procedure (STEP transaction mode)

```bash
python -m rhosocial.activerecord.backend.impl.oracle named-procedure \
    rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow.OrderProcessingProcedure \
    --param order_id=1 \
    --param user_id=100 \
    --param amount=99.99 \
    --transaction step
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
    """Create and seed the workflow tables via the prepare_orders_demo helper.

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
    print("Named Procedure CLI Demo")
    print("=" * 60)

    # 1. List all named procedures in a module
    print("\n【1】List all named procedures in module")
    run_cli_command(
        [
            "named-procedure",
            "rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow",
            "--list",
        ]
    )

    # 2. View single procedure signature
    print("\n【2】View single procedure signature and parameters")
    run_cli_command(
        [
            "named-procedure",
            "rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow.OrderProcessingProcedure",
            "--describe",
        ]
    )

    # 3. Dry-run
    print("\n【3】Dry-run: render each step's SQL, don't execute")
    run_cli_command(
        [
            "named-procedure",
            "rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow.OrderProcessingProcedure",
            "--dry-run",
            "--param",
            "order_id=1",
            "--param",
            "user_id=100",
            "--param",
            "amount=99.99",
        ]
    )

    # 4. Execute named procedure (AUTO)
    print("\n【4】Execute named procedure (AUTO transaction mode)")
    prepare_demo_schema()
    run_cli_command(
        [
            "named-procedure",
            "rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow.OrderProcessingProcedure",
            "--param",
            "order_id=1",
            "--param",
            "user_id=100",
            "--param",
            "amount=99.99",
            "--transaction",
            "auto",
        ]
    )

    # 5. Execute named procedure (STEP)
    print("\n【5】Execute named procedure (STEP transaction mode)")
    prepare_demo_schema()
    run_cli_command(
        [
            "named-procedure",
            "rhosocial.activerecord.backend.impl.oracle.examples.named_procedures.order_workflow.OrderProcessingProcedure",
            "--param",
            "order_id=1",
            "--param",
            "user_id=100",
            "--param",
            "amount=99.99",
            "--transaction",
            "step",
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