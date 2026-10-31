# src/rhosocial/activerecord/backend/impl/oracle/examples/introspection_schema_objects.py
"""Schema-object introspection: sequences, synonyms, packages.

Oracle keeps more than tables in a schema. This example walks the three
schema-object families the Oracle introspector answers for, through
``backend.introspector`` -- the same entry point the CLI uses:

1. ``list_sequences`` / ``get_sequence_info``  -- ``ALL_SEQUENCES``
2. ``list_synonyms`` / ``get_synonym_info``    -- ``ALL_SYNONYMS``
3. ``list_packages``                           -- ``ALL_OBJECTS``

It then shows what the introspector's cache buys: the warm pass for each
family asks the server *zero* questions. That is measured, not asserted --
the executor is wrapped in a counter and the cold and warm passes are
printed side by side with their wall times.

Connection settings come from the test suite's own configuration, in this
order:

1. ``ORACLE_SCENARIOS_CONFIG_PATH``, or this repo's ``tests/config/
   oracle_scenarios.yaml``, loaded through ``tests/providers/
   scenarios.py`` -- the same loader the tests use.
2. ``ORACLE_HOST`` / ``ORACLE_PORT`` / ``ORACLE_USER`` /
   ``ORACLE_PASSWORD`` / ``ORACLE_SERVICE`` environment variables.

If no server answers, the example prints what to start instead of
inventing rows, and exits non-zero.

Run it::

    python -m rhosocial.activerecord.backend.impl.oracle.examples.introspection_schema_objects

Oracle Version Support: 12c+ (``ALL_SEQUENCES``, ``ALL_SYNONYMS`` and
``ALL_OBJECTS`` all predate it).
"""

import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from rhosocial.activerecord.backend.errors import ConnectionError

from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig

# --------------------------------------------------------------------- #
# Resolving the connection the way the tests do
# --------------------------------------------------------------------- #


def _from_scenarios() -> Optional[Tuple[str, OracleConnectionConfig]]:
    """Load the first scenario from the test suite's provider, if present.

    The tests register their scenarios through ``tests/providers/
    scenarios.py``, which honours ``ORACLE_SCENARIOS_CONFIG_PATH`` and
    otherwise reads the repo's ``tests/config/oracle_scenarios.yaml``.
    Reusing that loader means a machine that runs the tests runs this
    example against the same server with no extra configuration.
    """
    tests_dir = Path(__file__).resolve().parents[7] / "tests"
    if not (tests_dir / "providers" / "scenarios.py").is_file():
        return None
    # ``providers`` is only importable with the tests directory on the
    # path; the tests set ``PYTHONPATH=tests`` for the same reason.
    if str(tests_dir) not in sys.path:
        sys.path.insert(0, str(tests_dir))
    try:
        from providers.scenarios import SCENARIO_MAP

        if not SCENARIO_MAP:
            return None
        name = next(iter(SCENARIO_MAP))
        return name, OracleConnectionConfig(**SCENARIO_MAP[name])
    except Exception as exc:
        print(
            f"note: test scenario configuration unusable ({exc}); "
            "falling back to environment variables"
        )
        return None


def _from_environment() -> Tuple[str, OracleConnectionConfig]:
    """Build a config from the ``ORACLE_*`` environment variables."""
    return (
        "environment",
        OracleConnectionConfig(
            host=os.getenv("ORACLE_HOST", "localhost"),
            port=int(os.getenv("ORACLE_PORT", "1521")),
            username=os.getenv("ORACLE_USER", "system"),
            password=os.getenv("ORACLE_PASSWORD", ""),
            service_name=os.getenv("ORACLE_SERVICE", "XEPDB1"),
        ),
    )


def resolve_connection() -> Tuple[str, OracleConnectionConfig]:
    """Return the scenario name and config to connect with."""
    resolved = _from_scenarios()
    return resolved if resolved is not None else _from_environment()


# --------------------------------------------------------------------- #
# Counting the questions the introspector asks
# --------------------------------------------------------------------- #


class CountingExecutor:
    """A pass-through executor wrapper that counts ``execute()`` calls.

    The introspector never asks the server for a cached answer, so the
    cold pass shows one question per family and the warm pass shows none.
    The count makes the cache contract visible; the wall time is what it
    saves.
    """

    def __init__(self, executor: Any) -> None:
        self._executor = executor
        self.questions = 0

    def execute(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        self.questions += 1
        return self._executor.execute(sql, params)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._executor, name)


# --------------------------------------------------------------------- #
# Printing
# --------------------------------------------------------------------- #


def print_sequences(sequences: List[Any]) -> None:
    print("\nSequences (ALL_SEQUENCES)")
    print("-" * 60)
    if not sequences:
        print("  (none in this schema)")
    for seq in sequences:
        # ``last_number`` is the next value the *current cache block* would
        # hand out: with CACHE 20 and 3 values consumed it reads 20, not
        # 3 and not the 4th. See OracleSequenceInfo's docstring.
        print(f"  {seq.owner}.{seq.name}")
        print(
            f"    range {seq.min_value} .. {seq.max_value}, "
            f"step {seq.increment}, cache {seq.cache_size}"
        )
        print(
            f"    cycle={seq.cycle}  order={seq.order}  "
            f"last_number={seq.last_number} (next cached value)"
        )


def print_synonyms(synonyms: List[Any], owner: str) -> None:
    print(f"\nSynonyms owned by {owner} (ALL_SYNONYMS)")
    print("-" * 60)
    if not synonyms:
        print("  (none)")
    for syn in synonyms:
        target = f"{syn.table_owner}.{syn.table_name}"
        if syn.db_link:
            target = f"{target}@{syn.db_link}"
        scope = "public" if syn.is_public else "private"
        print(f"  {syn.owner}.{syn.name} -> {target}  [{scope}]")


def print_packages(packages: List[Any]) -> None:
    print("\nPackages (ALL_OBJECTS, OBJECT_TYPE = 'PACKAGE')")
    print("-" * 60)
    if not packages:
        print("  (none)")
    for pkg in packages:
        print(
            f"  {pkg.owner}.{pkg.name}  status={pkg.status}  "
            f"created={pkg.created_time}  last_ddl={pkg.last_ddl_time}"
        )


def print_timing(
    name: str, cold_questions: int, cold_seconds: float, warm_questions: int, warm_seconds: float
) -> None:
    """Report a family's cold and warm pass: questions and wall time."""
    print(f"\nCache: {name}")
    print(f"  cold : {cold_questions} question(s), {cold_seconds * 1000:.2f} ms")
    print(
        f"  warm : {warm_questions} question(s), "
        f"{warm_seconds * 1000:.2f} ms   <- served from the cache"
    )


# --------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------- #


def _timed_pass(
    method: str, introspector: Any, counting: CountingExecutor, owner: str
) -> Tuple[Any, int, float]:
    """Run one family's cold call, then its warm call, and time both.

    Args:
        method: One of ``list_sequences``, ``list_synonyms``,
            ``list_packages`` -- the introspector method for a family.

    Returns:
        The family's rows, the warm question count (expected: zero) and
        the cold wall-clock seconds. The warm timing is printed inside
        so the pair is read as one measurement.
    """
    counting.questions = 0
    started = time.perf_counter()
    rows = getattr(introspector, method)(owner=owner)
    cold_seconds = time.perf_counter() - started
    cold_questions = counting.questions

    counting.questions = 0
    started = time.perf_counter()
    getattr(introspector, method)(owner=owner)
    warm_seconds = time.perf_counter() - started
    warm_questions = counting.questions

    print_timing(method, cold_questions, cold_seconds, warm_questions, warm_seconds)
    return rows, warm_questions, cold_seconds


def main() -> int:
    source, config = resolve_connection()
    print("=" * 60)
    print("Oracle schema-object introspection")
    print("=" * 60)
    print(f"Scenario : {source}")
    print(f"Server    : {config.host}:{config.port} (service {config.service_name})")
    print(f"User      : {config.username}")

    backend = OracleBackend(connection_config=config)
    try:
        backend.connect()
    except ConnectionError as exc:
        print(f"\nCould not reach the Oracle server: {exc}", file=sys.stderr)
        print("\nThis example needs a live Oracle instance. Either:", file=sys.stderr)
        print(
            "  point it at the test suite's scenario -- start the Oracle "
            "container mapped to the port in",
            file=sys.stderr,
        )
        print(
            "  tests/config/oracle_scenarios.yaml (or set ORACLE_SCENARIOS_CONFIG_PATH),",
            file=sys.stderr,
        )
        print(
            "  set ORACLE_HOST / ORACLE_PORT / ORACLE_USER / "
            "ORACLE_PASSWORD / ORACLE_SERVICE yourself.",
            file=sys.stderr,
        )
        print(
            "No rows are printed when the server is absent: an example that fabricates",
            file=sys.stderr,
        )
        print("teaches nothing.", file=sys.stderr)
        return 1

    try:
        backend.introspect_and_adapt()
        introspector = backend.introspector
        owner = config.username.upper()

        # Count every question the introspector asks of the server.
        counting = CountingExecutor(introspector._executor)
        introspector._executor = counting

        sequences, warm_seq, _ = _timed_pass("list_sequences", introspector, counting, owner)
        synonyms, warm_syn, _ = _timed_pass("list_synonyms", introspector, counting, owner)
        packages, warm_pkg, _ = _timed_pass("list_packages", introspector, counting, owner)

        print_sequences(sequences)
        print_synonyms(synonyms, owner)
        print_packages(packages)

        # Public synonyms are their own owner, never folded into a
        # schema's list: a caller inspecting AR_CRM gets AR_CRM's aliases,
        # not every global alias on the instance.
        public = introspector.list_synonyms(owner="PUBLIC")
        print(
            f"\nPublic synonyms (owner='PUBLIC'): {len(public)} listed; "
            "a schema's own list never contains them"
        )

        # One object, by name -- ``get_sequence_info`` reads a single row.
        if sequences:
            first = sequences[0]
            single = introspector.get_sequence_info(first.name, owner=owner)
            print(
                f"\nget_sequence_info('{first.name}') -> "
                f"{single.name if single else 'not found'}"
            )

        print(
            f"\nTotals: warm questions asked after the first pass -- "
            f"sequences={warm_seq}, synonyms={warm_syn}, packages={warm_pkg}"
        )
        print("Zero means the introspector's cache answered instead of the server:")
        print("the difference between a round trip and a dictionary lookup.")
    finally:
        backend.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
