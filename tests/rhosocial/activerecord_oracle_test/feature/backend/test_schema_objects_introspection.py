# tests/rhosocial/activerecord_oracle_test/feature/backend/test_schema_objects_introspection.py
"""Schema-object introspection: sequences, synonyms, packages.

Two halves, same as the rest of this directory:

Offline -- the SQL builders, the ``_parse_*`` helpers, the cache contract
and the sync/async signature parity are driven with synthetic
dictionary rows and a recording executor. No connection is opened.

Live (pinned to the ``oracle_schema_dml`` xdist group) -- a throwaway
schema user is provisioned with a sequence, a private synonym and, when
the connected user may create procedures, a trivial package; the
introspector then reads them back through ``backend.introspector``. When
package creation is refused for lack of privilege the package half
degrades to "the query is well formed and returns a list", never to a
fabricated row.
"""

import inspect
import json
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest
from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.introspection.types import IntrospectionScope

from rhosocial.activerecord.backend.impl.oracle.dialect import OracleDialect
from rhosocial.activerecord.backend.impl.oracle.introspection.introspector import (
    AsyncOracleIntrospector,
    SyncOracleIntrospector,
)
from rhosocial.activerecord.backend.impl.oracle.introspection.types import (
    OraclePackageInfo,
    OracleSequenceInfo,
    OracleSynonymInfo,
)

pytestmark = pytest.mark.xdist_group("oracle_schema_dml")

OBJECT_OWNER = "AR_OBJ"
OBJECT_OWNER_PASSWORD = "Rh0social#2026"
SEQUENCE_NAME = "SEQ_GATE"
SYNONYM_NAME = "SYN_GATE"
PACKAGE_NAME = "PKG_GATE"


# --------------------------------------------------------------------- #
# Offline collaborators
# --------------------------------------------------------------------- #


class StubBackend:
    """The two backend attributes the introspector mixin actually reads."""

    def __init__(self, username: str = "ar_crm") -> None:
        self.config = SimpleNamespace(username=username)
        self._version = (23, 0, 0)
        self.dialect = OracleDialect(version=(23, 0, 0))


class RecordingExecutor:
    """A sync executor that records questions and replays fixed rows."""

    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None) -> None:
        self.rows = rows if rows is not None else []
        self.questions: List[Tuple[str, Tuple]] = []

    def execute(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        self.questions.append((sql, params))
        return self.rows


class AsyncRecordingExecutor:
    """The async twin of :class:`RecordingExecutor`."""

    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None) -> None:
        self.rows = rows if rows is not None else []
        self.questions: List[Tuple[str, Tuple]] = []

    async def execute(self, sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        self.questions.append((sql, params))
        return self.rows


SEQUENCE_ROW = {
    "SEQUENCE_OWNER": "AR_OBJ",
    "SEQUENCE_NAME": "SEQ_GATE",
    "MIN_VALUE": 10,
    "MAX_VALUE": 100000,
    "INCREMENT_BY": 5,
    "CYCLE_FLAG": "Y",
    "ORDER_FLAG": "Y",
    "CACHE_SIZE": 20,
    "LAST_NUMBER": 100,
}

SYNONYM_ROW = {
    "OWNER": "AR_OBJ",
    "SYNONYM_NAME": "SYN_GATE",
    "TABLE_OWNER": "AR_OBJ",
    "TABLE_NAME": "SEQ_GATE",
    "DB_LINK": None,
}

PUBLIC_SYNONYM_ROW = {
    "OWNER": "PUBLIC",
    "SYNONYM_NAME": "PUB_GATE",
    "TABLE_OWNER": "AR_OBJ",
    "TABLE_NAME": "SEQ_GATE",
    "DB_LINK": "REMOTE",
}

PACKAGE_ROW = {
    "OWNER": "AR_OBJ",
    "OBJECT_NAME": "PKG_GATE",
    "STATUS": "VALID",
    "CREATED": "2026-10-23 10:00:00",
    "LAST_DDL_TIME": "2026-10-23 10:05:00",
}


def make_sync(
    rows: Optional[List[Dict[str, Any]]] = None, username: str = "ar_crm"
) -> Tuple[SyncOracleIntrospector, RecordingExecutor]:
    executor = RecordingExecutor(rows)
    return SyncOracleIntrospector(StubBackend(username), executor), executor


def make_async(
    rows: Optional[List[Dict[str, Any]]] = None, username: str = "ar_crm"
) -> Tuple[AsyncOracleIntrospector, AsyncRecordingExecutor]:
    executor = AsyncRecordingExecutor(rows)
    return AsyncOracleIntrospector(StubBackend(username), executor), executor


# --------------------------------------------------------------------- #
# SQL builders
# --------------------------------------------------------------------- #


class TestSqlBuilders:
    """The builders ask the dictionary, and every value travels as a bind.

    The dialect spells the tree, so what is asserted here is the *contract* --
    which view is read, and that a caller's value is compared against a
    parameter rather than being part of the text -- and not the identifier
    spelling the dialect chose. ``_squash`` drops whitespace and quoting, so a
    quoted ``"SEQUENCE_OWNER"`` and a bare one are the same name here: pinning
    either spelling would test the dialect rather than this layer.
    """

    @staticmethod
    def _squash(sql):
        """Whitespace-free, quote-free SQL, for shape assertions."""
        return "".join(sql.lower().split()).replace('"', "")

    def test_sequence_list_reads_all_sequences_and_binds_owner(self):
        insp, _ = make_sync()
        sql, params = insp._build_sequence_list_sql("ar_obj")
        assert "all_sequences" in self._squash(sql)
        assert "sequence_owner=?" in self._squash(sql)
        assert "ar_obj" not in sql.lower()
        assert params == ("AR_OBJ",)

    def test_sequence_info_binds_name_and_owner(self):
        insp, _ = make_sync()
        sql, params = insp._build_sequence_info_sql("seq_gate", "ar_obj")
        assert "sequence_name=?" in self._squash(sql)
        assert "sequence_owner=?" in self._squash(sql)
        assert "seq_gate" not in sql.lower()
        assert "ar_obj" not in sql.lower()
        assert params == ("SEQ_GATE", "AR_OBJ")

    def test_synonym_queries_read_all_synonyms(self):
        insp, _ = make_sync()

        list_sql, list_params = insp._build_synonym_list_sql("public")
        assert "all_synonyms" in self._squash(list_sql)
        assert "owner=?" in self._squash(list_sql)
        assert "public" not in list_sql.lower()
        assert list_params == ("PUBLIC",)

        info_sql, info_params = insp._build_synonym_info_sql("syn_gate", "ar_obj")
        assert "synonym_name=?" in self._squash(info_sql)
        assert "owner=?" in self._squash(info_sql)
        assert info_params == ("SYN_GATE", "AR_OBJ")

    def test_package_query_reads_specifications_only(self):
        insp, _ = make_sync()
        sql, params = insp._build_package_list_sql("ar_obj")
        squashed = self._squash(sql)
        assert "all_objects" in squashed
        assert "object_type='package'" in squashed
        # A body row for the same name would double every package.
        assert "packagebody" not in squashed.replace("'", "")
        assert "ar_obj" not in sql.lower()
        assert params == ("AR_OBJ",)

    def test_the_package_projection_carries_what_the_parser_reads(self):
        """``created`` is projected because ``_parse_packages`` reads it.

        Dropping it would not fail: the parser would report ``None`` for a
        column the server was never asked for, which is a silent wrong answer.
        """
        insp, _ = make_sync()
        sql, _ = insp._build_package_list_sql("ar_obj")
        squashed = self._squash(sql)
        assert "created," in squashed
        assert "last_ddl_time" in squashed

    def test_a_name_carrying_sql_is_bound_as_data(self):
        insp, _ = make_sync()
        sql, params = insp._build_synonym_info_sql("s' OR '1'='1", "ar_obj")
        assert "1'='1" not in sql
        assert params == ("S' OR '1'='1", "AR_OBJ")


# --------------------------------------------------------------------- #
# Parsers
# --------------------------------------------------------------------- #


class TestParsers:
    def test_sequences_map_every_documented_field(self):
        insp, _ = make_sync()
        (seq,) = insp._parse_sequences([SEQUENCE_ROW])
        assert isinstance(seq, OracleSequenceInfo)
        assert (seq.name, seq.owner) == ("SEQ_GATE", "AR_OBJ")
        assert (seq.min_value, seq.max_value) == (10, 100000)
        assert (seq.increment, seq.cache_size) == (5, 20)
        assert seq.cycle is True
        assert seq.order is True
        assert seq.last_number == 100

    def test_flags_default_to_false_when_the_view_says_no(self):
        insp, _ = make_sync()
        row = dict(SEQUENCE_ROW, CYCLE_FLAG="N", ORDER_FLAG="N")
        (seq,) = insp._parse_sequences([row])
        assert seq.cycle is False and seq.order is False

    def test_sequence_info_is_none_for_an_empty_result(self):
        insp, _ = make_sync()
        assert insp._parse_sequence_info([], "SEQ_GATE", "AR_OBJ") is None

    def test_synonyms_carry_the_target_and_public_flag(self):
        insp, _ = make_sync()
        private, public = insp._parse_synonyms([SYNONYM_ROW, PUBLIC_SYNONYM_ROW])
        assert (private.table_owner, private.table_name) == ("AR_OBJ", "SEQ_GATE")
        assert private.db_link is None and private.is_public is False
        assert public.is_public is True
        assert public.db_link == "REMOTE"

    def test_synonym_info_is_none_for_an_empty_result(self):
        insp, _ = make_sync()
        assert insp._parse_synonym_info([], "SYN_GATE", "AR_OBJ") is None

    def test_packages_carry_status_and_timestamps(self):
        insp, _ = make_sync()
        (package,) = insp._parse_packages([PACKAGE_ROW])
        assert isinstance(package, OraclePackageInfo)
        assert (package.name, package.owner, package.status) == (
            "PKG_GATE",
            "AR_OBJ",
            "VALID",
        )
        assert package.created_time == "2026-10-23 10:00:00"
        assert package.last_ddl_time == "2026-10-23 10:05:00"

    def test_missing_timestamps_stay_none(self):
        insp, _ = make_sync()
        row = dict(PACKAGE_ROW, CREATED=None, LAST_DDL_TIME=None)
        (package,) = insp._parse_packages([row])
        assert package.created_time is None and package.last_ddl_time is None


# --------------------------------------------------------------------- #
# Value objects
# --------------------------------------------------------------------- #


class TestValueObjects:
    @pytest.mark.parametrize(
        "instance,field",
        [
            (OracleSequenceInfo(name="S", owner="O"), "name"),
            (OracleSynonymInfo(name="S", owner="O", table_owner="T", table_name="N"), "name"),
            (OraclePackageInfo(name="P", owner="O", status="VALID"), "name"),
        ],
    )
    def test_they_are_frozen(self, instance, field):
        with pytest.raises(FrozenInstanceError):
            setattr(instance, field, "changed")

    def test_last_number_is_documented_as_the_next_value(self):
        """The docstring is part of the contract: it is the misreading guard."""
        doc = " ".join((OracleSequenceInfo.__doc__ or "").split()).lower()
        assert "next" in doc and "not the last one it handed out" in doc

    def test_public_synonyms_are_documented_as_owner_public(self):
        doc = " ".join((OracleSynonymInfo.__doc__ or "").split()).lower()
        assert "public" in doc and "owner" in doc


# --------------------------------------------------------------------- #
# Caching
# --------------------------------------------------------------------- #


class TestCache:
    def test_first_call_asks_once_and_the_warm_call_asks_nothing(self):
        insp, executor = make_sync([SEQUENCE_ROW])
        cold = insp.list_sequences("AR_OBJ")
        assert len(executor.questions) == 1

        warm = insp.list_sequences("AR_OBJ")
        assert len(executor.questions) == 1
        assert warm is cold

    def test_owner_case_folds_to_one_entry(self):
        insp, executor = make_sync([SEQUENCE_ROW])
        insp.list_sequences("ar_obj")
        insp.list_sequences("AR_OBJ")
        assert len(executor.questions) == 1

    def test_different_owners_are_different_questions(self):
        insp, executor = make_sync([SEQUENCE_ROW])
        insp.list_sequences("AR_OBJ")
        insp.list_sequences("AR_CRM")
        assert len(executor.questions) == 2
        assert executor.questions[0][1] == ("AR_OBJ",)
        assert executor.questions[1][1] == ("AR_CRM",)

    @pytest.mark.parametrize(
        "method,rows",
        [
            ("list_synonyms", [SYNONYM_ROW]),
            ("list_packages", [PACKAGE_ROW]),
        ],
    )
    def test_the_other_families_are_warm_too(self, method, rows):
        insp, executor = make_sync(rows)
        first = getattr(insp, method)("AR_OBJ")
        second = getattr(insp, method)("AR_OBJ")
        assert len(executor.questions) == 1
        assert second is first

    def test_synonyms_and_packages_do_not_answer_each_other(self):
        insp, executor = make_sync([SYNONYM_ROW])
        insp.list_synonyms("AR_OBJ")
        insp.list_packages("AR_OBJ")
        insp.list_synonyms("AR_OBJ")
        insp.list_packages("AR_OBJ")
        assert len(executor.questions) == 2
        assert len(insp.cache) == 2

    def test_get_sequence_info_caches_the_found_row(self):
        insp, executor = make_sync([SEQUENCE_ROW])
        first = insp.get_sequence_info("seq_gate", "ar_obj")
        second = insp.get_sequence_info("SEQ_GATE", "AR_OBJ")
        assert first is second
        assert len(executor.questions) == 1

    def test_get_sequence_info_does_not_cache_absence(self):
        """``None`` is an answer, not a value the cache may store as a hit."""
        insp, executor = make_sync([])
        assert insp.get_sequence_info("GHOST", "AR_OBJ") is None
        assert insp.get_sequence_info("GHOST", "AR_OBJ") is None
        assert len(executor.questions) == 2

    def test_keys_carry_the_scope_and_the_family(self):
        insp, _ = make_sync()
        keys = {
            "sequences": insp.cache.key(IntrospectionScope.SEQUENCE, schema="AR_OBJ"),
            "synonyms": insp.cache.key(
                IntrospectionScope.SCHEMA, schema="AR_OBJ", extra="oracle:synonyms"
            ),
            "packages": insp.cache.key(
                IntrospectionScope.SCHEMA, schema="AR_OBJ", extra="oracle:packages"
            ),
        }
        assert len(set(keys.values())) == 3
        assert keys["sequences"].startswith("sequence:")
        assert "schema:AR_OBJ" in keys["synonyms"]


# --------------------------------------------------------------------- #
# Sync / async parity
# --------------------------------------------------------------------- #


PARITY_METHODS = [
    "list_sequences",
    "get_sequence_info",
    "list_synonyms",
    "get_synonym_info",
    "list_packages",
]


class TestParity:
    @pytest.mark.parametrize("name", PARITY_METHODS)
    def test_sync_and_async_signatures_are_identical(self, name):
        sync_sig = inspect.signature(getattr(SyncOracleIntrospector, name))
        async_sig = inspect.signature(getattr(AsyncOracleIntrospector, name))
        assert sync_sig == async_sig

    @pytest.mark.parametrize("name", PARITY_METHODS)
    def test_the_async_half_is_actually_async(self, name):
        assert inspect.iscoroutinefunction(getattr(AsyncOracleIntrospector, name))
        assert not inspect.iscoroutinefunction(getattr(SyncOracleIntrospector, name))

    async def test_async_listing_caches_the_same_way(self):
        insp, executor = make_async([SEQUENCE_ROW])
        cold = await insp.list_sequences("ar_obj")
        warm = await insp.list_sequences("AR_OBJ")
        assert len(executor.questions) == 1
        assert warm is cold
        assert cold[0].name == "SEQ_GATE"

    async def test_async_get_sequence_info_returns_none_when_absent(self):
        insp, executor = make_async([])
        assert await insp.get_sequence_info("GHOST", "AR_OBJ") is None
        assert len(executor.questions) == 1


# --------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------- #


class FakeIntrospector:
    def __init__(self) -> None:
        self.calls: List[Tuple[str, Optional[str]]] = []

    def list_sequences(self, owner: Optional[str] = None):
        self.calls.append(("sequences", owner))
        return [OracleSequenceInfo(name="SEQ_GATE", owner=owner or "AR_CRM")]

    def list_synonyms(self, owner: Optional[str] = None):
        self.calls.append(("synonyms", owner))
        return [
            OracleSynonymInfo(
                name="SYN_GATE",
                owner=owner or "AR_CRM",
                table_owner="AR_CRM",
                table_name="SEQ_GATE",
            )
        ]

    def list_packages(self, owner: Optional[str] = None):
        self.calls.append(("packages", owner))
        return [OraclePackageInfo(name="PKG_GATE", owner=owner or "AR_CRM", status="VALID")]


def fake_sync_backend(monkeypatch, introspector: Any) -> None:
    from rhosocial.activerecord.backend.impl.oracle.cli import introspect as cli

    class FakeBackend:
        def __init__(self, connection_config: Any = None) -> None:
            self.introspector = introspector
            self._connection: Optional[object] = object()

        def connect(self) -> None:
            pass

        def disconnect(self) -> None:
            pass

    monkeypatch.setattr(cli, "OracleBackend", FakeBackend)


class TestCli:
    @pytest.mark.parametrize(
        "itype,title,key,value",
        [
            ("sequences", "Sequences", "last_number", "SEQ_GATE"),
            ("synonyms", "Synonyms", "table_name", "SYN_GATE"),
            ("packages", "Packages", "status", "PKG_GATE"),
        ],
    )
    def test_main_renders_the_schema_object_types_as_json(
        self, monkeypatch, capsys, itype, title, key, value
    ):
        from rhosocial.activerecord.backend.impl.oracle.__main__ import main

        insp = FakeIntrospector()
        fake_sync_backend(monkeypatch, insp)
        main(["introspect", itype, "-o", "json"])

        payload = json.loads(capsys.readouterr().out)
        # owner=None lets the introspector resolve the connecting user.
        assert payload[0]["owner"] == "AR_CRM"
        assert key in payload[0]
        assert value in str(payload[0])
        assert insp.calls[-1] == (itype, None)

    def test_main_without_owner_asks_the_default_schema(self, monkeypatch, capsys):
        from rhosocial.activerecord.backend.impl.oracle.__main__ import main

        insp = FakeIntrospector()
        fake_sync_backend(monkeypatch, insp)
        main(["introspect", "synonyms", "-o", "json"])
        assert insp.calls[-1] == ("synonyms", None)
        assert json.loads(capsys.readouterr().out)

    def test_main_async_branch_dispatches_through_the_introspector(self, monkeypatch, capsys):
        from rhosocial.activerecord.backend.impl.oracle.__main__ import main
        from rhosocial.activerecord.backend.impl.oracle.cli import introspect as cli

        insp = FakeIntrospector()

        class AsyncFacade:
            def __init__(self, sync: Any) -> None:
                for attr in ("list_sequences", "list_synonyms", "list_packages"):
                    fn = getattr(sync, attr)

                    async def call(*a, _fn=fn, **k):
                        return _fn(*a, **k)

                    setattr(self, attr, call)

        class FakeAsyncBackend:
            def __init__(self, connection_config: Any = None) -> None:
                self.introspector = AsyncFacade(insp)
                self._connection: Optional[object] = object()

            async def connect(self) -> None:
                pass

            async def disconnect(self) -> None:
                pass

        monkeypatch.setattr(cli, "AsyncOracleBackend", FakeAsyncBackend)
        main(["introspect", "packages", "--async", "-o", "json"])
        assert insp.calls[-1] == ("packages", None)
        assert json.loads(capsys.readouterr().out)[0]["status"] == "VALID"


# --------------------------------------------------------------------- #
# Live half
# --------------------------------------------------------------------- #


def _ddl_options():
    from rhosocial.activerecord.backend.options import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    return ExecutionOptions(stmt_type=StatementType.DDL)


@pytest.fixture(scope="function")
def provisioned():
    """A throwaway owner with a sequence, a synonym and a package."""
    from providers.scenarios import get_scenario_raw

    backend_class, config = get_scenario_raw("oracle_26ai")
    backend = backend_class(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()

    ddl = _ddl_options()
    drop_block = (
        f"BEGIN EXECUTE IMMEDIATE 'DROP USER {OBJECT_OWNER} CASCADE'; "
        f"EXCEPTION WHEN OTHERS THEN NULL; END;"
    )
    backend.execute(drop_block, options=ddl)
    try:
        backend.execute(
            f'CREATE USER {OBJECT_OWNER} IDENTIFIED BY "{OBJECT_OWNER_PASSWORD}"',
            options=ddl,
        )
    except DatabaseError as exc:
        backend.disconnect()
        if "ORA-01031" in str(exc):
            pytest.skip("connected user lacks privileges to create schema users")
        raise

    backend.execute(f"GRANT UNLIMITED TABLESPACE TO {OBJECT_OWNER}", options=ddl)
    backend.execute(
        f"GRANT CREATE SEQUENCE, CREATE SYNONYM, CREATE PROCEDURE " f"TO {OBJECT_OWNER}",
        options=ddl,
    )
    backend.execute(
        f"CREATE SEQUENCE {OBJECT_OWNER}.{SEQUENCE_NAME} START WITH 100 "
        f"INCREMENT BY 5 MINVALUE 10 MAXVALUE 100000 CYCLE CACHE 20 ORDER",
        options=ddl,
    )
    backend.execute(
        f"CREATE SYNONYM {OBJECT_OWNER}.{SYNONYM_NAME} " f"FOR {OBJECT_OWNER}.{SEQUENCE_NAME}",
        options=ddl,
    )
    package_created = True
    try:
        backend.execute(
            f"CREATE OR REPLACE PACKAGE {OBJECT_OWNER}.{PACKAGE_NAME} AS "
            f"PROCEDURE p; END {PACKAGE_NAME};",
            options=ddl,
        )
    except DatabaseError as exc:
        if "ORA-01031" not in str(exc):
            backend.execute(drop_block, options=ddl)
            backend.disconnect()
            raise
        package_created = False

    try:
        yield backend, package_created
    finally:
        try:
            backend.execute(drop_block, options=ddl)
        except Exception:
            pass
        backend.disconnect()


class TestLiveSchemaObjects:
    def test_sequence_is_listed_with_its_declared_shape(self, provisioned):
        backend, _ = provisioned
        sequences = backend.introspector.list_sequences(owner=OBJECT_OWNER)
        names = {s.name for s in sequences}
        assert SEQUENCE_NAME in names

        seq = backend.introspector.get_sequence_info(SEQUENCE_NAME, owner=OBJECT_OWNER)
        assert seq is not None
        assert (seq.owner, seq.increment) == (OBJECT_OWNER, 5)
        assert (seq.min_value, seq.max_value) == (10, 100000)
        assert seq.cycle is True and seq.order is True
        assert seq.cache_size == 20
        # LAST_NUMBER is the next value in the current cache block, so it
        # is at least the declared start and never the "last used" 99.
        assert seq.last_number is not None and seq.last_number >= 100

    def test_missing_sequence_is_none_not_an_invented_row(self, provisioned):
        backend, _ = provisioned
        assert backend.introspector.get_sequence_info("NO_SUCH_GATE", owner=OBJECT_OWNER) is None

    def test_private_synonym_points_at_the_sequence(self, provisioned):
        backend, _ = provisioned
        synonyms = backend.introspector.list_synonyms(owner=OBJECT_OWNER)
        found = {s.name: s for s in synonyms}
        assert SYNONYM_NAME in found
        syn = found[SYNONYM_NAME]
        assert syn.is_public is False
        assert (syn.table_owner, syn.table_name) == (OBJECT_OWNER, SEQUENCE_NAME)
        assert syn.db_link is None

        one = backend.introspector.get_synonym_info(SYNONYM_NAME, owner=OBJECT_OWNER)
        assert one is not None and one.table_name == SEQUENCE_NAME

    def test_public_synonyms_are_a_separate_owner_filter(self, provisioned):
        backend, _ = provisioned
        public = backend.introspector.list_synonyms(owner="PUBLIC")
        assert isinstance(public, list)
        assert all(s.owner == "PUBLIC" and s.is_public for s in public)
        # The private synonym is not in the PUBLIC family, and the PUBLIC
        # family is not folded into the private list.
        assert all(s.name != SYNONYM_NAME for s in public)
        private_names = {s.name for s in backend.introspector.list_synonyms(owner=OBJECT_OWNER)}
        assert SYNONYM_NAME in private_names

    def test_packages_when_the_privilege_is_there(self, provisioned):
        backend, package_created = provisioned
        packages = backend.introspector.list_packages(owner=OBJECT_OWNER)
        assert isinstance(packages, list)
        if not package_created:
            pytest.skip("connected user may not create procedures")
        found = {p.name: p for p in packages}
        assert PACKAGE_NAME in found
        assert found[PACKAGE_NAME].status in ("VALID", "INVALID")
        assert found[PACKAGE_NAME].last_ddl_time is not None

    async def test_async_reads_the_same_sequence(self):
        from providers.scenarios import get_scenario_raw
        from rhosocial.activerecord.backend.options import ExecutionOptions
        from rhosocial.activerecord.backend.schema import StatementType

        from rhosocial.activerecord.backend.impl.oracle.backend.async_backend import (
            AsyncOracleBackend,
        )

        _, config = get_scenario_raw("oracle_26ai")
        backend = AsyncOracleBackend(connection_config=config)
        await backend.connect()
        await backend.introspect_and_adapt()
        ddl = ExecutionOptions(stmt_type=StatementType.DDL)
        owner = config.username.upper()
        name = "AR_GATE_ASYNC_SEQ"
        drop = (
            f"BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE {owner}.{name}'; "
            f"EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
        try:
            await backend.execute(drop, options=ddl)
            await backend.execute(
                f"CREATE SEQUENCE {owner}.{name} START WITH 7 INCREMENT BY 3",
                options=ddl,
            )
            sequences = await backend.introspector.list_sequences(owner=owner)
            assert name in {s.name for s in sequences}
            seq = await backend.introspector.get_sequence_info(name, owner=owner)
            assert seq is not None and seq.increment == 3
        finally:
            try:
                await backend.execute(drop, options=ddl)
            finally:
                await backend.disconnect()
