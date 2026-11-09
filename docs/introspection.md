# Database Introspection

The Oracle backend answers two of the three layers the introspection contract draws: what the schema **declares**, read from the `ALL_*` data dictionary views, and what its columns **hold**, answered by two families that do not share a source. The operational layer — index use, I/O counters, runtime metrics, dependency edges, the engine's own DDL — is declined in full, and each refusal names its cause on the probe that makes it.

`backend.introspector` is the entry point for all of it. Every answer on this page is Oracle's own: the views this dictionary exposes, the columns it fills, and the two things it cannot be asked.

**Read `source` before comparing two answers.** The profile is an *estimate* — the optimiser's last statistics from `ALL_TAB_COLUMNS` (`NUM_DISTINCT`, `NUM_NULLS`, `SAMPLE_SIZE`, `LAST_ANALYZED`) — while the value listing is an *aggregate*, a grouped scan of the rows. This backend is the only one in this line where the two data families have different sources, and a reader who assumes one `source` per backend will misread one of the two listings.

## Overview

`backend.introspector` exposes:

- **Schema declarations**: tables, views and materialized views from `ALL_TABLES` / `ALL_VIEWS` / `ALL_MVIEWS`, with the comment joined in and the stored bytes derived from `BLOCKS`
- **Columns**: what `ALL_TAB_COLUMNS` declares, including the declared type string and its precision, scale and character length
- **Indexes**: `ALL_INDEXES` joined to `ALL_IND_COLUMNS`, with the primary key's own index identified through `ALL_CONSTRAINTS`
- **Foreign keys**: the four-way constraint join, with the delete rule and `NO ACTION` update semantics
- **Triggers**: `ALL_TRIGGERS`, with timing, events and body
- **Schema objects of Oracle's own**: sequences (`ALL_SEQUENCES`), synonyms (`ALL_SYNONYMS`), packages (`ALL_OBJECTS`)
- **Data, two ways**: an estimate profile from the dictionary, and an exact value listing bought with a scan

## What Oracle Answers

Every question in the introspection contract has a probe on the dialect, and a probe is a question about the server in front of you rather than about the product. This table is Oracle's answer to all of them, including the three switches this backend declares for its own families.

Read the `False` rows first: a probe that answers `False` says the question cannot be asked of this server, never that the answer was empty or zero.

| Probe | Oracle answers | The question it gates |
|-------|----------------|-----------------------|
| `supports_introspection()` | `True` | Anything about the schema at all |
| `supports_database_info()` | `True` | The schema, its version, and the NLS settings |
| `supports_table_introspection()` | `True` | Tables, views, materialized views and their storage slots |
| `supports_column_introspection()` | `True` | Columns and what they declare |
| `supports_index_introspection()` | `True` | Indexes, their columns, and whether one is the primary key |
| `supports_foreign_key_introspection()` | `True` | Foreign keys, their columns and their delete rule |
| `supports_view_introspection()` | `True` | Views and their definitions |
| `supports_trigger_introspection()` | `True` | Triggers, timing, events and body |
| `supports_table_storage_introspection()` | `True` | Per-table stored bytes |
| `supports_runtime_stats()` | `False` | What the instance itself has been doing |
| `supports_table_stats()` | `False` | Per-segment use counters |
| `supports_index_stats()` | `False` | Per-index use counters |
| `supports_unused_indexes_detection()` | `False` | Indexes with no recorded access |
| `supports_object_dependencies()` | `False` | The object dependency graph |
| `supports_ddl_extraction()` | `False` | The text that recreates an object |
| `supports_ddl_extraction_native()` | `False` (inherited) | Whether that text is the engine's own |
| `supports_column_data_profile()` | `True` | What each column of a table holds, as an estimate |
| `supports_column_value_sample()` | `True` | Which values occur, and how often, as a scan |
| `supports_join_coverage()` | `False` | How much two columns' domains overlap |
| `supports_partition_info()` | `False` (inherited) | Core's partition family |
| `supports_extensions()` | `False` (inherited) | Installed add-on components |
| `supports_sequence_introspection()` | `True` | Oracle's own sequence family |
| `supports_synonym_introspection()` | `True` | Oracle's own synonym family |
| `supports_package_introspection()` | `True` | Oracle's own package-specification family |

Three of those are this backend's own switches, beside core's set: `supports_sequence_introspection()`, `supports_synonym_introspection()` and `supports_package_introspection()`. Each answers `True` because its view is an ordinary dictionary view a `SELECT` can read — `ALL_SEQUENCES`, `ALL_SYNONYMS`, `ALL_OBJECTS`.

Three `False` rows are inherited rather than declared here. `supports_ddl_extraction_native()` stays `False` because the family it qualifies is already declined (`False` on a native extractor is "there is no native extraction to describe"). `supports_partition_info()` stays `False` because core's partition family has no method on this backend to oblige; Oracle has a partition-metadata switch of its own — `supports_partition_metadata_introspection()`, true from 11g — but no core family reads it and no CLI type offers it. `supports_extensions()` stays `False` for the reason core records: no catalogue of add-on components is mapped here.

A `True` promises only that the statement renders and the server accepts it. It says nothing about the shape of the answer or its price; the price of every family is stated where the family is described.

## Where the Catalogue Answers Come From

| Family | Method | Oracle source |
|--------|--------|---------------|
| Database info | `get_database_info()` | Four scalar subreads of `V$NLS_PARAMETERS`, projected from `DUAL` |
| Tables | `list_tables()` | `ALL_TABLES` ⟕ `ALL_TAB_COMMENTS` ⟕ `USER_TABLESPACES`; `ALL_VIEWS`; `ALL_MVIEWS` |
| Columns | `list_columns()` | `ALL_TAB_COLUMNS` |
| Primary key | `get_primary_key()` | `ALL_CONSTRAINTS` ⋈ `ALL_CONS_COLUMNS` |
| Indexes | `list_indexes()` | `ALL_INDEXES` ⋈ `ALL_IND_COLUMNS` ⟕ `ALL_CONSTRAINTS` (narrowed to `P`) |
| Foreign keys | `list_foreign_keys()` | `ALL_CONSTRAINTS` ⋈ `ALL_CONS_COLUMNS` ⋈ `ALL_CONSTRAINTS` ⋈ `ALL_CONS_COLUMNS` |
| Views | `list_views()`, `get_view_info()` | `ALL_VIEWS` |
| Triggers | `list_triggers()` | `ALL_TRIGGERS` |
| Sequences | `list_sequences()`, `get_sequence_info()` | `ALL_SEQUENCES` |
| Synonyms | `list_synonyms()`, `get_synonym_info()` | `ALL_SYNONYMS` |
| Packages | `list_packages()` | `ALL_OBJECTS`, narrowed to `PACKAGE` |

The `ALL_*` spelling is deliberate: these views are owner-scoped and show only the objects the account can reach, so the introspection layer never needs a privilege the schema itself would not grant. The owner is the account the connection authenticates as, and every query binds it upper case, because the dictionary stores unquoted identifiers that way. `owner="app"` and `owner="APP"` are folded to one question, so a caller's spelling cannot miss a schema that exists.

A few consequences of each statement, in the order a reader will meet them:

- **Tables.** A materialized view is stored as a table segment, so `ALL_TABLES` lists one — and the listing subtracts MVs from the table side by name, or one MV would be reported twice: once as a table and once as the MV it is. `TABLE_TYPE` is projected as a constant per side (`TABLE`, `VIEW`, `MATERIALIZED VIEW`) rather than assumed for every row. Views and MVs carry `NULL` for the comment, row count and size, because those facts are the table side's.
- **Stored bytes.** `ALL_TABLES.BLOCKS` counts blocks, and a block is bytes only once the tablespace's own `BLOCK_SIZE` multiplies it. That size is joined in from `USER_TABLESPACES` — the view every session can read — and the join is a `LEFT` one, so a table whose tablespace the session cannot see gets a `NULL` size rather than an assumed `8192`. The common minimum's split slots (`data_size_bytes`, `index_size_bytes`) stay `None`: `ALL_INDEXES` is a family of its own rather than a stored complement of the table's blocks.
- **Row count.** `NUM_ROWS` is the optimiser's estimate from the last statistics gathering, not a `COUNT(*)`, and it is `NULL` for a table never analysed. `update_time` is `LAST_ANALYZED` for the same reason — it is when the figures were collected, not when the table was written.
- **Indexes.** `IndexType.BTREE` is reported for `NORMAL` only. A `BITMAP` index and a function-based index arrive as `IndexType.UNKNOWN` with `extra["oracle_index_type"]` holding the engine's spelling and `extra["bitmap"]` / `extra["function_based"]` set, because there is no core member for them and inventing one would be a claim about structure. A primary key is unique whatever `UNIQUENESS` said, so the two facts are reported together rather than allowed to contradict.
- **Foreign keys.** Oracle enforces no `ON UPDATE` clause, so `on_update` is always `ReferentialAction.NO_ACTION`; the delete rule is parsed from `DELETE_RULE`. The referenced side is found through the four-way join (constraint, its columns, the referenced constraint, its columns), matched on constraint name **and** owner **and** column position — a same-named constraint in another schema must not contribute a column.
- **Views.** `check_option` is `None` because `ALL_VIEWS` does not carry it; `is_updatable` is `READ_ONLY != 'Y'`.
- **Triggers.** `timing` is the dictionary's `TRIGGER_TYPE` (`BEFORE`, `AFTER`, `INSTEAD OF`), `events` are `TRIGGERING_EVENT` split on `OR`, and `definition` is `TRIGGER_BODY`.
- **Columns.** A column record is the declaration, not the statistics. The statement projects the dictionary's statistics columns alongside the declaration, but they land on the profile family, not on the column record — and `comment` is always `None`, because Oracle keeps column comments in a separate view this family does not query. Each dimension is filled from the dictionary column that means it: `DATA_LENGTH` is bytes of storage (`22` for a `NUMBER`), `DATA_PRECISION`/`DATA_SCALE` are the numeric width (and, on a `TIMESTAMP`, `DATA_SCALE` is the fractional-seconds precision rather than a decimal scale), `CHAR_LENGTH` is the declared character length with a `DATA_LENGTH` fallback for pre-9i servers, and `RAW(n)` keeps its width because a `RAW` of another width is another column.
- **Database info.** `name` is the connected schema, `version` is the server version the backend measured (`"unknown"` and `version_tuple` `(0, 0, 0)` when it could not), and `encoding`/`collation` are `NLS_CHARACTERSET` and `NLS_LANGUAGE`. A version nobody measured is not reported as a number.

## The Two Data Families, and Their Two Sources

| Family | Method | Source | Price |
|--------|--------|--------|-------|
| Column profiles | `list_column_data_profiles(table)` | `ALL_TAB_COLUMNS` statistics | One dictionary read for the table |
| Value samples | `sample_column_values(table, column, limit=100)` | A `GROUP BY` over the rows | Every row of the table |
| Join coverage | `estimate_join_coverage(...)` | — | Declined; no statement |

The profile and the value listing are the two halves of "what does this column hold", and here they are produced differently. The profile is the optimiser's summary — cheap, and possibly stale. The listing is a count over the rows — exact, and paid for in time proportional to the table. The `source` column is what keeps the two apart.

### The Profile Is an Estimate From `ALL_TAB_COLUMNS`

One dictionary row per column carries `NUM_DISTINCT`, `NUM_NULLS`, `SAMPLE_SIZE`, `LAST_ANALYZED` and `HISTOGRAM`, so profiling a table costs one dictionary read however large the table is. (The introspector also reads the column catalogue first, for the names core's flow asks an aggregating dialect to pivot; that is the same view, one cheap query, and both answers are cached.)

The parser labels every row `source = 'estimate'` when `LAST_ANALYZED` records a vintage, and `source = 'unknown'` when it does not. Both are estimates; the rule for this family is that an estimate must be able to say when it was collected, and a column that was never analysed has no vintage to give.

A table that was never analysed still answers: `ALL_TAB_COLUMNS` lists one row per column either way, with every statistics column `NULL`. The profile then reports `None` for every figure and `source = 'unknown'` — a column whose shape is known and whose contents are not. That is a different answer from "no such column", and it must not be smoothed into zeros.

`distinct_count` is `NUM_DISTINCT` and needs no adjustment, but how exact it is depends on the histogram: under `FREQUENCY` every distinct value has a bucket of its own and the count is exact; otherwise it is an estimate derived from the sample. `histogram` carries which — `NONE`, `FREQUENCY`, `HEIGHT BALANCED`, `TOP-FREQUENCY` or `HYBRID` — rather than the parser guessing at the confidence. `sample_size` travels as its own field so a caller can see how much of the table was sampled; `data_type` travels beside the figures because it explains the one thing missing.

### The `SAMPLE_SIZE` Trap

`SAMPLE_SIZE` is the number of sampled rows **that were not null**. Oracle counts the nulls separately in `NUM_NULLS` and leaves them out of the sample size, so the population the statistics describe is the two parts added together — and that total, not `SAMPLE_SIZE`, is the fraction's denominator:

```
row_count     = NUM_NULLS + SAMPLE_SIZE
null_fraction = NUM_NULLS / row_count
```

Dividing by `SAMPLE_SIZE` alone reports nulls per *non-null* row rather than per row. A column that is 60% null over 1000 rows comes out as `600 / 400 = 1.5` — a fraction above 1, which is the arithmetic saying the denominator was the wrong population. The parser derives `row_count` from the sum and uses it for both slots, so the fraction and the population it was derived from travel together. A missing `NUM_NULLS` or `SAMPLE_SIZE` leaves both `None` — a total needs both parts — and a zero total (an analysed but empty table) leaves the fraction undefined rather than `0.0`.

Anyone implementing anything against these numbers must read `SAMPLE_SIZE` as "rows sampled that were not null", never as "rows sampled". `OracleColumnDataProfile` keeps the dictionary's own figure beside `row_count` precisely so the two cannot be confused: a profile whose `sample_size` is 400 while its `row_count` is 1000 is a column that is 60% null, and one whose `sample_size` is 1000 with the same `row_count` is not.

### Why `min_value` and `max_value` Are Always `None`

The dictionary holds the extremes in `LOW_VALUE`/`HIGH_VALUE`, and both are `RAW` in the column's *internal* encoding: for `NUMBER` a mantissa/exponent byte string, for `DATE` seven bytes of century-to-second. Neither is readable as it stands, and converting one needs `DBMS_STATS.CONVERT_RAW_VALUE` — a PL/SQL procedure whose result type is the column's own, so no single `SELECT` can call it in place.

The slots are therefore left `None` rather than filled with RAW bytes cast to text: bytes a caller cannot compare would be worse than an honest absence. Decoding is a per-column PL/SQL round trip, and if it is done it belongs beside the profile rather than inside it. The declared `data_type` on each profile is what says which type the extreme would have been, so a reader knows what to decode rather than guessing.

### `ALL_TAB_COLUMNS`, Not `ALL_TAB_COLS`

The profile reads `ALL_TAB_COLUMNS`, the same view the column family reads. `ALL_TAB_COLS` would also list hidden and internal columns; using it here would make the profile name columns the declaration listing does not, and the whole point of the family is to pair a declared shape with what it holds. The consequence is direct: **a hidden or internal column has no profile**. That is a gap in the answer, and it is the price of the two listings naming the same columns.

### The Value Listing Is an Aggregate

Oracle keeps no most-common-values list — the dictionary records how many distinct values a column has, never which ones — so the listing is computed from the rows: one `GROUP BY` over the column with `COUNT(*)`, ordered by that count descending and bounded by `FETCH FIRST ... ROWS ONLY`.

- The count is exact over the rows the query saw. It is the expensive half of the family, and on a large table it may never be worth asking.
- `ORDER BY COUNT(*) DESC` is what makes the answer "the most frequent values" rather than "the first values"; ties break by the value, so two runs of the same query agree.
- The `LIMIT` is bound as a parameter, so the caller's integer is never spliced into statement text.
- `GROUP BY` groups `NULL`s, so a column containing `NULL` has an entry whose `value` is `None`, and it is usually the most frequent one. `NULL` is a member of the domain in this listing, and the parser does not skip it.
- `frequency` stays `None`: the engine reports a count and the denominator is the profile's `row_count`, which this query does not carry. A caller that wants a share divides by it; a parser that divided would have had to assume the denominator.

The library's own rows carry no provenance field — core's `ColumnValueFrequency` has a value and a count, not how they were counted. So the CLI adds `source='aggregate'` to each row it prints, at the only layer that knows how the answer was produced. Without it the two tables of one run would read as the same kind of answer, and a sampled estimate beside a counted scan is exactly the pair a reader must not blur.

```python
from rhosocial.activerecord.backend.impl.oracle.backend import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig

config = OracleConnectionConfig(
    host="localhost",
    port=1521,
    username="app",
    password="secret",
    service_name="ORCL",
)
backend = OracleBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()

# The cheap half: one dictionary read for the whole table.
for profile in backend.introspector.list_column_data_profiles("orders"):
    print(
        profile.column_name,
        profile.source,          # 'estimate', or 'unknown' if never analysed
        profile.row_count,       # NUM_NULLS + SAMPLE_SIZE
        profile.sample_size,     # SAMPLE_SIZE: sampled rows that were not null
        profile.null_fraction,   # None on an empty or unanalysed column
        profile.distinct_count,
        profile.histogram,
        profile.min_value,       # always None: LOW_VALUE is RAW
    )

# The dear half: a grouped scan, and it says so in every row the CLI prints.
for sample in backend.introspector.sample_column_values("orders", "status", limit=20):
    print(sample.value, sample.count, sample.frequency)   # value is None when NULL is a group
```

## The Operational Layer Oracle Declines

All six families answer `False`, and the reasons are of two kinds: a source a schema-scoped `SELECT` cannot reach, or a statement nobody has written.

| Family | Probe | Why |
|--------|-------|-----|
| Unused indexes | `supports_unused_indexes_detection()` | Use is instance state in `V$OBJECT_USAGE` |
| Table counters | `supports_table_stats()` | The counters live in `V$SEGSTAT` / `V$SEGMENT_STATISTICS` |
| Runtime metrics | `supports_runtime_stats()` | The measurements are `V$SYSSTAT` and its siblings |
| Index counters | `supports_index_stats()` | Same source and same reason as the table counters |
| Dependencies | `supports_object_dependencies()` | The edges exist in `ALL_DEPENDENCIES`; the statement does not |
| DDL | `supports_ddl_extraction()` | The text is behind `DBMS_METADATA.GET_DDL`, a PL/SQL call |

**Index use, segment counters, runtime metrics.** `V$OBJECT_USAGE` (and its instance-wide siblings from 19c) records whether an index has served a query *on this instance, since this instance's counters were last reset* — a live measurement, not a dictionary row. `V$SEGSTAT`/`V$SEGMENT_STATISTICS` carry the per-segment counters, and `V$SYSSTAT` answers "how is this instance behaving", which is a question about the connection's host process rather than about a schema's contents. These are dynamic performance views: priced per read, scoped to the session's view of the instance, and a source the dictionary-shaped statements this dialect spells do not reach. No statement in this dialect spells them, so the probes decline rather than advertise a family the introspector could only fail on.

That is not the same as Oracle having no status surface. `backend.introspector.status` is a separate object with its own questions — `list_performance_metrics()` reads `V$SYSSTAT`, `get_connection_info()` reads `V$SESSION` and `V$PARAMETER` — and those are *server* questions, not the schema-scoped operational families above. The operational family is a question about *this schema's* objects; the status surface is a question about the instance. They are not substitutes, and the `False` probes describe the former.

The one `V$` read the catalogue layer does perform is `get_database_info()`'s four named parameters out of `V$NLS_PARAMETERS` — a fixed lookup of settings that have no dictionary equivalent, not a measurement of activity. It is the exception the catalogue table above shows, and it is a single-row read rather than a counter.

**Dependencies.** This one is a missing statement rather than an unreachable source: `ALL_DEPENDENCIES` is an ordinary dictionary view a `SELECT` could read, and the engine records real edges there. Until the statement is written the probe answers `False`, because a `True` would promise a formatter the dispatch cannot find. Writing it flips the probe; the two moves are one decision.

**DDL.** `DBMS_METADATA.GET_DDL` is a procedure whose result no `SELECT` can return, and no dictionary view stores the text. Reassembling a `CREATE TABLE` from dictionary columns was rejected: this engine has the real statement, and an assembly that reads as authoritative is worse than an honest refusal. With neither source reachable from a `SELECT`, there is nothing to format. This backend therefore does not offer DDL extraction: the CLI has no `ddl` type, the backend DDL formatter is kept for a future real implementation, and `supports_ddl_extraction()` answers `False`.

**Join coverage.** The engine can measure the overlap — group both columns, intersect the domains — so this too is a missing statement rather than a missing capability. It is a double scan of two tables, which is why it belongs behind an explicit call rather than in any automatic pass. Until it exists, `estimate_join_coverage()` is not offered.

A `False` here is "the question cannot be asked here", not "nothing was used". It does not say that no index was used, that no segment was read, or that the schema has no dependencies: it says the server cannot be asked *through this layer*. `False` lets a caller take its fallback knowingly; the empty list is reserved for a question that was asked and had nothing to report.

## Floors and Version Gates

Two limits decide whether a family or a spelling can run at all, and neither is a property of the query text.

| Family or field | Floor | What a missing floor looks like |
|-----------------|-------|----------------------------------|
| `value-samples` | Oracle 12c, where `FETCH FIRST` arrives | `UnsupportedFeatureError` from the formatter, before the query runs |
| Column character length | Oracle 9i, where `CHAR_LENGTH` arrives | Falls back to `DATA_LENGTH`, correct for a single-byte character set |
| `sequences` | Oracle 9i, where sequence objects arrive | `supports_sequence()` is `False`; `supports_sequence_introspection()` is not version-gated |
| every other family | none established here | — |

The value listing's floor is the one that reaches users. The bound is spelled `FETCH FIRST ... ROWS ONLY`, and this dialect's pagination mixin would otherwise fall back to appending `ROWNUM <= n` **after** the `ORDER BY`, which Oracle rejects as malformed. The formatter therefore refuses a pre-12c server outright rather than emitting bad SQL, and the probe still answers `True`: the question is answerable on any Oracle, and it is the *bounded* spelling that has a version.

The column catalogue's fallback is not a refusal: on a server that predates `CHAR_LENGTH`, the byte count is the only length there is, and it is the same number for a single-byte character set. Only character types take that fallback; for any other type `DATA_LENGTH` means bytes of storage and would be a length claim of the wrong kind.

No other family is version-gated here. The probes do not test grants either: a probe answers about the server, not about the account's privileges. If the connected account may not read a view the family asks for, the engine's own error arrives at execution; the probe said `True` because the server can answer, not because this account may ask.

## Command Line Introspection

```bash
# List tables, views or one table's columns
python -m rhosocial.activerecord.backend.impl.oracle introspect tables \
  --host localhost --port 1521 --service ORCL --user app --password secret
python -m rhosocial.activerecord.backend.impl.oracle introspect views
python -m rhosocial.activerecord.backend.impl.oracle introspect columns orders

# What a column holds: the estimate, then the scan
python -m rhosocial.activerecord.backend.impl.oracle introspect column-profiles orders
python -m rhosocial.activerecord.backend.impl.oracle introspect value-samples orders --column status

# The families this backend declines -- refused, not answered with an empty list
python -m rhosocial.activerecord.backend.impl.oracle introspect unused-indexes
python -m rhosocial.activerecord.backend.impl.oracle introspect join-coverage orders \
  --column customer_id --right-table customers --right-column id
```

Connection settings come from flags or from the environment: `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE`, `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_SSL`. `--database` is an alias for `--service`, and `--async` runs the same commands through `AsyncOracleBackend`. The owner is the user the run authenticates as, so there is no separate owner option; `PUBLIC` synonyms are not listed, because Oracle records them under the owner `PUBLIC` rather than under a schema this run connects as.

### Introspection Types

| Type | Description | NAME |
|------|-------------|------|
| `tables` | Tables, views and materialized views | not accepted |
| `views` | List all views, or describe one | optional (view) |
| `columns` | Column declarations | required (table) |
| `indexes` | Indexes, columns and primary-key flags | required (table) |
| `foreign-keys` | Foreign keys and their delete rule | required (table) |
| `triggers` | Triggers on one table, or all of them | optional (table) |
| `sequences` | List all sequences, or describe one | optional (sequence) |
| `synonyms` | List all synonyms, or describe one | optional (synonym) |
| `packages` | PL/SQL package specifications | not accepted |
| `database` | Schema, version and NLS settings | not accepted |
| `column-profiles` | What each column of a table holds, as an estimate | required (table) |
| `value-samples` | Most frequent values of one column, counted | required (table) |
| `unused-indexes` | Indexes with no recorded access | not accepted |
| `table-stats` | Per-segment use counters | not accepted |
| `runtime` | Live runtime measurements | not accepted |
| `index-stats` | Per-index use counters | not accepted |
| `dependencies` | The object dependency graph | optional (object) |
| `join-coverage` | Overlap of two columns' value domains | required (left table) |

Eighteen types, and the advertised list is derived from the same mapping that gates each type on its switch, so a family cannot be offered by the CLI and forgotten by the probe. DDL extraction is not a type: its switch is permanently `False`, so the CLI omits it rather than offering an entry every run would refuse.

NAME means different things per type, and two of those meanings point in opposite directions: for `triggers` it is a table, for `views` it is the view itself. The rules live in one table and are applied by one validator before anything connects. A `required` name that is missing is an incomplete invocation (exit 1); a name a type has no question for is a request the command declines (exit 2), never a dropped argument.

`column-profiles` requires a table name: a profile is a question about one table's columns, and there is no whole-schema profile. `value-samples` requires a table *and* `--column`. `join-coverage` requires the left table as its NAME plus `--column`, `--right-table` and `--right-column`. This command has no `--limit`: the listing is bounded by the introspector's own default (`VALUE_SAMPLE_LIMIT`, 100), so the number is not written in two places.

A type whose switch answers `False` on the *connected* server is refused with exit 2 and the reason on stderr, because "this server cannot answer" and "this schema has none" are different answers. Exit 1 is a runtime failure — a connection, a query, or an object that does not exist.

### Output Formats

`-o` selects `table` (default), `json`, `csv` or `tsv`; `--rich-ascii` switches the table borders to ASCII. The `table` format is the human one; the other three are for parsers.
