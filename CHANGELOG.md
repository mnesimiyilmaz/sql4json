# Changelog

All notable changes to SQL4Json are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-05-02

### Added — Grammar Introspection API
A public IDE-tooling surface in the new JPMS-exported `io.github.mnesimiyilmaz.sql4json.grammar` package — for syntax highlighters, completion popups, and lightweight static analysers that need to reason about SQL4Json text without depending on ANTLR.
- `SQL4JsonGrammar` with `keywords()`, `functions()`, and `tokenize(String)` static views; the tokenizer never throws — unrecognised spans surface as `BAD_TOKEN`.
- `Token` / `TokenKind` (record + enum) — tokenizer output with absolute, exclusive-end offsets.
- `FunctionInfo` / `Category` (record + enum) — function catalog entries carrying category, arity, signature, and description.
- `FunctionRegistry.scalarFunctionNames()` / `valueFunctionNames()` / `aggregateFunctionNames()` — unmodifiable views over registered names, used by tooling consumers and drift tests.
- Drift tests guard the hand-maintained `KEYWORDS`, `FUNCTIONS`, and `TOKEN_KIND_BY_TYPE` tables — adding a lexer rule or function without updating the catalog now fails CI.

### Added — Array Predicates
- Five new condition operators for querying values inside JSON array fields, modelled on PostgreSQL's native-array operators.
  - `tags CONTAINS 'admin'` — keyword operator for scalar membership.
  - `tags @> ARRAY['admin','editor']` — contains-all.
  - `tags <@ ARRAY['admin','editor','viewer']` — contained-by.
  - `tags && ARRAY['blocked','flagged']` — overlap.
  - `tags = ARRAY['admin','editor']` / `tags != ARRAY[…]` — structural equality (order-sensitive, length-sensitive).
- `ARRAY[expr, expr, …]` array-literal syntax in `rhsValue` — empty `ARRAY[]` allowed.
- Parameter binding follows the JDBC / Hibernate / jOOQ pattern: `tags @> :myList` (or `?`) binds a whole `Collection` to one slot; `ARRAY[?, ?]` is element-by-element with one scalar bind per slot. No collection-expansion inside `ARRAY[…]`. Bind-time validation raises `SQL4JsonBindException` for type mismatches (collection in `ARRAY[?]` slot, scalar in bare-array-RHS, collection in `CONTAINS`).
- `<`, `>`, `<=`, `>=` against an `ARRAY[…]` literal raise `SQL4JsonParseException` at parse time with a clear message.
- Field state on the LHS: missing, JSON null, scalar, or object → all five operators return `false` for that row (no exception).
- `JOIN` aliases work — array predicates resolve through the same alias-aware path as the rest of the engine; flat-key reassembly fallback handles post-JOIN merged rows.
- Catalog: `SQL4JsonGrammar.keywords()` now includes `ARRAY` and `CONTAINS`; `tokenize(...)` surfaces `@>`, `<@`, `&&` as `TokenKind.OPERATOR` and `[`, `]` as `TokenKind.PUNCTUATION`.

### Added — Command-line Interface
- Command-line entrypoint shipping as a separate shaded jar with classifier `cli` (`sql4json-1.2.0-cli.jar`). Flags: `-q/--query` (literal or `@path`), `-f/--file`, `-o/--output`, `--data name=path` (repeatable, multi-source JOIN), `-p/--param name=<json>` (repeatable, named-parameter bind), `--pretty`, `-h/--help`, `-v/--version`. Stable exit codes: `0` success / `--help` / `--version`, `1` runtime failure (SQL4Json error, IO error), `2` usage error. `SQL4JSON_DEBUG=1` attaches a full stack trace to failure messages on stderr.
- Library jar (`sql4json-1.2.0.jar`) unchanged — pure library, no `Main-Class`. The `io.github.mnesimiyilmaz.sql4json.cli` package is intentionally non-exported from `module-info.java`; the *flag set* and *exit codes* are the stable surface, not the implementation classes.
- `JsonSerializer.prettySerialize(JsonValue)` — public sibling of `serialize(JsonValue)` for two-space pretty-printing. Empty objects and arrays stay compact; output has no trailing newline. Drives the CLI `--pretty` flag.

### Added — Performance Profiling
- New `docs/performance.md` reference document with the full sweep across seven sizes (8 MB → 512 MB) and ~50 scenarios, the reference environment, dataset details, and the regen recipe. `README.md` gains a concise headline table that links to the full doc.
- `ProfilingTest` now runs each scenario `N` times (default `3`, configurable via `-Dprofiling.runs=N`) and reports the median wall-clock time per `(label, size)` cell. Report header now records total RAM, initial heap, profiling-runs count, OS arch, and the data seed read from `src/test/resources/data-files/SEED`. Data files are byte-reproducible via `generate_json.py --seed`.

### Changed
- `NOW()` is no longer a dedicated lexer literal; the `VALUE_FUNCTION` rule was retired and `NOW()` lexes as a regular function call, dispatched at parse time to `Expression.NowRef` (lazy / per-row) or an eager `SqlDateTime`. `containsNonDeterministic` still fires in every path, so cache-bypass behaviour is preserved.
- `IN` / `BETWEEN` non-literal operands generalised: any non-literal element/bound (not just `ParameterRef`) now flows through `ConditionContext.valueExpressions` / `lowerBoundExpr` / `upperBoundExpr` and is evaluated per-row. `ParameterSubstitutor` snapshots `NowRef` to a literal `SqlDateTime` at substitute time, so all rows in a parameterized execution see the same timestamp (JDBC-style "bind once, execute").
- String functions auto-coerce non-string inputs via `rawValue().toString()` (matching `CONCAT`): `LOWER`, `UPPER`, `SUBSTRING`, `TRIM`, `LENGTH`, `LEFT`, `RIGHT`, `LPAD`, `RPAD`, `REVERSE`, `REPLACE`, `POSITION`. String-typed argument positions coerce too; numeric positions (e.g. `SUBSTRING` start/length) are unchanged. Null input still short-circuits to `NULL`.
- `TO_DATE` consolidates non-string inputs through the same coerce-then-parse path; `SqlDate` / `SqlDateTime` pass through unchanged.
- Whitespace lexer channel: the `ESC` rule emits to `channel(HIDDEN)` instead of `-> skip`, so `tokenize()` can surface whitespace runs as `TokenKind.WHITESPACE`. Query parsing is unaffected (parser still filters by `DEFAULT_CHANNEL`).
- Sealed `JsonNumberValue` and `SqlNumber` types: split into `JsonLongValue` / `JsonDoubleValue` / `JsonDecimalValue` and `SqlLong` / `SqlDouble` / `SqlDecimal`. Primitives stored unboxed in the long/double variants — per-instance footprint roughly halves on row-materializing workloads. `JsonNumberValue` and `SqlNumber` become sealed interfaces; pattern-destructure call sites switch over the typed variants.
- `FlatRow` materialization: GROUP BY, HAVING, WINDOW, ORDER BY, JOIN, DISTINCT, SELECT and the engine pre-flatten path now emit `Object[]`-backed rows keyed by ordinal via a shared `RowSchema`. Lazy `Row` is unchanged for streaming WHERE / lazy SELECT. A `null` slot decodes as `SqlNull.INSTANCE` on read.
- `RowAccessor` sealed interface bridges lazy `Row` and `FlatRow` across `ExpressionEvaluator`, condition handlers (`InConditionHandler`, `BetweenConditionHandler`, `LikeConditionHandler`, `NotLikeConditionHandler`, `ComparisonConditionHandler`, `NullCheckConditionHandler`, `ArrayPredicateConditionHandler`), `ArrayPathNavigator`, `CriteriaNode`, the pipeline (`Stream<RowAccessor>`), `JsonUnflattener`, and `GroupAggregator`.
- `WindowStage` writes window results into per-row `Object[]` buffers indexed by `RowSchema.windowSlot`. The legacy `Row.windowResults` / `windowResultsByAlias` maps are gone; alias mirroring uses `RowSchema.withWindowSlots(calls, aliases)` so the SELECT alias becomes the canonical column key for the slot. CASE-buried windows resolve through the same schema-slot lookup via `Row.getWindowResult`.
- `SqlValueComparator` adds typed pattern-matched fast paths for `(SqlLong, SqlLong)` / `(SqlLong, SqlDouble)` / `(SqlDouble, SqlLong)` / `(SqlDouble, SqlDouble)` — avoids the `Number.doubleValue()` boxing on the WHERE / ORDER BY hot path. `SqlDecimal` involvement still routes through the generic `doubleValue()` compare.

### Fixed
- Window-only functions without `OVER` (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `LAG`, `LEAD`) now raise `SQL4JsonParseException` at parse time with a clear message, instead of the misleading runtime *"Scalar function 'X' requires at least one argument"*. Aggregate functions remain valid without `OVER`.
- Whole-number literals serialize as integers: `SELECT 42 AS x FROM $r` now produces `"x":42` instead of `"x":42.0`, matching column-from-JSON values.

## [1.1.0] - 2026-04-23

### Added — Object Mapping
- `SQL4Json.queryAs` and `queryAsList` — map query results directly to Java records, POJOs, or basic types (both single-source and JOIN variants).
- `PreparedQuery.executeAs` and `executeAsList` (`String` and `JsonValue` overloads).
- `SQL4JsonEngine.queryAs` and `queryAsList`.
- `JsonValue.as(Class)` and `JsonValue.as(Class, Sql4jsonSettings)` default methods on the sealed interface.
- `MappingSettings` subsection of `Sql4jsonSettings` with `MissingFieldPolicy` enum (`IGNORE` / `FAIL`).
- `SQL4JsonMappingException` (new sealed subclass of `SQL4JsonException`).

### Added — Parameter Binding
- `PreparedQuery.execute(String json, BoundParameters params)` and `execute(JsonValue, BoundParameters)`.
- `PreparedQuery.execute(String json, Object... positionalParams)` shortcut.
- `PreparedQuery.execute(String json, Map<String, ?> namedParams)` shortcut.
- `PreparedQuery.executeAs` / `executeAsList` with `BoundParameters` overloads.
- `SQL4JsonEngine.query` / `queryAsJsonValue` / `queryAs` / `queryAsList` with `BoundParameters` overloads.
- `BoundParameters` immutable carrier (named & positional modes, `named()` / `positional()` / `of(Object...)` / `of(Map)` factories, `bind` / `bindAll`, `EMPTY` singleton).
- Grammar support for `?` (positional) and `:name` (named) placeholders throughout WHERE / SELECT / GROUP BY / HAVING / JOIN / function arguments.
- Dynamic `LIMIT` / `OFFSET` binding (placeholders accepted in both positions).
- IN-list expansion: `IN (?)` + collection bound → expanded to N literals; empty collection → zero-row predicate.
- `LimitsSettings.maxParameters` (default `1024`) — DoS guard against placeholder flooding.
- `SQL4JsonBindException` (new sealed subclass of `SQL4JsonException`) — surfaced at substitute time for missing / extra / type-mismatch bindings, IN-list overflow, and LIMIT/OFFSET validation failures.

### Changed
- `Sql4jsonSettings` gains a `mapping` component (all existing construction paths preserved via builder).
- Grammar now recognizes `?` and `:name` as placeholders in value positions.
- `LimitsSettings` canonical-constructor signature extended (`maxParameters`) — public callers using `.builder()` unaffected.
- `SQL4JsonEngine` bypasses `QueryResultCache` for parameterized queries.
- Internal: ISO date/datetime/instant parsing consolidated into `json.IsoTemporals`; `registry.DateCoercion` delegates.

### Dependencies
- No new runtime dependencies — zero-dep philosophy preserved.

## [1.0.0] - 2026-04-10

Initial public release.

### Core Query Engine
- SQL SELECT parsing via ANTLR4 grammar (case-insensitive keywords)
- Pipeline-based query execution: WHERE → GROUP BY → HAVING → WINDOW → ORDER BY → LIMIT → SELECT → DISTINCT
- Lazy streaming evaluation for WHERE and SELECT stages; materializing stages for GROUP BY, WINDOW, and ORDER BY
- Hash join execution for multi-source JOIN queries
- Nested JSON flattening with path-based field keys and query-scoped string interning
- Streaming JSON parser for efficient processing of large root arrays
- Zero external runtime dependencies beyond the ANTLR4 runtime

### SQL Syntax
- **SELECT** with `*`, specific columns, aliases (`AS`), and dot-notation aliases for structured nested output
- **FROM** with root reference (`$r`), nested path drilling (`$r.response.data`), table names for JOINs, and subqueries
- **JOIN** / **INNER JOIN**, **LEFT JOIN**, **RIGHT JOIN** with equality `ON` conditions (single or multi-column via `AND`); chained JOINs supported
- **WHERE** with comparison (`=`, `!=`, `>`, `<`, `>=`, `<=`), pattern matching (`LIKE`, `NOT LIKE`), range (`BETWEEN`, `NOT BETWEEN`), set membership (`IN`, `NOT IN`), and null checks (`IS NULL`, `IS NOT NULL`)
- **GROUP BY** with multiple columns and `HAVING` filter on aggregated aliases
- **ORDER BY** with `ASC`/`DESC` on multiple columns and expressions
- **LIMIT** and **OFFSET** for pagination
- **DISTINCT** for duplicate row elimination
- **Subqueries** in FROM clause (nesting depth bounded by a configurable limit)
- Logical connectives `AND` / `OR` with parenthesized grouping
- Arbitrary nesting of function calls in SELECT, WHERE, GROUP BY, ORDER BY, and HAVING
- **CASE expressions:** Simple (`CASE expr WHEN val THEN result END`) and searched (`CASE WHEN condition THEN result END`) forms, usable in SELECT, WHERE, ORDER BY, GROUP BY, and HAVING with full nesting support

### Scalar & Aggregate Functions
- **String (13):** LOWER, UPPER (with locale support), CONCAT, SUBSTRING, TRIM, LENGTH, REPLACE, LEFT, RIGHT, LPAD, RPAD, REVERSE, POSITION
- **Math (8):** ABS, ROUND, CEIL, FLOOR, MOD, POWER, SQRT, SIGN
- **Date/Time (10):** TO_DATE (with optional format), NOW, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DATE_ADD, DATE_DIFF
- **Conversion (3):** CAST (7 target types: STRING, NUMBER, INTEGER, DECIMAL, BOOLEAN, DATE, DATETIME), NULLIF, COALESCE
- **Aggregate (5):** COUNT (including `COUNT(*)`), SUM, AVG, MIN, MAX

### Window Functions
- **Ranking:** ROW_NUMBER, RANK, DENSE_RANK, NTILE
- **Offset:** LAG, LEAD (with optional offset argument)
- **Aggregate windows:** SUM, AVG, COUNT, MIN, MAX usable with `OVER`
- **OVER clause** with `PARTITION BY` and/or `ORDER BY` (full-partition scope — window frames not yet supported)

### Public API
- `SQL4Json` — static facade: `query()`, `queryAsJsonValue()`, `prepare()`, `engine()`
- `PreparedQuery` — parse-once-execute-many, thread-safe and immutable
- `SQL4JsonEngine` — long-lived engine with bound data and optional result cache; thread-safe
- `SQL4JsonEngineBuilder` — fluent builder supporting both unnamed (`$r`) and named (JOIN) data sources
- `QueryResultCache` — SPI for custom cache implementations (default LRU provided); non-deterministic queries (e.g. `NOW()`) automatically bypass caching
- `JsonCodec` — SPI for plugging in external JSON libraries (Jackson, Gson, etc.)
- `JsonValue` — sealed interface providing a library-native, dependency-free JSON abstraction

### Configuration & Security Defaults
- `Sql4jsonSettings` — immutable top-level settings record composed of four subsection records; customize any subsection via `Sql4jsonSettings.builder()`
- **`SecuritySettings`:** `maxLikeWildcards` (soft-ReDoS guard on `LIKE` patterns), `redactErrorDetails` (multi-tenant information-disclosure guard)
- **`LimitsSettings`:** `maxSqlLength`, `maxSubqueryDepth` (default 16), `maxInListSize`, `maxRowsPerQuery` (enforced at GROUP BY, ORDER BY, WINDOW, JOIN, DISTINCT, and the final pipeline/streaming sink)
- **`CacheSettings`:** bounded LIKE-pattern cache (`likePatternCacheSize`), optional LRU query-result cache (`queryResultCacheEnabled`, `queryResultCacheSize`), plus a pluggable `QueryResultCache` SPI
- **`DefaultJsonCodecSettings`:** `maxInputLength`, `maxNestingDepth`, `maxStringLength`, `maxNumberLength`, `maxPropertyNameLength`, `maxArrayElements`, `duplicateKeyPolicy`
- `DuplicateKeyPolicy` enum — `REJECT` (default), `FIRST_WINS`, `LAST_WINS` — resolves RFC 8259 ambiguity on duplicate object keys
- Conservative defaults out of the box — protect against denial-of-service and information-disclosure risks in multi-tenant deployments without requiring any configuration

### Type System
- `JsonValue` sealed interface: object, array, string, number, boolean, null
- `SqlValue` sealed interface for typed query processing: string, number, boolean, date, datetime, null
- Sealed exception hierarchy: `SQL4JsonException` → `SQL4JsonParseException` (with line/column), `SQL4JsonExecutionException`

### Build & Infrastructure
- Java 21+ required (JPMS module with explicit exports)
- Published to Maven Central (`io.github.mnesimiyilmaz:sql4json`)
- GitHub Actions CI (formatting check + build + test)
- Spotless code formatting, CycloneDX SBOM generation
- OWASP dependency-check plugin
- Dependabot for automated dependency updates

[1.2.0]: https://github.com/mnesimiyilmaz/sql4json/releases/tag/v1.2.0
[1.1.0]: https://github.com/mnesimiyilmaz/sql4json/releases/tag/v1.1.0
[1.0.0]: https://github.com/mnesimiyilmaz/sql4json/releases/tag/v1.0.0
