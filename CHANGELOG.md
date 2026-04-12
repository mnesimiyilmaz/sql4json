# Changelog

All notable changes to SQL4Json are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.0.0]: https://github.com/mnesimiyilmaz/sql4json/releases/tag/v1.0.0
