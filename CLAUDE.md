# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQL4Json is a Java library that enables SQL-like querying of JSON data. It parses SQL SELECT statements via an ANTLR4 grammar and applies them to JSON objects — supporting filtering, aggregation, sorting, and nested queries without a database.

- **Language:** Java 21 (source/target/release 21) — enforced via maven-enforcer-plugin
- **Build:** Maven 3.9+ (use `./mvnw` wrapper when available)
- **Package:** `io.github.mnesimiyilmaz.sql4json`
- **Module:** JPMS module `io.github.mnesimiyilmaz.sql4json` — exports `sql4json`, `sql4json.exception`, `sql4json.settings`, and `sql4json.types`

## Build & Test Commands

```bash
./mvnw clean compile            # Compile (also generates ANTLR sources)
./mvnw clean test               # Run all tests
./mvnw clean package            # Package jar-with-dependencies
./mvnw clean package -DskipTests  # Package without tests
./mvnw spotless:check           # Check code formatting
./mvnw spotless:apply           # Auto-fix code formatting

# Run a single test
./mvnw test -Dtest="SQL4JsonQueryTests#when_select_asterisk_then_return_all"
```

Tests use JUnit 5 covering unit tests, integration tests, edge cases, and performance. **Gotcha:** Default `./mvnw test` excludes tests tagged `@Tag("large")` — run `./mvnw test -Plarge-tests` to execute only those. CI runs `spotless:check` then `clean verify` via GitHub Actions (`ci.yml`).

## Public API

Six usage patterns (all via static methods on `SQL4Json`):

```java
// 1. One-off query with defaults
String result = SQL4Json.query(sql, jsonString);

// 2. One-off query with custom settings
Sql4jsonSettings strict = Sql4jsonSettings.builder()
    .limits(l -> l.maxRowsPerQuery(10_000))
    .build();
String result = SQL4Json.query(sql, jsonString, strict);

// 3. Prepared query — parse once, execute many (thread-safe)
PreparedQuery q = SQL4Json.prepare(sql);
// or with custom settings:
PreparedQuery q = SQL4Json.prepare(sql, settings);
String r1 = q.execute(json1);
String r2 = q.execute(json2);

// 4. Engine — bound data + optional LRU result cache
SQL4JsonEngine engine = SQL4Json.engine()
    .settings(Sql4jsonSettings.builder()
        .cache(c -> c.queryResultCacheEnabled(true))
        .build())
    .data(jsonString)
    .build();
String result = engine.query(sql);

// 5. Custom codec via settings (no separate overload)
Sql4jsonSettings withCodec = Sql4jsonSettings.builder()
    .codec(new DefaultJsonCodec(
        DefaultJsonCodecSettings.builder()
            .maxInputLength(50 * 1024 * 1024)
            .duplicateKeyPolicy(DuplicateKeyPolicy.LAST_WINS)
            .build()))
    .build();
String result = SQL4Json.query(sql, jsonString, withCodec);

// 6. Multi-source JOIN query (named data sources)
String result = SQL4Json.query(joinSql, Map.of("users", usersJson, "orders", ordersJson));
// Or via Engine:
SQL4JsonEngine engine = SQL4Json.engine()
    .data("users", usersJson)
    .data("orders", ordersJson)
    .build();
String result = engine.query(joinSql);
```

Key public types: `SQL4Json`, `PreparedQuery`, `SQL4JsonEngine`, `SQL4JsonEngineBuilder`, `Sql4jsonSettings`, `SecuritySettings`, `LimitsSettings`, `CacheSettings`, `DefaultJsonCodecSettings`, `DuplicateKeyPolicy`, `JsonCodec`, `QueryResultCache` (cache SPI), `JsonValue` (sealed), `SQL4JsonException` (sealed).

## Architecture & Data Flow

`Sql4jsonSettings` is threaded through every step — `QueryParser`, `QueryExecutor`, and every materializing pipeline stage see the same instance.

```
Input JSON + SQL
  → Entry point (SQL4Json / PreparedQuery / SQL4JsonEngine)
    → Validation (query length, null checks)
    → [SQL4JsonEngine only: check QueryResultCache]
    → JsonParser (parses JSON string → JsonValue tree, with limits from DefaultJsonCodecSettings:
        maxInputLength, maxNestingDepth, maxStringLength, maxNumberLength,
        maxPropertyNameLength, maxArrayElements, duplicateKeyPolicy)
    → QueryParser (ANTLR lexer/parser → SQL4JsonParserListener → QueryDefinition record)
    → QueryExecutor orchestrates the pipeline:
        1a. Single-source: JsonFlattener streams rows lazily from JSON
        1b. Multi-source (JOIN): Resolve named sources → flatten with alias prefix
            → JoinExecutor chains hash JOINs (build hash map, probe) → merged rows
        2. Expression trees evaluated via ExpressionEvaluator (tree-walking interpreter)
        3. QueryPipeline (staged execution):
           WHERE (lazy) → GROUP BY (materializing) → HAVING →
           WINDOW (materializing) → ORDER BY (materializing) → LIMIT → SELECT (lazy) → DISTINCT
           Row-count enforcement: GROUP BY, ORDER BY, WINDOW, JOIN, DISTINCT, PIPELINE,
           and STREAMING all enforce maxRowsPerQuery — exceeding throws
           SQL4JsonExecutionException with message "<STAGE> row count exceeds configured maximum (<N>)"
        4. JsonUnflattener: reconstruct JSON from flat rows
    → JsonSerializer: serialize JsonValue result back to String
  → Returns String (or JsonValue via queryAsJsonValue overloads)
```

Key packages: `engine/` (QueryExecutor, QueryPipeline, Expression, ExpressionEvaluator, Row, FieldKey, WindowSpec, JoinExecutor, JoinKey, StreamMaterializer), `engine/stage/` (WhereStage, GroupByStage, HavingStage, WindowStage, OrderByStage, TopNOrderByStage, LimitStage, SelectStage, DistinctStage), `parser/` (QueryParser, QueryDefinition, SQL4JsonParserListener, JoinDef, JoinType, JoinEquality), `registry/` (FunctionRegistry, OperatorRegistry, ConditionHandlerRegistry, CriteriaNode), `json/` (JsonParser, JsonSerializer, DefaultJsonCodec, JsonFlattener, JsonUnflattener, StreamingJsonParser, StreamingSerializer, CompactStringMap, JsonToSqlConverter, JsonValue records), `settings/` (Sql4jsonSettings, SecuritySettings, LimitsSettings, CacheSettings, DefaultJsonCodecSettings, DuplicateKeyPolicy), `types/` (SqlValue hierarchy), `sorting/` (SqlValueComparator), `grouping/` (GroupAggregator, GroupKey), `exception/` (sealed exception hierarchy).

## Code Conventions

- **Sealed types:** Used extensively — sealed interfaces (`JsonValue`, `SqlValue`, `SQL4JsonException`, `Expression`) with record implementations for type-safe pattern matching.
- **Expression AST:** Sealed `Expression` interface (`ColumnRef`, `ScalarFnCall`, `AggregateFnCall`, `LiteralVal`, `WindowFnCall`, `SimpleCaseWhen`, `SearchedCaseWhen`, `NowRef`) represents all column expressions. `ExpressionEvaluator` is the tree-walking interpreter — used by all pipeline stages, condition handlers, and aggregation. `WindowFnCall` is evaluated by `WindowStage`, not by `ExpressionEvaluator`. Functions nest arbitrarily: `ROUND(AVG(NULLIF(col, 0)), 2)`.
- **JSON flattening:** Core mechanism. Nested JSON is flattened to `Map<FieldKey, Object>` for processing, then unflattened for output. `FieldKey` tracks the "family" (base path) for nested field grouping.
- **ANTLR generated code:** Located in `target/generated-sources/antlr4/`. Never edit generated files — modify `SQL4Json.g4` in `src/main/antlr4/` instead. Grammar changes require a rebuild (`mvn clean compile`).
- **Settings subsections:** `Sql4jsonSettings` is composed of four immutable record subsections (`security`, `limits`, `cache`, `codec`). Customize via `Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(N)).build()`. `Sql4jsonSettings.defaults()` is a JVM-wide shared singleton — all no-settings API calls land on it so the `BoundedPatternCache` and derived `ConditionHandlerRegistry` are shared.
- **JSON codec limits:** Built-in JSON parser limits live in `DefaultJsonCodecSettings` (in the `settings` package, not `json`). Customize via `Sql4jsonSettings.builder().codec(new DefaultJsonCodec(DefaultJsonCodecSettings.builder()...)).build()`.
- **Registry lifetime:** `ConditionHandlerRegistry.forSettings(settings)` is the single entry point and internally caches one registry per distinct `cache.likePatternCacheSize` — in practice one per JVM, since nearly every caller uses defaults. It shares a `BoundedPatternCache` between `LikeConditionHandler` and `NotLikeConditionHandler` (both package-private inside the `registry` package).
- **Functional style:** Heavy use of Java streams, `BiPredicate`, `Function`, `Supplier`, `Optional`
- **Spotless:** Code formatter configured — removes unused imports, trims trailing whitespace, ensures final newline. Run `./mvnw spotless:apply` before committing.
- **Surefire config:** Tests run with `--add-modules=java.management` and `--add-reads=io.github.mnesimiyilmaz.sql4json=java.management` JVM flags, plus `-Xmx8g -Xms1g`.

## SQL Syntax

- `SELECT *`, specific columns, aliases (`AS`), aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
- `FROM $r` (root reference), nested paths (`$r.data.items`), or table names for JOINs (`FROM users u`)
- `JOIN` / `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN` with equality ON conditions (`ON a.col = b.col AND ...`). Chained joins supported. Requires named data sources via `Map` or `SQL4JsonEngineBuilder.data(name, json)`.
- `WHERE` with `=`, `!=`, `<`, `>`, `<=`, `>=`, `LIKE`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, parentheses
- `GROUP BY` with `HAVING` (HAVING uses SELECT aliases, not original field names)
- `ORDER BY` with `ASC`/`DESC`
- **Nested queries:** Subquery in FROM clause (bounded by `Sql4jsonSettings.limits().maxSubqueryDepth()`, default 16)
- `DISTINCT` for duplicate elimination
- `LIMIT` and `OFFSET` for pagination
- `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`, `NOT LIKE` operators
- **Nested function calls:** Functions nest arbitrarily in SELECT, WHERE, GROUP BY, ORDER BY, HAVING: `AVG(NULLIF(col, 0))`, `ROUND(AVG(salary), 2)`, `LPAD(TRIM(NULLIF(col, '')), 10, '*')`
- **Window functions:** `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`, `LAG(col [, offset])`, `LEAD(col [, offset])` with `OVER (PARTITION BY ... ORDER BY ...)`. Aggregate functions (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`) also work as window functions with `OVER`. Window functions are only valid in SELECT and do not collapse rows (unlike GROUP BY). Evaluated by `WindowStage` (materializing, runs after HAVING, before ORDER BY).
- `CASE` expressions: simple (`CASE expr WHEN val THEN result END`) and searched (`CASE WHEN condition THEN result END`), fully nestable in SELECT, WHERE, ORDER BY, GROUP BY, HAVING
- **Functions:** String (LOWER, UPPER, CONCAT, SUBSTRING, TRIM, LENGTH, REPLACE, LEFT, RIGHT, LPAD, RPAD, REVERSE, POSITION), Math (ABS, ROUND, CEIL, FLOOR, MOD, POWER, SQRT, SIGN), Date (TO_DATE, NOW, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DATE_ADD, DATE_DIFF), Conversion (CAST, NULLIF, COALESCE), Aggregate (COUNT, SUM, AVG, MIN, MAX), Window (ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, plus aggregate functions with OVER)
