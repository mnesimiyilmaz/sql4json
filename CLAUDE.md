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
# Run multiple methods in one class
./mvnw test -Dtest='ClassName#m1+m2+m3'

# Dry-run the full release profile without publishing or signing —
# builds sources + javadoc jars and exercises the release pipeline so
# release-only failures surface before tagging.
./mvnw clean deploy -Prelease -DskipPublishing=true -Dgpg.skip=true
```

Tests use JUnit 5 covering unit tests, integration tests, edge cases, and performance. **Gotcha:** Default `./mvnw test` excludes tests tagged `@Tag("large")` — run `./mvnw test -Plarge-tests` to execute only those. CI runs `spotless:check` then `clean verify` via GitHub Actions (`ci.yml`).

**Never run two `./mvnw` invocations in parallel against this repo** — any goal that touches `target/` (including `clean`) races with any other goal reading it and fails with "Failed to delete target". Run maven commands sequentially.

**Reliable way to run an isolated Java snippet:** write a throwaway `@Test` under `src/test/java` and run via `-Dtest=`. Ad-hoc `javac -cp ...` against the Maven local repo is brittle (classpath assembly, Windows separator issues) — use the maven harness.

## Public API

Nine usage patterns (all via static methods on `SQL4Json`):

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

// 7. Typed result — record / POJO / scalar (single row)
record Person(String name, int age) {}
Person p = SQL4Json.queryAs(sql, jsonString, Person.class);
// or via JsonValue
Person p2 = SQL4Json.queryAsJsonValue(sql, jsonString).as(Person.class);

// 8. Typed result — List<T>
List<Person> people = SQL4Json.queryAsList(sql, jsonString, Person.class);
// Custom missing-field policy:
Sql4jsonSettings strict = Sql4jsonSettings.builder()
    .mapping(m -> m.missingFieldPolicy(MissingFieldPolicy.FAIL))
    .build();
List<Person> strictPeople = SQL4Json.queryAsList(sql, jsonString, Person.class, strict);

// 9. Parameterized query (JDBC-style parameter binding — PreparedQuery / Engine only)
PreparedQuery q = SQL4Json.prepare("SELECT * FROM $r WHERE age > :min AND dept = :d");
String result = q.execute(jsonString, BoundParameters.named()
    .bind("min", 25).bind("d", "Engineering"));

// Positional + IN expansion:
PreparedQuery q2 = SQL4Json.prepare("SELECT * FROM $r WHERE id IN (?)");
String r2 = q2.execute(jsonString, BoundParameters.of(List.of(1, 2, 3)));

// Combined with typed result:
List<Person> engineers = SQL4Json.prepare("SELECT * FROM $r WHERE dept = :d")
    .executeAsList(jsonString, Person.class, BoundParameters.named().bind("d", "Engineering"));
```

Key public types: `SQL4Json`, `PreparedQuery`, `SQL4JsonEngine`, `SQL4JsonEngineBuilder`, `Sql4jsonSettings`, `SecuritySettings`, `LimitsSettings`, `CacheSettings`, `MappingSettings`, `MissingFieldPolicy`, `DefaultJsonCodecSettings`, `DuplicateKeyPolicy`, `JsonCodec`, `QueryResultCache` (cache SPI), `JsonValue` (sealed), `SQL4JsonException` (sealed), `SQL4JsonMappingException`, `BoundParameters`, `SQL4JsonBindException`.

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

Key packages: `engine/` (QueryExecutor, QueryPipeline, Expression, ExpressionEvaluator, Row, FieldKey, WindowSpec, JoinExecutor, JoinKey, StreamMaterializer), `engine/stage/` (WhereStage, GroupByStage, HavingStage, WindowStage, OrderByStage, TopNOrderByStage, LimitStage, SelectStage, DistinctStage), `parser/` (QueryParser, QueryDefinition, SQL4JsonParserListener, JoinDef, JoinType, JoinEquality), `registry/` (FunctionRegistry, OperatorRegistry, ConditionHandlerRegistry, CriteriaNode), `json/` (JsonParser, JsonSerializer, DefaultJsonCodec, JsonFlattener, JsonUnflattener, StreamingJsonParser, StreamingSerializer, CompactStringMap, JsonToSqlConverter, IsoTemporals, JsonValue records), `mapper/` (JsonValueMapper, TypeDescriptor, TypeIntrospection, MappingPath, VisitedStack — internal, not exported), `settings/` (Sql4jsonSettings, SecuritySettings, LimitsSettings, CacheSettings, MappingSettings, MissingFieldPolicy, DefaultJsonCodecSettings, DuplicateKeyPolicy), `types/` (SqlValue hierarchy), `sorting/` (SqlValueComparator), `grouping/` (GroupAggregator, GroupKey), `exception/` (sealed exception hierarchy).

## Code Conventions

- **Sealed types:** Used extensively — sealed interfaces (`JsonValue`, `SqlValue`, `SQL4JsonException`, `Expression`) with record implementations for type-safe pattern matching.
- **Expression AST:** Sealed `Expression` interface (`ColumnRef`, `ScalarFnCall`, `AggregateFnCall`, `LiteralVal`, `WindowFnCall`, `SimpleCaseWhen`, `SearchedCaseWhen`, `NowRef`, `ParameterRef`) represents all column expressions. `ExpressionEvaluator` is the tree-walking interpreter — used by all pipeline stages, condition handlers, and aggregation. `WindowFnCall` is evaluated by `WindowStage`, not by `ExpressionEvaluator`. Functions nest arbitrarily: `ROUND(AVG(NULLIF(col, 0)), 2)`.
- **AST cross-hierarchy trap:** `Expression` and `CriteriaNode` are two separate sealed hierarchies that intersect inside `SearchedCaseWhen.SearchWhen` (it holds both a `CriteriaNode` condition and an `Expression` result). Any transformation or analysis that walks `Expression` must explicitly descend into the embedded `CriteriaNode` too — recursing only through the expression branches silently misses WHEN conditions.
- **Adding a new sealed variant is cross-cutting:** a new `Expression` / `CriteriaNode` / `SqlValue` / `JsonValue` subtype requires updating every exhaustive `switch` on the parent. Grep the sealed parent name before adding.
- **JSON flattening:** Core mechanism. Nested JSON is flattened to `Map<FieldKey, Object>` for processing, then unflattened for output. `FieldKey` tracks the "family" (base path) for nested field grouping.
- **ANTLR generated code:** Located in `target/generated-sources/antlr4/`. Never edit generated files — modify `SQL4Json.g4` in `src/main/antlr4/` instead. Grammar changes require a rebuild (`mvn clean compile`).
- **Settings subsections:** `Sql4jsonSettings` is composed of five immutable record subsections (`security`, `limits`, `cache`, `mapping`, `codec`). Customize via `Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(N)).build()`. `Sql4jsonSettings.defaults()` is a JVM-wide shared singleton — all no-settings API calls land on it so the `BoundedPatternCache` and derived `ConditionHandlerRegistry` are shared.
- **JSON codec limits:** Built-in JSON parser limits live in `DefaultJsonCodecSettings` (in the `settings` package, not `json`). Customize via `Sql4jsonSettings.builder().codec(new DefaultJsonCodec(DefaultJsonCodecSettings.builder()...)).build()`.
- **Registry lifetime:** `ConditionHandlerRegistry.forSettings(settings)` is the single entry point and internally caches one registry per distinct `cache.likePatternCacheSize` — in practice one per JVM, since nearly every caller uses defaults. It shares a `BoundedPatternCache` between `LikeConditionHandler` and `NotLikeConditionHandler` (both package-private inside the `registry` package).
- **Functional style:** Heavy use of Java streams, `BiPredicate`, `Function`, `Supplier`, `Optional`
- **Spotless:** Code formatter configured — removes unused imports, trims trailing whitespace, ensures final newline. Run `./mvnw spotless:apply` before committing.
- **Surefire config:** Tests run with `--add-modules=java.management` and `--add-reads=io.github.mnesimiyilmaz.sql4json=java.management` JVM flags, plus `-Xmx8g -Xms1g`.

## Code-Quality Guidelines (SonarQube-enforced)

The repo is scanned by SonarQube; write new code with these general principles in mind so findings don't accumulate.

- **Prefer record patterns over pattern variables.** When matching a record with `instanceof`, deconstruct its components directly rather than binding a variable and calling accessors on it. Java 21 record patterns are the default style here.
- **Empty method bodies need a nested comment.** Javadoc above the signature doesn't satisfy the rule — put a short `//` inside the body stating why it's empty. Applies to constructors too.
- **Keep cognitive complexity low.** Long `if/else` chains and deeply nested branches should be broken into small focused helpers. Reach for suppression only when extraction would genuinely hurt readability.
- **Suppress rules sparingly, and only with a reason.** Use the narrowest scope (declaration or method, not whole class unless the rule is class-level) and add a one-line comment above the annotation stating why the suppression is correct.
- **Use idiomatic Java 21 APIs.** Prefer modern sequenced-collection and convenience methods (`getFirst()`, `getLast()`, `reversed()`, `addFirst/Last`, etc.) over positional index arithmetic like `list.get(0)` or `list.get(list.size() - 1)`. Similarly, lean on switch expressions, pattern matching, `var`, and `List.of`/`Map.of` where they improve clarity.
- **No dangling Javadoc.** Don't place `/** ... */` blocks where the Javadoc tool won't attach them — most commonly on individual record components inline in the header. Document components via `@param` tags on the record's top-level Javadoc instead. The same applies to local variables, statements, and any other position that isn't a declaration.

## Development Workflow

- **Javadoc is mandatory:** Every new development (public types, methods, fields) must include a Java API doc comment. Do not skip this step — write Javadoc as part of the implementation, not as a follow-up.
- **Test placement:** Before creating a new test class, look for an existing test class where the new tests logically belong and add them there. Only create a new test file when no appropriate existing class exists — and state the reason explicitly in the response.
- **`@since` on public API:** Any new public API addition must carry an `@since` Javadoc tag with the version number the change ships in. If the user has not specified a version number, ask before adding the tag. If the user declines to specify one, omit the `@since` tag rather than guessing.
- **Tests must assert, not log:** a `try { ... } catch (Throwable t) { System.out.println(t); }` test passes silently even when the code throws. For "must not throw" / "must return X" behavior, assert on the success path so failure is a red CI; use `assertThrows` only when the throw is the contract.

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
- **Parameters:** `?` (positional) and `:name` (named) placeholders in WHERE / SELECT / GROUP BY / HAVING / function args / LIMIT / OFFSET. Bound via `BoundParameters` on `PreparedQuery.execute(...)` or `SQL4JsonEngine.query(...)`. Cannot mix `?` and `:name` in the same query (parse-time error). IN-list expansion: `IN (?)` + collection bind expands to N literals; empty collection → zero-row predicate. Null bind produces SQL-standard `col = NULL` (always false) — use literal `IS NULL` / `IS NOT NULL` for nullability tests (cannot be parameterized). `SQL4JsonEngine` bypasses result cache for parameterized queries. `LimitsSettings.maxParameters` (default 1024) caps total placeholder count per query.
