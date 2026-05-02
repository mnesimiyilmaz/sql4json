# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQL4Json is a Java library that enables SQL-like querying of JSON data. It parses SQL SELECT statements via an ANTLR4 grammar and applies them to JSON objects — supporting filtering, aggregation, sorting, and nested queries without a database.

- **Language:** Java 21 (source/target/release 21) — enforced via maven-enforcer-plugin
- **Build:** Maven 3.9+ (use `./mvnw` wrapper when available)
- **Package:** `io.github.mnesimiyilmaz.sql4json`
- **Module:** JPMS module `io.github.mnesimiyilmaz.sql4json` — exports `sql4json`, `sql4json.exception`, `sql4json.grammar`, `sql4json.settings`, and `sql4json.types`

## Build & Test Commands

```bash
./mvnw clean compile            # Compile (also generates ANTLR sources)
./mvnw clean test               # Run all tests
./mvnw clean verify             # Run tests + JaCoCo coverage gate (INSTRUCTION >= 95%, BRANCH >= 90%)
./mvnw clean verify jacoco:report  # Same, plus HTML report at target/site/jacoco/index.html
./mvnw clean package            # Build the library jar plus the shaded CLI jar (classifier `cli`, ANTLR included, Main-Class set)
./mvnw clean package -DskipTests  # Package without tests
./mvnw spotless:check           # Check code formatting
./mvnw spotless:apply           # Auto-fix code formatting

# Run a single test
./mvnw test -Dtest="SQL4JsonQueryTests#when_select_asterisk_then_return_all"
# Run multiple methods in one class
./mvnw test -Dtest='ClassName#m1+m2+m3'

# Run the profiling/scaling sweep — see docs/performance.md for the published numbers.
# Each scenario runs N times; -Dprofiling.runs=N (default 3, median is the official number).
./mvnw test -Plarge-tests -Dtest=ProfilingTest -Dprofiling.runs=3

# Dry-run the full release profile without publishing, signing, or hitting the NVD —
# builds sources + javadoc jars and exercises the release pipeline so
# release-only failures surface before tagging. Skip OWASP locally (the release profile
# binds it to verify; without NVD_API_KEY it falls back to the public feed and is slow).
./mvnw clean deploy -Prelease -DskipPublishing=true -Dgpg.skip=true -Ddependency-check.skip=true
```

Tests use JUnit 5 covering unit tests, integration tests, edge cases, and performance. **Gotcha:** Default `./mvnw test` excludes tests tagged `@Tag("large")` — run `./mvnw test -Plarge-tests` to execute only those. CI runs `spotless:check` then `clean verify` via GitHub Actions (`ci.yml`).

**Coverage gate:** The `verify` phase enforces a JaCoCo BUNDLE gate — **INSTRUCTION >= 95%** and **BRANCH >= 90%** (configured in `pom.xml`, generated ANTLR sources excluded). Run `./mvnw clean verify` at meaningful checkpoints during development — don't ship code that drops below these thresholds.

**Never run two `./mvnw` invocations in parallel against this repo** — any goal that touches `target/` (including `clean`) races with any other goal reading it and fails with "Failed to delete target". Run maven commands sequentially.

**Reliable way to run an isolated Java snippet:** write a throwaway `@Test` under `src/test/java` and run via `-Dtest=`. Ad-hoc `javac -cp ...` against the Maven local repo is brittle (classpath assembly, Windows separator issues) — use the maven harness.

## Public API

Ten usage patterns (most via static methods on `SQL4Json`; pattern 10 is a separate read-only grammar catalog):

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

// 10. Grammar introspection (IDE / tooling consumers — no ANTLR types leaked)
List<String> keywords        = SQL4JsonGrammar.keywords();
List<FunctionInfo> functions = SQL4JsonGrammar.functions();   // category, arity, signature, description
List<Token> tokens           = SQL4JsonGrammar.tokenize("SELECT * FROM $r");
```

Key public types: `SQL4Json`, `PreparedQuery`, `SQL4JsonEngine`, `SQL4JsonEngineBuilder`, `Sql4jsonSettings`, `SecuritySettings`, `LimitsSettings`, `CacheSettings`, `MappingSettings`, `MissingFieldPolicy`, `DefaultJsonCodecSettings`, `DuplicateKeyPolicy`, `JsonCodec`, `QueryResultCache` (cache SPI), `JsonValue` (sealed), `SQL4JsonException` (sealed), `SQL4JsonMappingException`, `BoundParameters`, `SQL4JsonBindException`, `SQL4JsonGrammar`, `FunctionInfo`, `Category`, `Token`, `TokenKind`.

### CLI

A command-line entrypoint ships as a separate shaded jar with classifier `cli`
(`sql4json-X.Y.Z-cli.jar`). The library jar is unchanged — pure library, no `Main-Class`.

```bash
java -jar sql4json-X.Y.Z-cli.jar -q "SELECT * FROM \$r WHERE age > 25" -f data.json
cat data.json | java -jar sql4json-X.Y.Z-cli.jar -q @query.sql --pretty
java -jar sql4json-X.Y.Z-cli.jar -q @join.sql --data users=u.json --data orders=o.json
java -jar sql4json-X.Y.Z-cli.jar -q "SELECT * FROM \$r WHERE id = :id" -p id=42 -f d.json
```

The `cli` package (`io.github.mnesimiyilmaz.sql4json.cli`) is **intentionally non-exported**
from `module-info.java` — `Main`, `CliRunner`, `ArgParser`, etc. are not part of the
library's stable API surface; the *flag set* and *exit codes* are. `--help` (exit 0),
`--version` (exit 0), invalid usage (exit 2), execution failure (exit 1).

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

Key packages: `engine/` (QueryExecutor, QueryPipeline, Expression, ExpressionEvaluator, Row, FlatRow, RowAccessor, RowSchema, FieldKey, WindowSpec, JoinExecutor, JoinKey, StreamMaterializer, ParameterSubstitutor), `engine/stage/` (WhereStage, GroupByStage, HavingStage, WindowStage, OrderByStage, TopNOrderByStage, LimitStage, SelectStage, DistinctStage), `parser/` (QueryParser, QueryDefinition, SQL4JsonParserListener, JoinDef, JoinType, JoinEquality, ParameterPositionKind, ParameterConverter), `registry/` (FunctionRegistry, OperatorRegistry, ConditionHandlerRegistry, CriteriaNode, ArrayPathNavigator, ArrayPredicateConditionHandler, plus the per-operator `*ConditionHandler` set), `json/` (JsonParser, JsonSerializer, DefaultJsonCodec, JsonFlattener, JsonUnflattener, StreamingJsonParser, StreamingSerializer, CompactStringMap, JsonToSqlConverter, IsoTemporals, sealed `JsonValue` records — `JsonObjectValue`, `JsonArrayValue`, `JsonStringValue`, `JsonBooleanValue`, `JsonNullValue`, plus the sealed `JsonNumberValue` family `JsonLongValue` / `JsonDoubleValue` / `JsonDecimalValue`), `mapper/` (JsonValueMapper, TypeDescriptor, TypeIntrospection, MappingPath, VisitedStack — internal, not exported), `settings/` (Sql4jsonSettings, SecuritySettings, LimitsSettings, CacheSettings, MappingSettings, MissingFieldPolicy, DefaultJsonCodecSettings, DuplicateKeyPolicy), `types/` (SqlValue hierarchy plus the sealed `SqlNumber` family `SqlLong` / `SqlDouble` / `SqlDecimal`, JsonCodec SPI), `sorting/` (SqlValueComparator), `grouping/` (GroupAggregator, GroupKey), `exception/` (sealed exception hierarchy), `grammar/` (public IDE-tooling catalog: SQL4JsonGrammar, FunctionInfo, Category, Token, TokenKind), `cli/` (intentionally non-exported: Main, CliRunner, ArgParser, ParamValueParser, ParsedArgs, UsageException), `internal/` (SkipCoverageGenerated annotation).

## Code Conventions

- **Separation of Concerns — single source of truth.** Don't duplicate domain knowledge across layers. Function names, keyword sets, type rules, category labels, limits, and similar facts must live in one place — typically the grammar, the registry, `SQL4JsonGrammar.FUNCTIONS`, or `Sql4jsonSettings`. Before hardcoding a list / set / map / switch in a class that doesn't own that knowledge, **check whether the same information already exists elsewhere and derive from it** (e.g. via a stream filter or a public accessor). A class should only carry knowledge it owns; everything else it should query. Duplication = guaranteed drift; the next person who edits one site will forget the other. The drift tests in `SQL4JsonGrammarDriftTest` exist precisely because we accepted some duplication for IDE-tooling needs — they are a last-line defence, not an excuse to add more copies.
- **Sealed types:** Used extensively — sealed interfaces (`JsonValue`, `SqlValue`, `SQL4JsonException`, `Expression`) with record implementations for type-safe pattern matching.
- **Expression AST:** Sealed `Expression` interface (`ColumnRef`, `ScalarFnCall`, `AggregateFnCall`, `LiteralVal`, `WindowFnCall`, `SimpleCaseWhen`, `SearchedCaseWhen`, `NowRef`, `ParameterRef`) represents all column expressions. `ExpressionEvaluator` is the tree-walking interpreter — used by all pipeline stages, condition handlers, and aggregation. `WindowFnCall` is **computed** by `WindowStage` (which stores the result on the row) and **looked up** by `ExpressionEvaluator` when it encounters a `WindowFnCall` node, so wrappers like `ROUND(AVG(x) OVER (...), 2)` and windows buried inside CASE WHEN conditions evaluate through the normal expression path. Functions nest arbitrarily: `ROUND(AVG(NULLIF(col, 0)), 2)`.
- **Window dispatch & Row state (since 1.2.0):** `WindowStage` consumes the parser-collected `QueryDefinition.windowFunctionCalls()` list and an alias map (`aliasKeysByCall`) derived from SELECT columns whose top-level expression IS a `WindowFnCall`. Builds a window-aware `RowSchema` via `withWindowSlots(calls, aliases)` — the alias becomes the canonical column key for the slot, so `ORDER BY alias` / any `ColumnRef` lookup against the alias resolves naturally through the schema index. Wrapped windows (`ROUND(... OVER (...))`) get a synthetic column key and resolve through `getWindowResult(WindowFnCall)` → `RowSchema.windowSlot`. `WindowStage` writes per-row results into per-row `Object[]` working buffers indexed by slot ordinal, and emits `FlatRow.of(schema, vals)` directly into the `Stream<RowAccessor>` pipeline. The prior `windowResults` / `windowResultsByAlias` maps on `Row` are gone. `Row.flatSchema` is non-null only on lazy rows that retained a schema reference; for the regular pipeline path the schema lives on the `FlatRow` itself and `getWindowResult` / `hasWindowResults` consult it via the `RowAccessor` interface. `JsonUnflattener` accepts `RowAccessor` and dispatches on `instanceof FlatRow` for the SELECT * asterisk case; aggregated-row routing reads the row's `isAggregated()` flag (value-tagged on both `Row` and `FlatRow`).
- **`RowAccessor` sealed bridge (since 1.2.0):** `RowAccessor` is a `sealed interface permits Row, FlatRow` exposed across the engine. `Row` is the lazy on-demand-flatten view (still used for streaming WHERE / lazy SELECT and as input to GROUP BY / WINDOW); `FlatRow` is the materialized `Object[]`-backed shape used by every materializing stage (GROUP BY, HAVING, WINDOW, ORDER BY, JOIN, DISTINCT, SELECT projection, engine pre-flatten). Pipeline stages take `Stream<RowAccessor>`; `ExpressionEvaluator`, every `*ConditionHandler`, `CriteriaNode`, `ArrayPathNavigator`, and `JsonUnflattener` accept `RowAccessor`. Don't reach for a `(Row) row` cast in handler / evaluator paths — write against `RowAccessor` and add `keys()` / `entries()` to the interface if you need flat iteration.
- **`NOW()` dispatch (since 1.2.0):** `NOW()` is not a grammar literal — it lexes as a regular zero-arg function call and is intercepted at parse time by `SQL4JsonParserListener.resolveFunctionCallExpression` / `tryDispatchRhsValueFunction` / `resolveRhsFunctionCall`. The result is `Expression.NowRef` (lazy, per-row) in column-expression / comparison-RHS / IN-list / BETWEEN-bound positions, or an evaluated `SqlDateTime` literal in eager positions (`CAST(NOW() AS …)` inner, nested function args on RHS). Every dispatch path sets `containsNonDeterministic = true` so the query-result cache bypasses `NOW()`-bearing queries. Adding a new zero-arg value function only requires registering it via `FunctionRegistry.registerValue(...)`.
- **Non-literal `IN` / `BETWEEN` operands:** When an IN-list element or a BETWEEN bound is anything other than a `LiteralVal` (e.g. a `ParameterRef` or `NowRef`), `SQL4JsonParserListener` routes it through `ConditionContext.valueExpressions` / `lowerBoundExpr` / `upperBoundExpr`. The handlers (`InConditionHandler`, `BetweenConditionHandler`) evaluate those expressions per row. `ParameterSubstitutor` snapshots `NowRef` to a literal `SqlDateTime` at substitution time (so all rows in a parameterized execution see the same timestamp); in non-parameterized queries the `NowRef` survives into the handler and is re-evaluated per row. Document any future divergence in the `BoundParameters` Javadoc.
- **Grammar API drift tests (since 1.2.0):** The public `io.github.mnesimiyilmaz.sql4json.grammar` package contains three hand-maintained tables — `SQL4JsonGrammar.KEYWORDS`, `FUNCTIONS`, and `TOKEN_KIND_BY_TYPE`. Every change to the ANTLR grammar (`SQL4Json.g4`) or `FunctionRegistry` must be mirrored in these tables. The drift tests in `SQL4JsonGrammarDriftTest` enforce this bidirectionally and fail CI on rot. When adding a new keyword, function, or lexer rule, run `./mvnw test -Dtest=SQL4JsonGrammarDriftTest` to confirm the catalog is in sync.
- **AST cross-hierarchy trap:** `Expression` and `CriteriaNode` are two separate sealed hierarchies that intersect inside `SearchedCaseWhen.SearchWhen` (it holds both a `CriteriaNode` condition and an `Expression` result). Any transformation or analysis that walks `Expression` must explicitly descend into the embedded `CriteriaNode` too — recursing only through the expression branches silently misses WHEN conditions.
- **Array predicates bypass `SqlValue` (since 1.2.0):** the array operators `CONTAINS`, `@>`, `<@`, `&&`, and `=`/`!=` with `ARRAY[…]` RHS are routed to `ArrayPredicateConditionHandler`, which navigates `Row.originalValue()` directly to a `JsonArrayValue` — falling back to flat-key reassembly via `Row.valuesByFamily(path)` for post-JOIN merged rows. Arrays never become `SqlValue`s; equality is delegated to `SqlValueComparator` element-wise. Don't reflexively introduce a `SqlArray` type to "support arrays" — it isn't needed for these operators and would ripple across every sealed switch on `SqlValue`. A future phase that adds value-returning array ops (`ARRAY_LENGTH`, `ARRAY_POSITION`, element indexing) is the right place to revisit that decision.
- **Adding a new sealed variant is cross-cutting:** a new `Expression` / `CriteriaNode` / `SqlValue` / `JsonValue` subtype requires updating every exhaustive `switch` on the parent. Grep the sealed parent name before adding.
- **JSON flattening:** Core mechanism. Nested JSON is flattened to `Map<FieldKey, Object>` for processing, then unflattened for output. `FieldKey` tracks the "family" (base path) for nested field grouping.
- **ANTLR generated code:** Located in `target/generated-sources/antlr4/`. Never edit generated files — modify `SQL4Json.g4` in `src/main/antlr4/` instead. Grammar changes require a rebuild (`mvn clean compile`).
- **Settings subsections:** `Sql4jsonSettings` is composed of five immutable record subsections (`security`, `limits`, `cache`, `mapping`, `codec`). Customize via `Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(N)).build()`. `Sql4jsonSettings.defaults()` is a JVM-wide shared singleton — all no-settings API calls land on it so the `BoundedPatternCache` and derived `ConditionHandlerRegistry` are shared.
- **JSON codec limits:** Built-in JSON parser limits live in `DefaultJsonCodecSettings` (in the `settings` package, not `json`). Customize via `Sql4jsonSettings.builder().codec(new DefaultJsonCodec(DefaultJsonCodecSettings.builder()...)).build()`.
- **Registry lifetime:** `ConditionHandlerRegistry.forSettings(settings)` is the single entry point and internally caches one registry per distinct `cache.likePatternCacheSize` — in practice one per JVM, since nearly every caller uses defaults. It shares a `BoundedPatternCache` between `LikeConditionHandler` and `NotLikeConditionHandler` (both package-private inside the `registry` package).
- **String function coerce (since 1.2.0):** the twelve string functions (`LOWER`, `UPPER`, `SUBSTRING`, `TRIM`, `LENGTH`, `LEFT`, `RIGHT`, `LPAD`, `RPAD`, `REVERSE`, `REPLACE`, `POSITION`) plus `CONCAT` coerce non-string inputs and string-typed argument positions via the private `FunctionRegistry::coerceToString` helper (`rawValue().toString()`). Null val or a null string-typed argument short-circuits to `SqlNull`. `TO_DATE` keeps a date-type passthrough — `SqlDate`/`SqlDateTime` return as-is; other types coerce-then-parse. Numeric argument positions (e.g. start/length in `SUBSTRING`) keep their existing `(SqlNumber)` cast — wrong-type there is a real user error.
- **Window-only functions require `OVER` (since 1.2.0):** `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `LAG`, `LEAD` are rejected at parse time when called without `OVER (...)`. The check (`SQL4JsonParserListener::rejectIfWindowOnly`) lives at all three function-dispatch entry points: `resolveFunctionCallExpression` (column-expression position), `tryDispatchRhsValueFunction` (lazy RHS), and `resolveRhsFunctionCall` (eager RHS). The set is **derived** at class-init from `SQL4JsonGrammar.functions()` filtered by `Category.WINDOW` — single source of truth, no hardcoded duplicate. Adding a `WINDOW`-category entry to the grammar catalog automatically extends the guard. Aggregate functions (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) are not affected — they remain valid plain aggregates without `OVER`.
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
- **Coverage gate is non-negotiable:** Maintain INSTRUCTION >= 95% and BRANCH >= 90% on every change. When adding new code (especially new branches in `Expression` / `CriteriaNode` evaluators, condition handlers, or pipeline stages), add tests that cover the new branches in the same change, and re-run `./mvnw clean verify` before declaring the task done. If `jacoco:check` fails, write the missing tests rather than tightening the rule.
- **`@SkipCoverageGenerated` escape hatch (since 1.2.0):** Internal annotation in `internal/SkipCoverageGenerated.java` that JaCoCo (0.8.2+) auto-excludes from coverage analysis — the `Generated` suffix is JaCoCo's trigger; the `SkipCoverage` prefix records the human-readable intent (this is hand-written, not generated). **Use sparingly** — only on perf-critical hot-path helpers whose adaptive `instanceof` false-branches are legitimately reachable at runtime but where synthetic per-branch tests would cost more boilerplate than the optimisation is worth (current call sites: `Row.lazy` cache pre-sizing, `JoinExecutor.estimateFlatSize`, `SQL4JsonEngineBuilder.estimateFlatSize`). The default expectation is still full coverage; reach for this only after confirming a small refactor/extraction can't make the branch testable instead.
- **Tests must assert, not log:** a `try { ... } catch (Throwable t) { System.out.println(t); }` test passes silently even when the code throws. For "must not throw" / "must return X" behavior, assert on the success path so failure is a red CI; use `assertThrows` only when the throw is the contract.
- **CHANGELOG style:** Follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) — top-down, newest version first, ISO date (`YYYY-MM-DD`). Sections per version go in this order, omit if empty: **Added** → **Changed** → **Fixed** → **Dependencies**. One section per kind — never two `### Changed` blocks for the same version. Each entry is a single bullet, one or two sentences, leading with the user-visible behaviour (file paths and method names allowed when they aid orientation; full prose paragraphs are not). Group large additions under an `### Added — <Topic>` subheader (see v1.1.0 "Object Mapping" / "Parameter Binding"). Append a `[x.y.z]: https://github.com/mnesimiyilmaz/sql4json/releases/tag/vX.Y.Z` link reference at the end of the file when adding a new version. Do not edit released entries — once a version ships, its bullets are frozen; corrections go in the next version's section.

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
- **Array predicates (since 1.2.0):** `tags CONTAINS 'admin'` (scalar membership), `tags @> ARRAY['a','b']` (contains-all), `tags <@ ARRAY[…]` (contained-by), `tags && ARRAY[…]` (overlap), and `tags = ARRAY[…]` / `tags != ARRAY[…]` (structural equality). `<` / `>` / `<=` / `>=` against an `ARRAY[…]` literal are rejected at parse time. `ARRAY[?,?]` binds element-by-element; bare `:list` / `?` against an array operator binds a whole `Collection`.
- **Nested function calls:** Functions nest arbitrarily in SELECT, WHERE, GROUP BY, ORDER BY, HAVING: `AVG(NULLIF(col, 0))`, `ROUND(AVG(salary), 2)`, `LPAD(TRIM(NULLIF(col, '')), 10, '*')`
- **Window functions:** `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`, `LAG(col [, offset])`, `LEAD(col [, offset])` with `OVER (PARTITION BY ... ORDER BY ...)`. Aggregate functions (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`) also work as window functions with `OVER`. Window functions are only valid in SELECT and do not collapse rows (unlike GROUP BY). Evaluated by `WindowStage` (materializing, runs after HAVING, before ORDER BY).
- `CASE` expressions: simple (`CASE expr WHEN val THEN result END`) and searched (`CASE WHEN condition THEN result END`), fully nestable in SELECT, WHERE, ORDER BY, GROUP BY, HAVING
- **Functions:** String (LOWER, UPPER, CONCAT, SUBSTRING, TRIM, LENGTH, REPLACE, LEFT, RIGHT, LPAD, RPAD, REVERSE, POSITION), Math (ABS, ROUND, CEIL, FLOOR, MOD, POWER, SQRT, SIGN), Date (TO_DATE, NOW, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DATE_ADD, DATE_DIFF), Conversion (CAST, NULLIF, COALESCE), Aggregate (COUNT, SUM, AVG, MIN, MAX), Window (ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, plus aggregate functions with OVER)
- **Parameters:** `?` (positional) and `:name` (named) placeholders in WHERE / SELECT / GROUP BY / HAVING / function args / LIMIT / OFFSET. Bound via `BoundParameters` on `PreparedQuery.execute(...)` or `SQL4JsonEngine.query(...)`. Cannot mix `?` and `:name` in the same query (parse-time error). IN-list expansion: `IN (?)` + collection bind expands to N literals; empty collection → zero-row predicate. Null bind produces SQL-standard `col = NULL` (always false) — use literal `IS NULL` / `IS NOT NULL` for nullability tests (cannot be parameterized). `SQL4JsonEngine` bypasses result cache for parameterized queries. `LimitsSettings.maxParameters` (default 1024) caps total placeholder count per query.
