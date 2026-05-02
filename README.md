<p align="center">
  <h1 align="center">SQL4Json</h1>
  <p align="center">
    Query JSON data using SQL — filter, aggregate, sort, and project without a database.
  </p>
  <p align="center">
    <a href="https://github.com/mnesimiyilmaz/sql4json/actions"><img src="https://img.shields.io/github/actions/workflow/status/mnesimiyilmaz/sql4json/ci.yml?branch=main&style=flat-square" alt="Build"></a>
    <a href="https://central.sonatype.com/artifact/io.github.mnesimiyilmaz/sql4json"><img src="https://img.shields.io/maven-central/v/io.github.mnesimiyilmaz/sql4json?style=flat-square" alt="Maven Central"></a>
    <a href="https://javadoc.io/doc/io.github.mnesimiyilmaz/sql4json"><img src="https://javadoc.io/badge2/io.github.mnesimiyilmaz/sql4json/javadoc.svg?style=flat-square" alt="Javadoc"></a>
    <a href="https://github.com/mnesimiyilmaz/sql4json/blob/main/LICENSE"><img src="https://img.shields.io/github/license/mnesimiyilmaz/sql4json?style=flat-square" alt="License"></a>
    <img src="https://img.shields.io/badge/Java-21%2B-blue?style=flat-square" alt="Java 21+">
    <img src="https://img.shields.io/badge/runtime_deps-ANTLR_only-brightgreen?style=flat-square" alt="Zero runtime dependencies">
    <a href="https://codecov.io/gh/mnesimiyilmaz/sql4json"><img src="https://img.shields.io/codecov/c/github/mnesimiyilmaz/sql4json?style=flat-square&label=coverage" alt="Code coverage"></a>
    <img src="https://img.shields.io/badge/branch_coverage-90%25-brightgreen?style=flat-square" alt="Branch coverage 90%">
  </p>
</p>

---

SQL4Json lets you query in-memory JSON data with familiar SQL SELECT syntax. If you know SQL, you already know how to
use it. No database, no schema, no setup. It's just ~500KB.

```sql
SELECT dept,
       COUNT(*)    AS headcount,
       AVG(salary) AS avgSalary,
       MAX(salary) AS topSalary
FROM $r
WHERE active = true
  AND hire_date BETWEEN '2023-01-01' AND '2024-12-31'
GROUP BY dept
HAVING headcount > 2
ORDER BY avgSalary DESC LIMIT 10
```

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Command-line usage](#command-line-usage)
- [Performance](#performance)
- [API Reference](#api-reference)
    - [Simple Query](#simple-query)
    - [Prepared Query](#prepared-query)
    - [Engine with Data Binding](#engine-with-data-binding)
    - [Multi-Source Queries (JOINs)](#multi-source-queries-joins)
    - [Custom JSON Codec](#custom-json-codec)
    - [Object Mapping](#object-mapping)
    - [Parameterized Queries](#parameterized-queries)
- [SQL Syntax](#sql-syntax)
    - [SELECT](#select)
    - [FROM](#from)
    - [WHERE](#where)
    - [Array Predicates](#array-predicates-since-120)
    - [GROUP BY & HAVING](#group-by--having)
    - [ORDER BY](#order-by)
    - [LIMIT & OFFSET](#limit--offset)
    - [DISTINCT](#distinct)
    - [Subqueries](#subqueries)
    - [CASE Expressions](#case-expressions)
    - [JOINs](#joins)
- [Window Functions](#window-functions)
- [Functions](#functions)
    - [String Functions](#string-functions)
    - [Math Functions](#math-functions)
    - [Date & Time Functions](#date--time-functions)
    - [Conversion Functions](#conversion-functions)
    - [Aggregate Functions](#aggregate-functions)
    - [Nested Function Calls](#nested-function-calls)
- [Type System](#type-system)
- [Pipeline Stages](#pipeline-stages)
- [NULL Handling](#null-handling)
- [Error Handling](#error-handling)
- [Security Defaults](#security-defaults)
- [Thread Safety](#thread-safety)
- [Limitations](#limitations)
- [License](#license)

## Requirements

- **Java 21** or above

## Installation

### Maven

```xml
<dependency>
    <groupId>io.github.mnesimiyilmaz</groupId>
    <artifactId>sql4json</artifactId>
    <version>1.3.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'io.github.mnesimiyilmaz:sql4json:1.3.0'
```

## Quick Start

```java
import io.github.mnesimiyilmaz.sql4json.SQL4Json;

String json = """
        [
            {"name": "Alice", "age": 30, "dept": "Engineering"},
            {"name": "Bob",   "age": 25, "dept": "Marketing"},
            {"name": "Carol", "age": 35, "dept": "Engineering"}
        ]
        """;

String result = SQL4Json.query(
        "SELECT name, age FROM $r WHERE age > 28 ORDER BY age DESC",
        json
);
// [{"name":"Carol","age":35},{"name":"Alice","age":30}]
```

## Command-line usage

A command-line entrypoint ships as a separate shaded jar published alongside the library under classifier `cli`:

```xml
<dependency>
    <groupId>io.github.mnesimiyilmaz</groupId>
    <artifactId>sql4json</artifactId>
    <version>1.3.0</version>
    <classifier>cli</classifier>
</dependency>
```

```bash
java -jar sql4json-1.3.0-cli.jar -q "SELECT name FROM \$r WHERE age > 25" -f data.json
```

### Options

| Flag                          | Description                                                      |
|-------------------------------|------------------------------------------------------------------|
| `-q`, `--query <sql\|@path>`  | SQL to run; `@path` reads the SQL from a file. Required.         |
| `-f`, `--file <path>`         | JSON input file. Omit to read from stdin.                        |
| `-o`, `--output <path>`       | Write result here. Omit for stdout.                              |
| `--data <name>=<path>`        | Repeatable. Named source for JOIN queries. Excludes `-f`.        |
| `-p`, `--param <name>=<json>` | Repeatable. Bind `:name` parameters. Value is a JSON literal.    |
| `--pretty`                    | Pretty-print the JSON output (default: compact).                 |
| `-h`, `--help`                | Print help and exit 0.                                           |
| `-v`, `--version`             | Print library version and exit 0.                                |

### Examples

```bash
# Read from a file
java -jar sql4json-1.3.0-cli.jar \
  -q "SELECT * FROM \$r WHERE age > 25" -f data.json

# Pipe from stdin and pretty-print
cat data.json | java -jar sql4json-1.3.0-cli.jar \
  -q @query.sql --pretty

# JOIN across multiple sources
java -jar sql4json-1.3.0-cli.jar \
  -q @join.sql \
  --data users=users.json \
  --data orders=orders.json

# Parameter binding (named only — positional ? is not exposed by the CLI)
java -jar sql4json-1.3.0-cli.jar \
  -q "SELECT * FROM \$r WHERE id = :id AND tags @> :allowed" \
  -p id=42 \
  -p 'allowed=["admin","editor"]' \
  -f data.json
```

### Exit codes

| Code | Meaning                                              |
|------|------------------------------------------------------|
| `0`  | Success, or `--help` / `--version` short-circuit.    |
| `1`  | Runtime failure (SQL4Json error, IO error).          |
| `2`  | Usage error (bad flags, missing required option).    |

Set `SQL4JSON_DEBUG=1` to attach the full stack trace to failure messages on stderr.

## Performance

Numbers below are median wall-clock time across 3 runs on a single reference machine (OpenJDK 21.0.1+12-LTS / Windows 11 amd64 / 32 cores / 64 GB RAM / `-Xmx 8g -Xms 1g`) against synthetic data generated with `--seed 20260428`. Re-run `./mvnw test -Plarge-tests -Dtest=ProfilingTest` to reproduce on your hardware.

> **Single-threaded.** Each query runs end-to-end on the calling thread — there is no internal parallelism. Multi-core scaling, if any, comes from running independent queries concurrently in your application.

| Query                                                          | 8 MB | 32 MB | 128 MB | 512 MB | Rows out (at 512 MB) |
|----------------------------------------------------------------|-----:|------:|-------:|-------:|---------------------:|
| `SELECT * FROM $r`                                             |   68 |   183 |    821 |  3,392 |              781,553 |
| `SELECT id, is_active, score FROM $r`                          |   55 |   217 |    986 |  3,936 |              781,553 |
| `WHERE score >= 80 AND score <= 90`                            |   47 |   183 |    752 |  2,993 |               58,520 |
| `GROUP BY country` (multi-aggregate)                           |   69 |   270 |  1,276 |  5,460 |                    9 |
| `ORDER BY score DESC LIMIT 50`                                 |   47 |   179 |    728 |  2,883 |                   50 |
| `users JOIN countries ON country=code`                         |   97 |   349 |  1,436 |  5,445 |              808,446 |
| `ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC)` |   79 |   300 |  1,377 |  6,118 |              781,553 |
| Filter + GROUP BY + HAVING + ORDER BY (kitchen sink)           |   51 |   198 |    815 |  3,304 |                    9 |

All times in milliseconds. See [`docs/performance.md`](docs/performance.md) for the full sweep across all 7 sizes and ~50 scenarios, plus the reference environment, dataset details, and the regen recipe.

## API Reference

### Simple Query

Parse and execute in a single call.

```java
// Returns JSON string
String result = SQL4Json.query("SELECT * FROM $r WHERE active = true", jsonString);

// Returns JsonValue (library's own JSON abstraction)
JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r", jsonString);

// With custom limits
Sql4jsonSettings settings = Sql4jsonSettings.builder()
        .limits(l -> l.maxRowsPerQuery(10_000))
        .build();
String result = SQL4Json.query(sql, jsonString, settings);
```

### Prepared Query

Parse once, execute many times against different data. Analogous to JDBC `PreparedStatement`.

```java
PreparedQuery query = SQL4Json.prepare("SELECT name FROM $r WHERE age > 25");

for(String json: dataList) {
    String result = query.execute(json);  // no re-parsing
}
```

### Engine with Data Binding

Bind JSON data once at build time and run multiple queries against it. Optionally cache query results with an LRU cache.

```java
// Minimal — no cache
SQL4JsonEngine engine = SQL4Json.engine()
        .data(jsonString)
        .build();

engine.query("SELECT * FROM $r WHERE active = true");
engine.query("SELECT dept, COUNT(*) AS cnt FROM $r GROUP BY dept");

// With default LRU cache (64 entries)
SQL4JsonEngine cached = SQL4Json.engine()
        .settings(Sql4jsonSettings.builder()
                .cache(c -> c.queryResultCacheEnabled(true))
                .build())
        .data(jsonString)
        .build();

cached.query(sql);  // cache miss — executes and caches result
cached.query(sql);  // cache hit — returns cached result instantly

// With custom-size cache
SQL4JsonEngine engine = SQL4Json.engine()
        .settings(Sql4jsonSettings.builder()
                .cache(c -> c.queryResultCacheEnabled(true).queryResultCacheSize(256))
                .build())
        .data(jsonString)
        .build();
```

The engine is designed to be long-lived (e.g., one per dataset). It holds no external resources and needs no cleanup.

You can also plug in your own cache implementation via the `QueryResultCache` interface:

```java
SQL4JsonEngine engine = SQL4Json.engine()
        .settings(Sql4jsonSettings.builder()
                .cache(c -> c.customCache(myCaffeineCache))
                .build())
        .data(jsonString)
        .build();
```

### Multi-Source Queries (JOINs)

For queries that JOIN across multiple JSON sources, bind named data sources:

```java
// Static API — one-off multi-source query
String result = SQL4Json.query(
        "SELECT u.name AS name, o.amount AS amount FROM users u JOIN orders o ON u.id = o.user_id",
        Map.of("users", usersJson, "orders", ordersJson));

// Engine API — bind named sources for repeated queries
SQL4JsonEngine engine = SQL4Json.engine()
        .data("users", usersJson)
        .data("orders", ordersJson)
        .build();

String result = engine.query(
        "SELECT u.name AS name, o.amount AS amount FROM users u JOIN orders o ON u.id = o.user_id");
```

Named sources and unnamed data can coexist in the same engine — unnamed for `$r` queries, named for JOIN queries.

### Custom JSON Codec

SQL4Json ships with a built-in JSON parser and serializer (zero external dependencies). If you prefer to use your own
JSON library (Jackson, Gson, etc.), implement the `JsonCodec` interface:

```java
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;

public class GsonJsonCodec implements JsonCodec {
    private final Gson gson = new Gson();

    @Override
    public JsonValue parse(String json) { /* parse with Gson, convert to JsonValue */ }

    @Override
    public String serialize(JsonValue value) { /* convert JsonValue, serialize with Gson */ }
}

// Wrap the codec in settings and pass to any API entry point
Sql4jsonSettings settings = Sql4jsonSettings.builder()
        .codec(new GsonJsonCodec())
        .build();

String result = SQL4Json.query(sql, json, settings);
PreparedQuery q = SQL4Json.prepare(sql, settings);
SQL4JsonEngine engine = SQL4Json.engine().settings(settings).data(json).build();
```

<details>
<summary>Example: Jackson-based JsonCodec</summary>

```java
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import io.github.mnesimiyilmaz.sql4json.json.*;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class JacksonJsonCodec implements JsonCodec {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public JsonValue parse(String json) {
        try {
            return convertNode(mapper.readTree(json));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON", e);
        }
    }

    @Override
    public String serialize(JsonValue value) {
        try {
            return mapper.writeValueAsString(toJsonNode(value));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize JSON", e);
        }
    }

    private JsonValue convertNode(JsonNode node) {
        return switch (node) {
            case ObjectNode obj -> {
                var map = new LinkedHashMap<String, JsonValue>();
                obj.fields().forEachRemaining(e -> map.put(e.getKey(), convertNode(e.getValue())));
                yield new JsonObjectValue(map);
            }
            case ArrayNode arr -> {
                var list = new ArrayList<JsonValue>(arr.size());
                arr.forEach(n -> list.add(convertNode(n)));
                yield new JsonArrayValue(list);
            }
            case TextNode t -> new JsonStringValue(t.asText());
            case NumericNode n -> {
                Number num = n.numberValue();
                yield (num instanceof java.math.BigDecimal bd)
                        ? new JsonDecimalValue(bd)
                        : (num instanceof Double || num instanceof Float)
                                ? new JsonDoubleValue(num.doubleValue())
                                : new JsonLongValue(num.longValue());
            }
            case BooleanNode b -> new JsonBooleanValue(b.asBoolean());
            case NullNode ignored -> JsonNullValue.INSTANCE;
            default -> JsonNullValue.INSTANCE;
        };
    }

    private JsonNode toJsonNode(JsonValue value) {
        return switch (value) {
            case JsonObjectValue(var map) -> {
                var obj = mapper.createObjectNode();
                map.forEach((k, v) -> obj.set(k, toJsonNode(v)));
                yield obj;
            }
            case JsonArrayValue(var list) -> {
                var arr = mapper.createArrayNode();
                list.forEach(v -> arr.add(toJsonNode(v)));
                yield arr;
            }
            case JsonStringValue(var s) -> new TextNode(s);
            case JsonLongValue(long l) -> mapper.getNodeFactory().numberNode(l);
            case JsonDoubleValue(double d) -> mapper.getNodeFactory().numberNode(d);
            case JsonDecimalValue(var bd) -> mapper.getNodeFactory().numberNode(bd);
            case JsonBooleanValue(var b) -> BooleanNode.valueOf(b);
            case JsonNullValue ignored -> NullNode.getInstance();
        };
    }
}
```

Usage:
```java
String result = SQL4Json.query(sql, json,
    Sql4jsonSettings.builder()
        .codec(new JacksonJsonCodec())
        .build());
```

> This is an example implementation. Adapt `ObjectMapper` configuration, error handling, and security limits to your requirements.

</details>

### Object Mapping

Map query results directly to Java records or POJOs. Zero dependencies — built-in.

```java
// Record target
record Person(String name, int age, LocalDate birthDate) {}
List<Person> people = SQL4Json.queryAsList(
    "SELECT name, age, birthDate FROM $r", json, Person.class);

// POJO target
class Employee {
    private String name; private int age;
    public void setName(String n) { this.name = n; }
    public void setAge(int a)     { this.age = a; }
    public String getName()       { return name; }
    public int getAge()           { return age; }
}
List<Employee> emps = SQL4Json.queryAsList(
    "SELECT name, age FROM $r", json, Employee.class);

// Single-row unwrap
Person alice = SQL4Json.queryAs(
    "SELECT * FROM $r WHERE name = 'Alice'", json, Person.class);

// JsonValue.as()
Person p = SQL4Json.queryAsJsonValue(sql, json).as(Person.class);
```

#### Supported target types

| Category           | Types                                                                                        |
|--------------------|----------------------------------------------------------------------------------------------|
| Primitives / boxed | `boolean, byte, short, int, long, float, double, char` + wrappers                            |
| String             | `String`, `CharSequence`                                                                     |
| Big numbers        | `BigDecimal`, `BigInteger`, `Number`                                                         |
| Time               | `LocalDate`, `LocalDateTime`, `Instant` (ISO strings; `Instant` also accepts epoch millis)   |
| Enums              | `Enum.valueOf` — case-sensitive                                                              |
| Records            | Via canonical constructor                                                                    |
| POJOs              | Public no-arg constructor + public setters (return type ignored; inherited setters included) |
| Collections        | `List<T>`, `Set<T>`, `Collection<T>`, `Map<String,V>`, `T[]`                                 |
| Passthrough        | `JsonValue` (and subtypes), `Object` (natural Java value)                                    |

#### Missing-field policy

```java
// IGNORE (default): missing fields → null / Optional.empty() / primitive default / empty collection
Person p = SQL4Json.queryAs(sql, json, Person.class);

// FAIL: any missing field → SQL4JsonMappingException with $.path.to.field
Sql4jsonSettings strict = Sql4jsonSettings.builder()
    .mapping(m -> m.missingFieldPolicy(MissingFieldPolicy.FAIL))
    .build();
List<Person> people = SQL4Json.queryAsList(sql, json, Person.class, strict);
```

#### When to use your own codec (Jackson / Gson)

Object mapping covers typical JSON → record/POJO needs. If you need full Jackson power (polymorphism, annotations, custom deserializers), use the existing codec integration path: call `SQL4Json.queryAsJsonValue(...)` to get the raw tree, serialize it back to a string via your preferred library, then deserialize with Jackson/Gson. See the "Custom JsonCodec" section above.

### Parameterized Queries

Bind values to a pre-parsed query via `PreparedQuery` (JDBC-style). Placeholders come in two flavors — cannot be mixed within a single query:

```java
// Named parameters
PreparedQuery q = SQL4Json.prepare(
    "SELECT * FROM $r WHERE age > :minAge AND dept = :dept");
String r = q.execute(json, BoundParameters.named()
    .bind("minAge", 25)
    .bind("dept", "Engineering"));

// Positional parameters
PreparedQuery q2 = SQL4Json.prepare(
    "SELECT * FROM $r WHERE age > ? AND salary BETWEEN ? AND ?");
String r2 = q2.execute(json, 25, 50_000, 150_000);

// IN expansion
PreparedQuery q3 = SQL4Json.prepare("SELECT * FROM $r WHERE id IN (?)");
String r3 = q3.execute(json, BoundParameters.of(List.of(1, 2, 3)));

// Dynamic pagination
PreparedQuery page = SQL4Json.prepare(
    "SELECT * FROM $r ORDER BY id LIMIT :n OFFSET :off");
String p = page.execute(json, BoundParameters.named().bind("n", 20).bind("off", 100));

// Combined with object mapping
List<Person> engineers = SQL4Json.prepare("SELECT * FROM $r WHERE dept = :d")
    .executeAsList(json, Person.class, BoundParameters.named().bind("d", "Engineering"));
```

Supported Java parameter types: `String`, all numeric primitives + wrappers, `BigDecimal`, `BigInteger`, `Boolean`, `LocalDate`, `LocalDateTime`, `Instant`, `ZonedDateTime`, `OffsetDateTime`, `java.util.Date`, `null`.

**IN-list semantics:**
- `IN (?)` + collection → expanded to N literals
- `IN (?)` + scalar → single-element list
- `IN (?)` + empty collection → zero-row predicate (SQL-standard behavior)

**Null semantics:** `col = :x` with a `null` bind produces `col = NULL` which is always false per SQL standard (zero rows). Use literal `IS NULL` / `IS NOT NULL` to test nullability — these cannot be parameterized.

**Scope:** parameter binding is available on `PreparedQuery` and `SQL4JsonEngine` only; there are no `SQL4Json.query(..., params)` overloads. Use `SQL4Json.prepare(sql).execute(json, params)` for one-off parameterized queries.

> **Building IDE tooling?** SQL4Json publishes a small read-only grammar catalog (`SQL4JsonGrammar.keywords()`, `functions()`, `tokenize(...)`) for syntax highlighters, completion popups, and static analysers — see [docs/grammar-api.md](docs/grammar-api.md).

## SQL Syntax

SQL4Json supports a subset of SQL SELECT. All keywords are **case-insensitive**.

### SELECT

Select all fields, specific fields, or use aliases with dot-notation for structured output:

```sql
SELECT * FROM $r

SELECT name, age FROM $r

SELECT fullname,
       id AS userId,
       username AS user.username
FROM $r
```

Dotted aliases create nested JSON in the output:

```json
[
  {
    "fullname": "Alice",
    "userId": 1,
    "user": {
      "username": "alice"
    }
  }
]
```

#### Literal and derived columns

Beyond field references, the SELECT list accepts literal values, function calls, and arbitrary
expressions — each becomes a new column in the output:

```sql
-- string / number / boolean literals
SELECT 'hello' AS greeting FROM $r;
SELECT 42 AS answer, TRUE AS flag FROM $r;

-- functions with literal arguments
SELECT ROUND(3.14159, 2) AS pi FROM $r;

-- mix fields and literals via CONCAT
SELECT CONCAT(firstName, ' ', lastName) AS fullName FROM $r;

-- nested function calls
SELECT CONCAT(UPPER(name), '!') AS shouty FROM $r;
```

### FROM

`$r` references the root JSON data. Use dot-notation to drill into nested structures:

```sql
-- Query the root array
SELECT * FROM $r

-- Query a nested array
SELECT * FROM $r.response.data.items
```

If the input is a single JSON object (not an array), it is automatically wrapped in an array.

### WHERE

Filter rows with comparison operators, logical connectives, and parenthesized groups:

```sql
SELECT *
FROM $r
WHERE age >= 18
  AND status IN ('active', 'pending')
  AND name LIKE 'A%'
  AND score BETWEEN 50 AND 100
  AND email IS NOT NULL
```

**Operators:**

| Operator                 | Example                                              |
|--------------------------|------------------------------------------------------|
| `=`, `!=`                | `status = 'active'`                                  |
| `>`, `<`, `>=`, `<=`     | `age >= 18`                                          |
| `LIKE`, `NOT LIKE`       | `name LIKE '%son'` (`%` = any chars, `_` = one char) |
| `IN`, `NOT IN`           | `dept IN ('IT', 'HR')`                               |
| `BETWEEN`, `NOT BETWEEN` | `age BETWEEN 18 AND 65`                              |
| `IS NULL`, `IS NOT NULL` | `email IS NOT NULL`                                  |
| `AND`, `OR`              | `a > 1 AND (b = 2 OR c = 3)`                         |

`AND` has higher precedence than `OR`. Use parentheses to override.

Functions can be used on the left-hand side of any comparison, including nested calls:

```sql
WHERE LOWER(name) = 'alice'
WHERE CAST(age AS STRING) = '25'
WHERE TRIM(NULLIF(name, '')) = 'Alice'
WHERE LENGTH(TRIM(name)) > 5
```

### Array Predicates (since 1.2.0)

Test values inside JSON array fields without unrolling the array via `FROM`:

```sql
-- Scalar membership (case-sensitive equality)
SELECT id FROM $r WHERE tags CONTAINS 'admin'

-- Contains-all (PostgreSQL @>): tags includes every element of the right array
SELECT id FROM $r WHERE tags @> ARRAY['admin', 'editor']

-- Contained-by (PostgreSQL <@): every element of tags is in the right array
SELECT id FROM $r WHERE tags <@ ARRAY['admin', 'editor', 'viewer']

-- Overlap (PostgreSQL &&): tags shares at least one element with the right array
SELECT id FROM $r WHERE tags && ARRAY['blocked', 'flagged']

-- Structural equality with array literal (order-sensitive, length-sensitive)
SELECT id FROM $r WHERE tags = ARRAY['admin', 'editor']
SELECT id FROM $r WHERE tags != ARRAY['admin']
```

**Parameter binding (matches JDBC / Hibernate / jOOQ patterns):**

```java
// Whole-array bind via bare parameter — list becomes the array RHS
PreparedQuery q = SQL4Json.prepare("SELECT * FROM $r WHERE tags @> :tagList");
q.execute(json, BoundParameters.named().bind("tagList", List.of("admin","editor")));

// Element-by-element via ARRAY[?,?]
PreparedQuery q2 = SQL4Json.prepare("SELECT * FROM $r WHERE tags @> ARRAY[?,?]");
q2.execute(json, BoundParameters.of("admin","editor"));
```

**Semantics:**

- `<`, `>`, `<=`, `>=` against `ARRAY[…]` raise `SQL4JsonParseException` at parse time.
- `CONTAINS`, `@>`, `<@`, `&&`, `=`, `!=` return `false` when the LHS field is missing, JSON null, or not an array.
- `NULL` element → that pairwise comparison is false (SQL-standard `col = NULL` semantics; use `IS NULL` for a null-test).
- `@>`, `<@`, `&&` are set semantics — duplicates do not affect the result. `=` / `!=` are order-sensitive structural equality.
- Empty array literal `ARRAY[]`: `@>` always true; `&&` always false; `<@` true iff LHS is empty; `=` matches only an empty array.

### GROUP BY & HAVING

Aggregate data with `GROUP BY` and filter groups with `HAVING`:

```sql
SELECT dept,
       COUNT(*)              AS headcount,
       SUM(salary)           AS totalPay,
       ROUND(AVG(salary), 2) AS avgPay,
       MAX(salary)           AS topPay,
       MIN(salary)           AS lowPay
FROM $r
GROUP BY dept
HAVING headcount > 5
   AND avgPay > 50000
```

`HAVING` conditions **must** reference aliases defined in the SELECT clause.

All aggregate functions except `COUNT(*)` operate on non-null values only.

Aggregate functions accept nested expressions — for example, `AVG(NULLIF(salary, 0))` excludes zeros from the average.

### ORDER BY

Sort by one or more fields. Default direction is `ASC`. Expressions are supported:

```sql
SELECT * FROM $r
ORDER BY age DESC, name ASC

SELECT * FROM $r
ORDER BY LENGTH(TRIM(name)) ASC
```

For grouped results, use SELECT aliases:

```sql
SELECT dept, AVG(salary) AS avgPay
FROM $r
GROUP BY dept
ORDER BY avgPay DESC
```

### LIMIT & OFFSET

Restrict result size and paginate:

```sql
-- First 10 results
SELECT *
FROM $r
ORDER BY created_at DESC LIMIT 10

-- Page 3 (20 items per page)
SELECT *
FROM $r
ORDER BY id LIMIT 20
OFFSET 40
```

### DISTINCT

Remove duplicate rows from results:

```sql
SELECT DISTINCT dept FROM $r

SELECT DISTINCT dept, status FROM $r ORDER BY dept
```

### Subqueries

Use standard SQL FROM subqueries. The inner query executes first, and its result becomes the input for the outer query.

```sql
SELECT dept, COUNT(*) AS cnt
FROM (SELECT *
      FROM $r
      WHERE active = true)
GROUP BY dept
ORDER BY cnt DESC
```

### CASE Expressions

SQL4Json supports both simple and searched CASE expressions, usable in SELECT, WHERE, ORDER BY, GROUP BY, and HAVING.

**Simple CASE** — compares an expression against values:

```sql
SELECT name,
       CASE dept
         WHEN 'eng' THEN 'Engineering'
         WHEN 'hr'  THEN 'Human Resources'
         ELSE 'Other'
       END AS department
FROM $r
```

**Searched CASE** — evaluates boolean conditions:

```sql
SELECT name,
       CASE
         WHEN salary > 100000 THEN 'senior'
         WHEN salary > 60000  THEN 'mid'
         ELSE 'junior'
       END AS level
FROM $r
```

CASE expressions nest freely with functions and other CASE expressions:

```sql
-- Function inside CASE
SELECT CASE WHEN LENGTH(name) > 4 THEN 'long' ELSE 'short' END AS label FROM $r

-- CASE inside function
SELECT UPPER(CASE WHEN active THEN 'yes' ELSE 'no' END) AS flag FROM $r

-- Nested CASE
SELECT CASE WHEN dept = 'eng'
         THEN CASE WHEN salary > 100000 THEN 'senior eng' ELSE 'eng' END
         ELSE 'other'
       END AS category
FROM $r
```

> If no WHEN clause matches and no ELSE is provided, the result is `NULL`. Simple CASE uses SQL NULL semantics: `CASE x WHEN NULL` never matches — use `CASE WHEN x IS NULL` instead.

### JOINs

Join across multiple JSON sources using `INNER JOIN`, `LEFT JOIN`, or `RIGHT JOIN` with equality-based ON conditions:

```sql
-- INNER JOIN (both spellings)
SELECT u.name AS name, o.amount AS amount
FROM users u JOIN orders o ON u.id = o.user_id

SELECT u.name AS name, o.amount AS amount
FROM users u INNER JOIN orders o ON u.id = o.user_id

-- LEFT JOIN — preserves all left-side rows, NULLs for unmatched right side
SELECT u.name AS name, o.amount AS amount
FROM users u LEFT JOIN orders o ON u.id = o.user_id

-- RIGHT JOIN — preserves all right-side rows, NULLs for unmatched left side
SELECT u.name AS name, o.amount AS amount
FROM users u RIGHT JOIN orders o ON u.id = o.user_id

-- Chained JOINs
SELECT u.name AS user_name, o.amount AS amount, p.name AS product
FROM users u
JOIN orders o ON u.id = o.user_id
LEFT JOIN products p ON o.product_id = p.id

-- Multi-column ON
SELECT * FROM a JOIN b ON a.x = b.x AND a.y = b.y

-- JOIN with all clauses
SELECT u.dept AS dept, COUNT(*) AS cnt, AVG(o.amount) AS avg_amount
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.status = 'completed'
GROUP BY u.dept
HAVING cnt >= 5
ORDER BY avg_amount DESC
LIMIT 10
```

**Table aliases** are recommended. Without aliases, table names are used as prefixes (e.g., `users.name`).

**ON conditions** support only equality (`=`) with AND:
- `ON a.col = b.col` — single column
- `ON a.x = b.x AND a.y = b.y` — multiple columns

**Output format:** Use `AS` aliases for flat output keys. Without aliases, dotted column refs like `u.name` produce the key `u.name` in the output.

**Data binding:** JOIN queries require named data sources — see [Multi-Source Queries](#multi-source-queries-joins) in the API Reference.

## Window Functions

Window functions perform calculations across a set of rows related to the current row, without collapsing rows like `GROUP BY`. Use the `OVER` clause with optional `PARTITION BY` and `ORDER BY`:

```sql
-- Ranking within departments
SELECT name, dept, salary,
    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank,
    RANK()       OVER (ORDER BY salary DESC)                  AS global_rank
FROM $r

-- Running context: previous and next salary
SELECT name, salary,
    LAG(salary)  OVER (ORDER BY salary ASC) AS prev_salary,
    LEAD(salary) OVER (ORDER BY salary ASC) AS next_salary
FROM $r

-- Aggregate windows — same value for every row in the partition
SELECT name, dept, salary,
    SUM(salary) OVER (PARTITION BY dept) AS dept_total,
    AVG(salary) OVER (PARTITION BY dept) AS dept_avg,
    COUNT(*)    OVER (PARTITION BY dept) AS dept_count
FROM $r

-- NTILE — divide into buckets
SELECT name, salary,
    NTILE(4) OVER (ORDER BY salary DESC) AS quartile
FROM $r

-- Combine with WHERE, ORDER BY, LIMIT
SELECT name, salary,
    RANK() OVER (ORDER BY salary DESC) AS rnk
FROM $r
WHERE dept = 'Engineering'
ORDER BY rnk
LIMIT 10
```

### Ranking Functions

| Function         | Description                                      |
|------------------|--------------------------------------------------|
| `ROW_NUMBER()`   | Sequential number (1, 2, 3, ...) per partition   |
| `RANK()`         | Rank with gaps on ties (1, 1, 3, ...)            |
| `DENSE_RANK()`   | Rank without gaps on ties (1, 1, 2, ...)         |
| `NTILE(n)`       | Divide rows into `n` roughly equal buckets       |

### Offset Functions

| Function               | Description                                                   |
|------------------------|---------------------------------------------------------------|
| `LAG(col [, offset])`  | Value from `offset` rows before (default 1), NULL at boundary |
| `LEAD(col [, offset])` | Value from `offset` rows after (default 1), NULL at boundary  |

### Aggregate Window Functions

Aggregate functions (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`) can be used with `OVER` to compute values across the full partition without collapsing rows. `COUNT(*)` counts all rows; `COUNT(col)` counts non-null values.

```sql
SELECT name, dept, salary,
    SUM(salary) OVER (PARTITION BY dept) AS dept_total,
    COUNT(*)    OVER ()                  AS total_employees
FROM $r
```

### OVER Clause

- `OVER ()` — entire result set is one partition
- `OVER (PARTITION BY col1, col2)` — group rows into partitions
- `OVER (ORDER BY col ASC/DESC)` — order within partition (required for ranking/offset functions)
- `OVER (PARTITION BY dept ORDER BY salary DESC)` — both

**Note:** Window functions are only valid in SELECT. They cannot appear in WHERE, HAVING, or GROUP BY.

## Functions

### String Functions

> **Note:** All string functions auto-coerce non-string inputs to text via `toString` (matching `CONCAT`). For example, `LPAD(salary, 10, '0')` where `salary` is a number returns the zero-padded numeric string. Null inputs always return `NULL`.

| Function                            | Description                                        | Example                    |
|-------------------------------------|----------------------------------------------------|----------------------------|
| `LOWER(str)`                        | Lowercase (optional locale: `LOWER(str, 'tr-TR')`) | `LOWER(name)`              |
| `UPPER(str)`                        | Uppercase (optional locale)                        | `UPPER(name, 'en-US')`     |
| `CONCAT(a, b, ...)`                 | Concatenate strings                                | `CONCAT(first, ' ', last)` |
| `SUBSTRING(str, start, len)`        | Extract substring (1-based start, length optional) | `SUBSTRING(name, 1, 3)`    |
| `TRIM(str)`                         | Remove leading/trailing whitespace                 | `TRIM(name)`               |
| `LENGTH(str)`                       | String length                                      | `LENGTH(name)`             |
| `REPLACE(str, search, replacement)` | Replace all occurrences                            | `REPLACE(name, ' ', '_')`  |
| `LEFT(str, n)`                      | First n characters                                 | `LEFT(name, 3)`            |
| `RIGHT(str, n)`                     | Last n characters                                  | `RIGHT(name, 3)`           |
| `LPAD(str, len, pad)`               | Left-pad to length                                 | `LPAD(id, 5, '0')`         |
| `RPAD(str, len, pad)`               | Right-pad to length                                | `RPAD(name, 20, '.')`      |
| `REVERSE(str)`                      | Reverse string                                     | `REVERSE(name)`            |
| `POSITION(substr, str)`             | Find position (1-based, 0 if not found)            | `POSITION('a', name)`      |

### Math Functions

| Function             | Description                          | Example           |
|----------------------|--------------------------------------|-------------------|
| `ABS(n)`             | Absolute value                       | `ABS(delta)`      |
| `ROUND(n, decimals)` | Round (decimals optional, default 0) | `ROUND(price, 2)` |
| `CEIL(n)`            | Ceiling (round up)                   | `CEIL(score)`     |
| `FLOOR(n)`           | Floor (round down)                   | `FLOOR(score)`    |
| `MOD(a, b)`          | Modulo                               | `MOD(id, 10)`     |
| `POWER(base, exp)`   | Exponentiation                       | `POWER(n, 2)`     |
| `SQRT(n)`            | Square root                          | `SQRT(area)`      |
| `SIGN(n)`            | Returns -1, 0, or 1                  | `SIGN(balance)`   |

### Date & Time Functions

| Function                    | Description                                    | Example                         |
|-----------------------------|------------------------------------------------|---------------------------------|
| `TO_DATE(str, pattern?)`    | Parse date string (ISO format if no pattern)   | `TO_DATE(d, 'yyyy-MM-dd')`      |
| `NOW()`                     | Current date and time                          | `NOW()`                         |
| `YEAR(d)`                   | Extract year                                   | `YEAR(created_at)`              |
| `MONTH(d)`                  | Extract month (1-12)                           | `MONTH(created_at)`             |
| `DAY(d)`                    | Extract day of month                           | `DAY(created_at)`               |
| `HOUR(dt)`                  | Extract hour (0 for date-only)                 | `HOUR(timestamp)`               |
| `MINUTE(dt)`                | Extract minute                                 | `MINUTE(timestamp)`             |
| `SECOND(dt)`                | Extract second                                 | `SECOND(timestamp)`             |
| `DATE_ADD(d, amount, unit)` | Add to date (units: DAYS, MONTHS, HOURS, etc.) | `DATE_ADD(d, 30, 'DAYS')`       |
| `DATE_DIFF(d1, d2, unit)`   | Difference between dates                       | `DATE_DIFF(end, start, 'DAYS')` |

`TO_DATE` without a pattern tries ISO datetime (`yyyy-MM-dd'T'HH:mm:ss`) first, then ISO date (`yyyy-MM-dd`).

### Conversion Functions

| Function              | Description                            | Example                    |
|-----------------------|----------------------------------------|----------------------------|
| `CAST(expr AS type)`  | Type conversion                        | `CAST(age AS STRING)`      |
| `NULLIF(a, b)`        | Returns NULL if `a = b`, otherwise `a` | `NULLIF(status, 'N/A')`    |
| `COALESCE(a, b, ...)` | First non-null argument                | `COALESCE(nickname, name)` |

**CAST target types:** `STRING`, `NUMBER`, `INTEGER`, `DECIMAL`, `BOOLEAN`, `DATE`, `DATETIME`

### Aggregate Functions

Used with `GROUP BY`. All operate on non-null values except `COUNT(*)`.

| Function       | Description                       |
|----------------|-----------------------------------|
| `COUNT(field)` | Count of non-null values          |
| `COUNT(*)`     | Count of all rows including nulls |
| `SUM(field)`   | Sum of numeric values             |
| `AVG(field)`   | Average of numeric values         |
| `MIN(field)`   | Minimum value                     |
| `MAX(field)`   | Maximum value                     |

### Nested Function Calls

Functions nest arbitrarily — in SELECT, WHERE, GROUP BY, ORDER BY, and HAVING:

```sql
-- Exclude zeros from average, then round
SELECT dept, ROUND(AVG(NULLIF(salary, 0)), 2) AS avg_salary
FROM $r
GROUP BY dept

-- Pad trimmed values
SELECT LPAD(TRIM(NULLIF(name, '')), 10, '*') AS padded
FROM $r

-- Nested functions in WHERE and ORDER BY
SELECT *
FROM $r
WHERE TRIM(NULLIF(name, '')) = 'Alice'
ORDER BY LENGTH(TRIM(name)) ASC
```

## Type System

SQL4Json maps JSON values to a sealed type hierarchy for type-safe processing:

| JSON Type       | SQL4Json Type                                                | Notes                                                                                                            |
|-----------------|--------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| String          | `SqlString`                                                  |                                                                                                                  |
| Number          | `SqlNumber` (sealed: `SqlLong` / `SqlDouble` / `SqlDecimal`) | Sealed since 1.2.0 — primitive long/double stored unboxed; `BigDecimal` reserved for arbitrary-precision values. |
| Boolean         | `SqlBoolean`                                                 | `true` / `false` literals                                                                                        |
| Null            | `SqlNull`                                                    | Singleton                                                                                                        |
| Date string     | `SqlDate`                                                    | Via `TO_DATE()` or `CAST(x AS DATE)`                                                                             |
| DateTime string | `SqlDateTime`                                                | Via `TO_DATE()`, `NOW()`, or `CAST(x AS DATETIME)`                                                               |

The `JsonValue` sealed interface provides the library's own JSON abstraction (object / array / string / boolean / null
plus the sealed `JsonNumberValue` family `JsonLongValue` / `JsonDoubleValue` / `JsonDecimalValue`). The public API accepts
and returns `String` or `JsonValue` with zero external dependencies. You can plug in your own JSON library via the
`JsonCodec` interface.

## Pipeline Stages

SQL4Json executes queries through a staged pipeline. Each stage transforms the row stream and is either **lazy** (processes rows one at a time, O(1) memory) or **materializing** (buffers the entire input, O(n) memory).

```
WHERE → GROUP BY → HAVING → WINDOW → ORDER BY → LIMIT → SELECT → DISTINCT
(lazy)  (mat.)    (lazy)   (mat.)    (mat.)     (mat.)  (lazy)   (mat.)
```

| Stage    | SQL Clause         | Behavior      | Row-count Enforced | Notes                                                                                            |
|----------|--------------------|---------------|--------------------|--------------------------------------------------------------------------------------------------|
| WHERE    | `WHERE`            | Lazy          | STREAMING          | Filters rows as they stream through                                                              |
| GROUP BY | `GROUP BY`         | Materializing | GROUP_BY           | Collects all rows, groups by key, applies aggregates                                             |
| HAVING   | `HAVING`           | Lazy          | —                  | Filters groups after aggregation                                                                 |
| WINDOW   | `OVER (...)`       | Materializing | WINDOW             | Partitions, sorts, computes window function values                                               |
| ORDER BY | `ORDER BY`         | Materializing | ORDER_BY           | Sorts result set; fused with LIMIT into a bounded heap (TopN optimization) when both are present |
| LIMIT    | `LIMIT` / `OFFSET` | Materializing | —                  | Truncates result set                                                                             |
| SELECT   | `SELECT`           | Lazy          | —                  | Projects columns, evaluates expressions and aliases                                              |
| DISTINCT | `DISTINCT`         | Materializing | DISTINCT           | Eliminates duplicate rows                                                                        |

Exceeding `maxRowsPerQuery` at any materializing stage throws `SQL4JsonExecutionException`. Configure via:

```java
Sql4jsonSettings.builder()
    .limits(l -> l.maxRowsPerQuery(50_000))
    .build();
```

## NULL Handling

SQL4Json follows these rules for NULL values:

- **Non-existent fields** are treated as NULL
- **Any comparison involving NULL returns `false`** — matches standard SQL semantics where NULL represents an unknown value. `NULL = NULL`, `NULL != x`, `NULL > x`, etc. all evaluate to `false`.
- Use `IS NULL` / `IS NOT NULL` to test for NULL values
- Aggregate functions (`SUM`, `AVG`, `MIN`, `MAX`, `COUNT(field)`) ignore NULL values
- `COUNT(*)` counts all rows including those with NULL fields
- `COALESCE` returns the first non-null argument

## Error Handling

SQL4Json uses a sealed exception hierarchy:

```
SQL4JsonException (sealed base)
  +-- SQL4JsonParseException      -- syntax errors (includes line & column position)
  +-- SQL4JsonExecutionException  -- runtime errors during query execution
```

```java
try{
    String result = SQL4Json.query(sql, json);
}catch(SQL4JsonParseException e){
        // e.getLine() is 1-based, e.getCharPosition() is 0-based
        System.err.println("Syntax error at line "+e.getLine()+", col "+e.getCharPosition() +": "+e.getMessage());
}catch(SQL4JsonExecutionException e){
        System.err.println("Execution error: "+e.getMessage());
}
```

## Security Defaults

SQL4Json enforces conservative limits by default to protect against denial-of-service and information-disclosure risks.
All limits are enforced by default via `SQL4Json.query(sql, json)`; pass a custom `Sql4jsonSettings` to relax or tighten them.

| Setting                         | Default         | Protects against                                                                                                                     | When to loosen                                                     |
|---------------------------------|-----------------|--------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `security.maxLikeWildcards`     | 16              | Soft ReDoS via `LIKE '%a%b%c%...'` patterns                                                                                          | Queries with many literal wildcards                                |
| `security.redactErrorDetails`   | false           | Leaking data source names, format strings, or user input into error messages (multi-tenant info disclosure)                          | Already off by default — set to `true` in multi-tenant deployments |
| `limits.maxSqlLength`           | 65 536 (64 KiB) | Unbounded SQL input consuming parser memory                                                                                          | Generated SQL longer than 64 KiB                                   |
| `limits.maxSubqueryDepth`       | 16              | Recursion bomb via deeply nested FROM subqueries                                                                                     | Programmatically generated deep nestings                           |
| `limits.maxInListSize`          | 1 024           | Parse-time amplification attacks via huge `IN (...)` lists                                                                           | Bulk filter UIs that pass large allow-lists                        |
| `limits.maxRowsPerQuery`        | 1 000 000       | Out-of-memory at materializing pipeline stages (GROUP BY, ORDER BY, WINDOW, JOIN, DISTINCT, and the final pipeline / streaming sink) | Batch queries over datasets larger than 1M rows                    |
| `cache.likePatternCacheSize`    | 1 024           | Unbounded growth of compiled-LIKE-pattern cache (cache-poisoning DoS)                                                                | High-cardinality hot-pattern workloads                             |
| `cache.queryResultCacheEnabled` | false           | Unbounded result memory on engines that don't need caching                                                                           | When you want repeated identical queries to hit a result cache     |
| `cache.queryResultCacheSize`    | 64              | Cache oversize when enabled                                                                                                          | Workloads with more than 64 hot queries                            |
| `codec.maxInputLength`          | 10 MiB          | Parser memory exhaustion via oversize JSON input                                                                                     | Known-large payloads                                               |
| `codec.maxNestingDepth`         | 64              | Stack/recursion blowup on deeply nested JSON                                                                                         | Legitimately deep JSON documents                                   |
| `codec.maxStringLength`         | 1 MiB           | Memory exhaustion via a single huge string value                                                                                     | JSON contains embedded base64 blobs, etc.                          |
| `codec.maxNumberLength`         | 64              | DoS via excessively long number literals                                                                                             | N/A — almost never needs to be loosened                            |
| `codec.maxPropertyNameLength`   | 1 024           | DoS via excessively long object keys                                                                                                 | N/A — almost never needs to be loosened                            |
| `codec.maxArrayElements`        | 1 000 000       | Memory exhaustion via a single massive array                                                                                         | Datasets with >1M elements per JSON array                          |
| `codec.duplicateKeyPolicy`      | REJECT          | HTTP-Parameter-Pollution-style desync between systems that disagree on which duplicate wins (RFC 8259 ambiguity)                     | Legacy data sources that rely on last-wins or first-wins behaviour |

To override specific limits, build a custom `Sql4jsonSettings` and pass it to any entry point:

```java
Sql4jsonSettings strict = Sql4jsonSettings.builder()
        .limits(l -> l.maxRowsPerQuery(10_000).maxSqlLength(8 * 1024))
        .codec(new DefaultJsonCodec(
                DefaultJsonCodecSettings.builder()
                        .maxInputLength(50 * 1024 * 1024)
                        .build()))
        .build();

String result = SQL4Json.query(sql, json, strict);
```

For the Javadoc on each setting, see `Sql4jsonSettings`, `SecuritySettings`, `LimitsSettings`, `CacheSettings`, and `DefaultJsonCodecSettings`.

## Thread Safety

- **`PreparedQuery`** is immutable and safe to share across threads.
- **`SQL4JsonEngine`** binds immutable data and uses a synchronized cache &mdash; safe for concurrent access from
  multiple threads.
- **Static `SQL4Json` methods** are stateless and thread-safe.
- Each query execution creates its own scoped state (rows, interner, pipeline) &mdash; no cross-query interference.

## Limitations

- **In-memory only.** The entire JSON dataset must fit in the JVM heap.
- **SELECT only.** No `INSERT`, `UPDATE`, `DELETE`, `CREATE`, or DDL operations.
- **No FULL OUTER JOIN or CROSS JOIN.** Only INNER, LEFT, and RIGHT JOINs with equality ON conditions.
- **No UNION / INTERSECT / EXCEPT.** One query, one result set.
- **No window frame clause.** Window functions operate over the full partition. `ROWS BETWEEN N PRECEDING AND M FOLLOWING` is not supported.

## License

[Apache License 2.0](LICENSE)
