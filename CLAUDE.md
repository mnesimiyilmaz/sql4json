# CLAUDE.md - SQL4Json

## Project Overview

SQL4Json is a Java library that enables SQL-like querying of JSON data. It parses SQL SELECT statements via an ANTLR4 grammar and applies them to JSON objects — supporting filtering, aggregation, sorting, and nested queries without a database.

- **Language:** Java 8+ (source/target 1.8)
- **Build:** Maven
- **Version:** 0.0.2
- **License:** Apache 2.0
- **Package:** `io.github.mnesimiyilmaz.sql4json`

## Build & Test Commands

```bash
# Compile
mvn clean compile

# Run tests
mvn clean test

# Package (creates jar-with-dependencies)
mvn clean package

# Full build skipping tests
mvn clean package -DskipTests
```

Tests use JUnit 5 (Jupiter). The single test class `SQL4JsonQueryTests` contains ~19 tests covering SELECT, WHERE, GROUP BY, HAVING, ORDER BY, functions, and nested queries.

## Project Structure

```
src/
├── main/
│   ├── antlr4/.../generated/
│   │   └── SQL4Json.g4              # ANTLR grammar (do NOT hand-edit generated code)
│   └── java/.../sql4json/
│       ├── SQL4JsonProcessor.java   # Main entry point - orchestrates query execution
│       ├── SQL4JsonInput.java       # Input wrapper (fromObject, fromJsonString, fromJsonNodeSupplier)
│       ├── SQL4JsonListenerImpl.java # ANTLR parse tree listener
│       ├── condition/               # WHERE/HAVING condition AST
│       │   ├── CriteriaNode.java    # Interface for condition evaluation
│       │   ├── ComparisonNode.java  # Leaf node - single comparison
│       │   ├── AndNode.java         # AND logical operator
│       │   ├── OrNode.java          # OR logical operator
│       │   └── ConditionProcessor.java # Shunting Yard algorithm - infix to AST
│       ├── definitions/             # Column and aggregation definitions
│       │   ├── SelectColumnDefinition.java
│       │   ├── JsonColumnWithAggFunctionDefinion.java    # Note: "Definion" typo is intentional
│       │   ├── JsonColumnWithNonAggFunctionDefinion.java
│       │   └── OrderByColumnDefinion.java
│       ├── processor/               # SQL query pipeline
│       │   ├── SQLProcessor.java    # Chains SQLBuilders for nested queries
│       │   ├── SQLBuilder.java      # Holds parsed clauses per query level
│       │   └── SQLConstruct.java    # Applies query to flattened data
│       ├── grouping/                # GROUP BY processing
│       │   ├── GroupByProcessor.java
│       │   ├── GroupByInput.java
│       │   └── GroupRowData.java
│       ├── sorting/
│       │   └── SortProcessor.java   # ORDER BY comparator builder
│       └── utils/                   # Utilities
│           ├── JsonUtils.java       # JSON flatten/unflatten (core utility, ~250 lines)
│           ├── FieldKey.java        # Flattened field key with family tracking
│           ├── AggregateFunction.java
│           ├── AggregationUtils.java
│           ├── ComparisonOperator.java
│           ├── ComparisonUtils.java
│           ├── ValueUtils.java
│           ├── ParameterizedFunctionUtils.java
│           ├── ValueFunctionUtils.java
│           ├── AntlrSyntaxErrorListener.java
│           └── MurmurHash3.java
└── test/
    └── java/.../sql4json/
        ├── SQL4JsonQueryTests.java  # Main test suite
        └── dataclasses/             # Test POJOs (Person, Account, LoginHistory, SomeLoginData)
```

## Architecture & Data Flow

```
Input JSON
  → SQL4JsonInput (wraps as Jackson JsonNode)
  → SQL4JsonProcessor (entry point)
    → SQLProcessor (handles nested queries via ">>>" splitting)
      → SQLBuilder (ANTLR parses SQL into clause objects)
      → SQLConstruct (executes query):
          1. Flatten JSON to Map<FieldKey, Object>
          2. WHERE filter (ConditionProcessor → CriteriaNode AST)
          3. GROUP BY (GroupByProcessor with aggregation functions)
          4. HAVING filter
          5. ORDER BY (SortProcessor)
          6. SELECT projection (column selection/aliasing)
          7. Unflatten back to JsonNode
  → Returns ArrayNode result
```

## Key Dependencies

| Dependency | Version | Purpose |
|-----------|---------|---------|
| ANTLR 4 | 4.9.3 | SQL grammar parsing (pinned for Java 8 compat) |
| Jackson | 2.15.2 | JSON processing (databind, dataformat-xml, date/time modules) |
| Lombok | 1.18.30 | Boilerplate reduction (@Getter, @Setter, @RequiredArgsConstructor) |
| JUnit 5 | 5.10.0 | Testing |

## Code Conventions

- **Naming:** CamelCase classes/methods, UPPER_SNAKE_CASE constants
- **Known typo:** `Definion` (not `Definition`) in class names — this is consistent throughout and should not be "fixed"
- **Lombok:** Used extensively — `@Getter`, `@Setter`, `@RequiredArgsConstructor`, `@AllArgsConstructor`
- **Functional style:** Heavy use of Java 8 streams, `BiPredicate`, `Function`, `Supplier`, `Optional`
- **JSON flattening:** Nested JSON is flattened to `Map<FieldKey, Object>` for processing, then unflattened for output. `FieldKey` tracks the "family" (base path) for nested field grouping.
- **ANTLR generated code:** Located in `target/generated-sources/antlr4/`. Never edit generated files — modify `SQL4Json.g4` instead.

## SQL Syntax Supported

- `SELECT *`, specific columns, aliases (`AS`), aggregate functions
- `FROM $r` (root reference), nested paths (`$r.data.items`)
- `WHERE` with `=`, `!=`, `<`, `>`, `<=`, `>=`, `LIKE`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, parentheses
- `GROUP BY` with `HAVING`
- `ORDER BY` with `ASC`/`DESC`
- **Nested queries:** `>>>` operator or subquery in FROM clause
- **Functions:** `LOWER()`, `UPPER()`, `COALESCE()`, `TO_DATE()`, `NOW()`, `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`

## Development Notes

- No CI/CD pipeline configured — run `mvn clean test` locally before committing
- ANTLR plugin runs during `generate-sources` phase — grammar changes require a rebuild
- The project targets Java 8 compatibility; avoid Java 9+ APIs
- Publishing is configured for OSSRH (snapshots) and GitHub Packages via Maven profiles (`ossrh-snapshot`, `github`, `release`)
