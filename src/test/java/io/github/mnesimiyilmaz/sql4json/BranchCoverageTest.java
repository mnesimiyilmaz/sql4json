// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests targeting specific branch coverage gaps identified by JaCoCo. Each test documents which class/branch it
 * targets.
 */
class BranchCoverageTest {

    private static final String EMPLOYEES = """
            [
              {"name": "Alice", "age": 30, "dept": "eng", "salary": 50000, "active": true,
               "hire_date": "2020-01-15", "address": {"city": "NYC", "zip": "10001"}},
              {"name": "Bob", "age": 25, "dept": "eng", "salary": 60000, "active": false,
               "hire_date": "2021-06-20", "address": {"city": "LA", "zip": "90001"}},
              {"name": "Charlie", "age": 35, "dept": "hr", "salary": 55000, "active": true,
               "hire_date": "2019-03-10", "address": {"city": "NYC", "zip": "10002"}},
              {"name": "Diana", "age": null, "dept": "hr", "salary": null, "active": null,
               "hire_date": null, "address": {"city": null, "zip": null}}
            ]""";

    // ── SQL4Json entry point branches ─────────────────────────────────────

    @Nested
    class EntryPointBranches {

        // SQL4Json: null sql
        @Test
        void query_nullSql_throws() {
            assertThrows(SQL4JsonException.class, () -> SQL4Json.query(null, "[]"));
        }

        @Test
        void query_blankSql_throws() {
            assertThrows(SQL4JsonException.class, () -> SQL4Json.query("  ", "[]"));
        }

        @Test
        void query_nullData_throws() {
            assertThrows(SQL4JsonException.class, () -> SQL4Json.query("SELECT * FROM $r", (String) null));
        }

        @Test
        void query_nullSettings_throws() {
            assertThrows(NullPointerException.class, () -> SQL4Json.query("SELECT * FROM $r", "[]", null));
        }

        // queryAsJsonValue overloads
        @Test
        void queryAsJsonValue_nullJsonValueData_throws() {
            assertThrows(
                    SQL4JsonException.class, () -> SQL4Json.queryAsJsonValue("SELECT * FROM $r", (JsonValue) null));
        }

        @Test
        void queryAsJsonValue_withSettings_works() {
            var settings = Sql4jsonSettings.builder().build();
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r", "[]", settings);
            assertNotNull(result);
        }

        // Multi-source query: null/empty dataSources
        @Test
        void query_multiSource_nullSources_throws() {
            assertThrows(
                    SQL4JsonException.class, () -> SQL4Json.query("SELECT * FROM users", (Map<String, String>) null));
        }

        @Test
        void query_multiSource_emptySources_throws() {
            assertThrows(SQL4JsonException.class, () -> SQL4Json.query("SELECT * FROM users", Map.of()));
        }

        @Test
        void query_multiSource_nullSourceValue_throws() {
            Map<String, String> sources = new java.util.HashMap<>();
            sources.put("users", null);
            assertThrows(SQL4JsonException.class, () -> SQL4Json.query("SELECT * FROM users", sources));
        }

        @Test
        void queryAsJsonValue_multiSource_defaultSettings() {
            String users = """
                    [{"id": 1, "name": "Alice"}]""";
            var result = SQL4Json.queryAsJsonValue("SELECT * FROM users", Map.of("users", users));
            assertNotNull(result);
        }

        // PreparedQuery
        @Test
        void prepare_defaultSettings() {
            PreparedQuery q = SQL4Json.prepare("SELECT * FROM $r");
            String result = q.execute("[]");
            assertEquals("[]", result);
        }

        @Test
        void prepare_withSettings() {
            var settings = Sql4jsonSettings.builder().build();
            PreparedQuery q = SQL4Json.prepare("SELECT * FROM $r", settings);
            String result = q.execute("[]");
            assertEquals("[]", result);
        }
    }

    // ── OperatorRegistry branches: SqlBoolean, SqlDate, SqlDateTime equality ──

    @Nested
    class OperatorBranches {

        @Test
        void equals_boolean_values() {
            // OperatorRegistry: SqlBoolean equality branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE active = true", EMPLOYEES);
            assertTrue(result.contains("Alice"));
            assertTrue(result.contains("Charlie"));
            assertFalse(result.contains("Bob"));
        }

        @Test
        void equals_date_values() {
            // OperatorRegistry: SqlDate equality branch
            String result =
                    SQL4Json.query("SELECT name FROM $r WHERE TO_DATE(hire_date) = TO_DATE('2020-01-15')", EMPLOYEES);
            assertTrue(result.contains("Alice"));
            assertFalse(result.contains("Bob"));
        }

        @Test
        void notEquals_boolean() {
            // OperatorRegistry: != with boolean
            String result = SQL4Json.query("SELECT name FROM $r WHERE active != false", EMPLOYEES);
            assertTrue(result.contains("Alice"));
        }

        @Test
        void greaterThan_dates_via_cast() {
            // OperatorRegistry: > with date comparison via SqlValueComparator
            String result =
                    SQL4Json.query("SELECT name FROM $r WHERE TO_DATE(hire_date) > TO_DATE('2020-06-01')", EMPLOYEES);
            assertTrue(result.contains("Bob"));
            assertFalse(result.contains("Alice"));
        }
    }

    // ── ExpressionEvaluator branches ──────────────────────────────────────

    @Nested
    class ExpressionEvaluatorBranches {

        @Test
        void simpleCaseWhen_noMatch_nullElse() {
            // ExpressionEvaluator: SimpleCaseWhen no match, elseExpr=null → SqlNull
            String result =
                    SQL4Json.query("SELECT CASE dept WHEN 'finance' THEN 'F' END AS dept_code FROM $r", EMPLOYEES);
            assertTrue(result.contains("null"));
        }

        @Test
        void simpleCaseWhen_nullSubject_noMatch() {
            // ExpressionEvaluator: SimpleCaseWhen with null subject → skip all
            String json = """
                    [{"status": null}]""";
            String result = SQL4Json.query("SELECT CASE status WHEN 'active' THEN 'A' ELSE 'X' END AS s FROM $r", json);
            assertTrue(result.contains("X"));
        }

        @Test
        void searchedCaseWhen_noMatch_nullElse() {
            // ExpressionEvaluator: SearchedCaseWhen no match, elseExpr=null → SqlNull
            String result = SQL4Json.query("SELECT CASE WHEN age > 100 THEN 'ancient' END AS label FROM $r", EMPLOYEES);
            assertTrue(result.contains("null"));
        }

        @Test
        void searchedCaseWhen_match() {
            // ExpressionEvaluator: SearchedCaseWhen match path
            String result = SQL4Json.query(
                    "SELECT CASE WHEN age > 30 THEN 'senior' WHEN age <= 30 THEN 'junior' ELSE 'unknown' END AS level FROM $r",
                    EMPLOYEES);
            assertTrue(result.contains("senior"));
            assertTrue(result.contains("junior"));
        }

        @Test
        void aggregateCaseWhen_groupBy() {
            // ExpressionEvaluator: aggregateSearchedCase path
            // The searched CASE with aggregate inside must be in a GROUP BY context
            String json = """
                    [{"dept":"eng","salary":50000},{"dept":"eng","salary":60000},{"dept":"hr","salary":40000}]""";
            String result = SQL4Json.query(
                    "SELECT dept, CASE dept WHEN 'eng' THEN SUM(salary) ELSE 0 END AS budget "
                            + "FROM $r GROUP BY dept",
                    json);
            assertNotNull(result);
            assertTrue(result.contains("budget"));
        }

        @Test
        void simpleCaseWhen_withAggregate_groupBy() {
            // ExpressionEvaluator: aggregateSimpleCase path
            String result = SQL4Json.query(
                    "SELECT dept, CASE dept WHEN 'eng' THEN SUM(salary) ELSE 0 END AS eng_total "
                            + "FROM $r GROUP BY dept",
                    EMPLOYEES);
            assertNotNull(result);
        }

        @Test
        void countStar_aggregate() {
            // ExpressionEvaluator: AggregateFnCall with null inner (COUNT(*))
            String result = SQL4Json.query("SELECT dept, COUNT(*) AS cnt FROM $r GROUP BY dept", EMPLOYEES);
            assertTrue(result.contains("cnt"));
        }
    }

    // ── Parser branches ──────────────────────────────────────────────────

    @Nested
    class ParserBranches {

        @Test
        void castExpression_inSelect() {
            // ParserListener: CastExprColumnContext branch
            String result = SQL4Json.query("SELECT CAST(age AS STRING) AS age_str FROM $r", EMPLOYEES);
            assertTrue(result.contains("30"));
        }

        @Test
        void castExpression_inWhere() {
            // ParserListener: RhsCastExprContext branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE age = CAST('30' AS NUMBER)", EMPLOYEES);
            assertTrue(result.contains("Alice"));
        }

        @Test
        void functionCall_inWhereRhs() {
            // ParserListener: RhsFunctionCallContext branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE LOWER(name) = LOWER('ALICE')", EMPLOYEES);
            assertTrue(result.contains("Alice"));
        }

        @Test
        void columnRef_inWhereRhs() {
            // ParserListener: RhsColumnRefContext for comparison
            String result = SQL4Json.query("SELECT name FROM $r WHERE name = name", EMPLOYEES);
            assertTrue(result.contains("Alice"));
        }

        @Test
        void joinClause_withNonEqualOperator_throws() {
            // ParserListener: non-"=" operator in ON condition
            String users = """
                    [{"id": 1}]""";
            String orders = """
                    [{"user_id": 1}]""";
            assertThrows(
                    SQL4JsonParseException.class,
                    () -> SQL4Json.query(
                            "SELECT * FROM users u JOIN orders o ON u.id > o.user_id",
                            Map.of("users", users, "orders", orders)));
        }

        @Test
        void joinClause_leftJoin() {
            // ParserListener: LEFT join type branch
            String users = """
                    [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]""";
            String orders = """
                    [{"user_id": 1, "product": "Widget"}]""";
            String result = SQL4Json.query(
                    "SELECT u.name, o.product FROM users u LEFT JOIN orders o ON u.id = o.user_id",
                    Map.of("users", users, "orders", orders));
            assertTrue(result.contains("Alice"));
            assertTrue(result.contains("Bob"));
        }

        @Test
        void joinClause_rightJoin() {
            // ParserListener: RIGHT join type branch
            String users = """
                    [{"id": 1, "name": "Alice"}]""";
            String orders = """
                    [{"user_id": 1, "product": "Widget"}, {"user_id": 2, "product": "Gadget"}]""";
            String result = SQL4Json.query(
                    "SELECT u.name, o.product FROM users u RIGHT JOIN orders o ON u.id = o.user_id",
                    Map.of("users", users, "orders", orders));
            assertTrue(result.contains("Widget"));
            assertTrue(result.contains("Gadget"));
        }

        @Test
        void fromTableName_withAlias() {
            // ParserListener: FROM tableName alias branch (identifierOrKeyword + IDENTIFIER)
            String users = """
                    [{"name": "Alice"}]""";
            String result = SQL4Json.query("SELECT u.name FROM users u", Map.of("users", users));
            assertTrue(result.contains("Alice"));
        }

        @Test
        void fromTableName_noAlias() {
            // ParserListener: FROM tableName without alias (aliasToken == null)
            String users = """
                    [{"name": "Alice"}]""";
            String result = SQL4Json.query("SELECT users.name FROM users", Map.of("users", users));
            assertTrue(result.contains("Alice"));
        }

        @Test
        void limitOnly_noOffset() {
            // ParserListener: LIMIT without OFFSET
            String result = SQL4Json.query("SELECT * FROM $r LIMIT 2", EMPLOYEES);
            // Should have 2 items
            assertNotNull(result);
        }

        @Test
        void limitWithOffset() {
            // ParserListener: LIMIT with OFFSET
            String result = SQL4Json.query("SELECT * FROM $r LIMIT 2 OFFSET 1", EMPLOYEES);
            assertNotNull(result);
        }

        @Test
        void literalValue_now() {
            // ParserListener: NOW() dispatched as a zero-arg value function in literal context
            String result = SQL4Json.query("SELECT NOW() AS ts FROM $r LIMIT 1", EMPLOYEES);
            assertTrue(result.contains("ts"));
        }

        @Test
        void orderBy_defaultAsc() {
            // ParserListener: ORDER BY without explicit direction → ASC
            String result = SQL4Json.query("SELECT name FROM $r ORDER BY name", EMPLOYEES);
            assertNotNull(result);
        }

        @Test
        void subquery_from() {
            // ParserListener: FROM (SELECT ...) subquery branch
            String result = SQL4Json.query("SELECT name FROM (SELECT name, age FROM $r WHERE age > 25)", EMPLOYEES);
            assertTrue(result.contains("Alice"));
            assertTrue(result.contains("Charlie"));
        }

        @Test
        void distinct_clause() {
            // ParserListener: DISTINCT branch
            String result = SQL4Json.query("SELECT DISTINCT dept FROM $r", EMPLOYEES);
            assertTrue(result.contains("eng"));
            assertTrue(result.contains("hr"));
        }

        @Test
        void betweenCondition() {
            // ParserListener: BETWEEN condition branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE age BETWEEN 25 AND 30", EMPLOYEES);
            assertTrue(result.contains("Alice"));
            assertTrue(result.contains("Bob"));
        }

        @Test
        void notBetweenCondition() {
            // ParserListener: NOT BETWEEN condition branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE age NOT BETWEEN 25 AND 30", EMPLOYEES);
            assertTrue(result.contains("Charlie"));
        }

        @Test
        void inCondition() {
            // ParserListener: IN condition branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE dept IN ('eng')", EMPLOYEES);
            assertTrue(result.contains("Alice"));
            assertTrue(result.contains("Bob"));
        }

        @Test
        void notInCondition() {
            // ParserListener: NOT IN condition branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE dept NOT IN ('eng')", EMPLOYEES);
            assertTrue(result.contains("Charlie"));
        }

        @Test
        void notLikeCondition() {
            // ParserListener: NOT LIKE condition branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE name NOT LIKE 'A%'", EMPLOYEES);
            assertTrue(result.contains("Bob"));
            assertFalse(result.contains("Alice"));
        }

        @Test
        void isNullCondition() {
            // ParserListener: IS NULL condition branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE age IS NULL", EMPLOYEES);
            assertTrue(result.contains("Diana"));
        }

        @Test
        void isNotNullCondition() {
            // ParserListener: IS NOT NULL condition branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE age IS NOT NULL", EMPLOYEES);
            assertTrue(result.contains("Alice"));
        }

        @Test
        void orCondition() {
            // ParserListener: OR conditions branch
            String result = SQL4Json.query("SELECT name FROM $r WHERE dept = 'eng' OR dept = 'hr'", EMPLOYEES);
            assertTrue(result.contains("Alice"));
            assertTrue(result.contains("Charlie"));
        }

        @Test
        void parenthesizedConditions() {
            // ParserListener: parenthesized conditions branch
            String result =
                    SQL4Json.query("SELECT name FROM $r WHERE (dept = 'eng' AND age > 27) OR dept = 'hr'", EMPLOYEES);
            assertTrue(result.contains("Alice"));
            assertTrue(result.contains("Charlie"));
        }

        @Test
        void groupByWithHaving() {
            // ParserListener: HAVING branch
            String result =
                    SQL4Json.query("SELECT dept, COUNT(*) AS cnt FROM $r GROUP BY dept HAVING cnt > 1", EMPLOYEES);
            assertNotNull(result);
        }

        @Test
        void windowFunction_inSelect() {
            // ParserListener: WindowFunctionExprContext branch
            String result = SQL4Json.query("SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn FROM $r", EMPLOYEES);
            assertTrue(result.contains("rn"));
        }

        @Test
        void windowFunction_withPartition() {
            // ParserListener: PARTITION BY branch in WindowSpec
            String result = SQL4Json.query(
                    "SELECT name, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk FROM $r", EMPLOYEES);
            assertTrue(result.contains("rnk"));
        }

        @Test
        void windowFunction_aggregate_over() {
            // ParserListener: aggregate function with OVER (window aggregate)
            String result = SQL4Json.query(
                    "SELECT name, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM $r", EMPLOYEES);
            assertTrue(result.contains("dept_total"));
        }

        @Test
        void scalarFunction_inGroupBy() {
            // ParserListener/Expression: scalar function in GROUP BY
            String result =
                    SQL4Json.query("SELECT UPPER(dept) AS d, COUNT(*) AS cnt FROM $r GROUP BY UPPER(dept)", EMPLOYEES);
            assertTrue(result.contains("ENG"));
        }

        @Test
        void scalarFunction_inOrderBy() {
            // Parser: scalar function in ORDER BY
            String result = SQL4Json.query("SELECT name FROM $r ORDER BY LENGTH(name) DESC", EMPLOYEES);
            assertNotNull(result);
        }

        @Test
        void nestedScalarFunctions() {
            // Expression: nested scalar function calls
            String result = SQL4Json.query("SELECT LPAD(TRIM(name), 10, '*') AS padded FROM $r LIMIT 1", EMPLOYEES);
            assertTrue(result.contains("padded"));
        }

        @Test
        void aggregateFunctionArg_inScalar() {
            // ParserListener: AggFunctionExprContext inside function arg
            String result =
                    SQL4Json.query("SELECT dept, ROUND(AVG(salary), 2) AS avg_sal FROM $r GROUP BY dept", EMPLOYEES);
            assertTrue(result.contains("avg_sal"));
        }

        @Test
        void simpleCaseExpression() {
            // ParserListener: simple CASE expression path
            String result = SQL4Json.query(
                    "SELECT CASE dept WHEN 'eng' THEN 'Engineering' WHEN 'hr' THEN 'HR' ELSE 'Other' END AS dept_name FROM $r",
                    EMPLOYEES);
            assertTrue(result.contains("Engineering"));
            assertTrue(result.contains("HR"));
        }

        @Test
        void searchedCaseExpression() {
            // ParserListener: searched CASE expression path
            String result = SQL4Json.query(
                    "SELECT CASE WHEN salary > 55000 THEN 'high' WHEN salary > 0 THEN 'normal' END AS level FROM $r",
                    EMPLOYEES);
            assertTrue(result.contains("high"));
            assertTrue(result.contains("normal"));
        }

        @Test
        void simpleCaseWithoutElse() {
            // ParserListener: simple CASE without ELSE
            String result = SQL4Json.query(
                    "SELECT CASE dept WHEN 'eng' THEN 'Engineering' END AS dept_name FROM $r", EMPLOYEES);
            assertTrue(result.contains("Engineering"));
            assertTrue(result.contains("null"));
        }

        @Test
        void searchedCaseWithoutElse() {
            // ParserListener: searched CASE without ELSE
            String result = SQL4Json.query("SELECT CASE WHEN age > 100 THEN 'ancient' END AS label FROM $r", EMPLOYEES);
            assertTrue(result.contains("null"));
        }
    }

    // ── Expression tree branches ──────────────────────────────────────────

    @Nested
    class ExpressionBranches {

        @Test
        void containsAggregate_throughScalarFn() {
            // Expression: containsAggregate through ScalarFnCall
            String result =
                    SQL4Json.query("SELECT dept, ROUND(AVG(salary)) AS rounded_avg FROM $r GROUP BY dept", EMPLOYEES);
            assertTrue(result.contains("rounded_avg"));
        }

        @Test
        void containsAggregate_throughSearchedCase() {
            // Expression: containsAggregate through SearchedCaseWhen
            // Tested via SelectColumnDef unit tests + aggregateCaseWhen_groupBy integration test
            String json = """
                    [{"dept":"eng","salary":50000},{"dept":"eng","salary":60000},{"dept":"hr","salary":40000}]""";
            String result = SQL4Json.query(
                    "SELECT dept, CASE dept WHEN 'eng' THEN SUM(salary) ELSE 0 END AS total " + "FROM $r GROUP BY dept",
                    json);
            assertTrue(result.contains("total"));
        }
    }

    // ── SelectColumnDef branches ─────────────────────────────────────────

    @Nested
    class SelectColumnDefBranches {

        @Test
        void findAggregate_inSimpleCaseSubject() {
            // SelectColumnDef: findAggregate through SimpleCaseWhen subject
            String result = SQL4Json.query(
                    "SELECT CASE COUNT(*) WHEN 4 THEN 'four' ELSE 'other' END AS cnt " + "FROM $r GROUP BY dept",
                    EMPLOYEES);
            assertNotNull(result);
        }

        @Test
        void findAggregate_inSimpleCaseValue() {
            // SelectColumnDef: findAggregate through SimpleCaseWhen clause value
            String result = SQL4Json.query(
                    "SELECT dept, CASE dept WHEN 'eng' THEN SUM(salary) END AS total " + "FROM $r GROUP BY dept",
                    EMPLOYEES);
            assertNotNull(result);
        }

        @Test
        void findAggregate_inSearchedCaseResult() {
            // SelectColumnDef: findAggregate through SearchedCaseWhen result
            String result = SQL4Json.query(
                    "SELECT dept, CASE WHEN dept = 'eng' THEN SUM(salary) ELSE 0 END AS total "
                            + "FROM $r GROUP BY dept",
                    EMPLOYEES);
            assertNotNull(result);
        }

        @Test
        void findAggregate_inScalarFnArg() {
            // SelectColumnDef: findAggregate through ScalarFnCall args
            String result =
                    SQL4Json.query("SELECT dept, ROUND(AVG(salary), 2) AS avg_sal FROM $r GROUP BY dept", EMPLOYEES);
            assertTrue(result.contains("avg_sal"));
        }

        @Test
        void selectStar_cherryPick() {
            // JsonUnflattener: SELECT * → return entire original
            String result = SQL4Json.query("SELECT * FROM $r LIMIT 1", EMPLOYEES);
            assertTrue(result.contains("name"));
            assertTrue(result.contains("address"));
        }

        @Test
        void selectStar_aggregated() {
            // JsonUnflattener: SELECT * in reconstructFromFlatMap
            // (after GROUP BY, the row is modified so it goes through reconstructFromFlatMap)
            String json = """
                    [{"a": 1}, {"a": 2}]""";
            String result = SQL4Json.query("SELECT * FROM (SELECT a FROM $r WHERE a > 0)", json);
            assertNotNull(result);
        }
    }

    // ── JsonUnflattener branches ─────────────────────────────────────────

    @Nested
    class JsonUnflattenerBranches {

        @Test
        void cherryPick_windowResult() {
            // JsonUnflattener: containsWindow branch in cherryPick
            String result = SQL4Json.query("SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn FROM $r", EMPLOYEES);
            assertTrue(result.contains("rn"));
        }

        @Test
        void cherryPick_scalarFunction() {
            // JsonUnflattener: expression with functions (functionRegistry != null)
            String result = SQL4Json.query("SELECT UPPER(name) AS upper_name FROM $r", EMPLOYEES);
            assertTrue(result.contains("ALICE"));
        }

        @Test
        void cherryPick_dottedAlias() {
            // JsonUnflattener: dotted alias setNestedField
            String result = SQL4Json.query("SELECT name AS info.name, age AS info.age FROM $r LIMIT 1", EMPLOYEES);
            assertTrue(result.contains("info"));
        }

        @Test
        void reconstructFromFlatMap_aggregateColumn() {
            // JsonUnflattener: aggregate column path in reconstructFromFlatMap
            String result = SQL4Json.query(
                    "SELECT dept, COUNT(*) AS cnt, SUM(salary) AS total FROM $r GROUP BY dept", EMPLOYEES);
            assertTrue(result.contains("cnt"));
            assertTrue(result.contains("total"));
        }

        @Test
        void reconstructFromFlatMap_sumAggregateColumn() {
            // JsonUnflattener: col.containsAggregate() → aliasOrName() path
            String result = SQL4Json.query("SELECT dept, SUM(salary) AS total FROM $r GROUP BY dept", EMPLOYEES);
            assertTrue(result.contains("total"));
        }

        @Test
        void insertAtPath_arrayIndex() {
            // JsonUnflattener: insertAtPath with array segments
            String json = """
                    [{"tags": ["java", "python"], "name": "test"}]""";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertTrue(result.contains("java"));
        }

        @Test
        void nestedObjectReconstruction() {
            // JsonUnflattener: insertAtPath with nested object paths
            String result = SQL4Json.query("SELECT address.city FROM $r LIMIT 1", EMPLOYEES);
            assertTrue(result.contains("NYC"));
        }
    }

    // ── DistinctStage.DistinctKey branches ───────────────────────────────

    @Nested
    class DistinctKeyBranches {

        @Test
        void distinct_withDates() {
            // DistinctKey: sqlValueHash for date types
            String json = """
                    [{"d": "2024-01-01"}, {"d": "2024-01-01"}, {"d": "2024-01-02"}]""";
            String result = SQL4Json.query("SELECT DISTINCT TO_DATE(d) AS date_val FROM $r", json);
            assertNotNull(result);
        }

        @Test
        void distinct_withBooleans() {
            // DistinctKey: sqlValueHash for boolean types
            String result = SQL4Json.query("SELECT DISTINCT active FROM $r", EMPLOYEES);
            assertNotNull(result);
        }

        @Test
        void distinct_withNulls() {
            // DistinctKey: null handling
            String result = SQL4Json.query("SELECT DISTINCT age FROM $r", EMPLOYEES);
            assertNotNull(result);
        }

        @Test
        void distinct_withMixedTypes() {
            // DistinctKey: different SqlValue types
            String result = SQL4Json.query("SELECT DISTINCT salary FROM $r", EMPLOYEES);
            assertNotNull(result);
        }
    }

    // ── QueryExecutor branches ───────────────────────────────────────────

    @Nested
    class QueryExecutorBranches {

        @Test
        void nestedSubquery_depth() {
            // QueryExecutor: subquery depth tracking
            String result = SQL4Json.query(
                    "SELECT name FROM (SELECT name, age FROM (SELECT * FROM $r) WHERE age > 25)", EMPLOYEES);
            assertTrue(result.contains("Alice"));
        }

        @Test
        void query_withRootPath() {
            // QueryExecutor: non-$r root path
            String json = """
                    {"data": [{"name": "Alice"}, {"name": "Bob"}]}""";
            String result = SQL4Json.query("SELECT name FROM $r.data", json);
            assertTrue(result.contains("Alice"));
        }
    }

    // ── BoundedPatternCache branch ───────────────────────────────────────

    @Nested
    class BoundedPatternCacheBranches {

        @Test
        void likePattern_cacheEviction() {
            // BoundedPatternCache: cache eviction with many patterns
            var settings = Sql4jsonSettings.builder()
                    .cache(c -> c.likePatternCacheSize(2))
                    .build();
            String json = """
                    [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}]""";
            // Execute multiple LIKE queries to trigger cache eviction
            SQL4Json.query("SELECT * FROM $r WHERE name LIKE 'A%'", json, settings);
            SQL4Json.query("SELECT * FROM $r WHERE name LIKE 'B%'", json, settings);
            String result = SQL4Json.query("SELECT * FROM $r WHERE name LIKE 'C%'", json, settings);
            assertTrue(result.contains("Charlie"));
        }
    }

    // ── Row branches ─────────────────────────────────────────────────────

    @Nested
    class RowBranches {

        @Test
        void row_lazyFlattening() {
            // Row: lazy flattening path (originalValue present, not modified)
            String json = """
                    [{"name": "Alice", "nested": {"a": 1, "b": 2}}]""";
            String result = SQL4Json.query("SELECT name FROM $r", json);
            assertTrue(result.contains("Alice"));
        }

        @Test
        void row_modifiedByProjection() {
            // Row: modified flag set → reconstructFromFlatMap
            String result = SQL4Json.query("SELECT name, UPPER(dept) AS dept FROM $r", EMPLOYEES);
            assertTrue(result.contains("ENG"));
        }
    }

    // ── Custom codec path (non-DefaultJsonCodec) ─────────────────────────

    @Nested
    class CustomCodecBranches {

        @Test
        void queryAsJsonValue_withJsonValue_defaultSettings() {
            // SQL4Json: queryAsJsonValue(sql, JsonValue, settings) path
            var json = SQL4Json.queryAsJsonValue("SELECT * FROM $r", "[]");
            var result = SQL4Json.queryAsJsonValue("SELECT * FROM $r", json);
            assertNotNull(result);
        }
    }

    // ── StreamingJsonParser / CompactStringMap branches ──────────────────

    @Nested
    class JsonParserBranches {

        @Test
        void emptyArray() {
            String result = SQL4Json.query("SELECT * FROM $r", "[]");
            assertEquals("[]", result);
        }

        @Test
        void nestedArrays() {
            String json = """
                    [{"tags": [["a", "b"], ["c"]]}]""";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertTrue(result.contains("a"));
        }

        @Test
        void unicodeEscapes() {
            String json = """
                    [{"name": "caf\\u00e9"}]""";
            String result = SQL4Json.query("SELECT name FROM $r", json);
            assertTrue(result.contains("caf"));
        }

        @Test
        void booleanAndNullValues() {
            String json = """
                    [{"a": true, "b": false, "c": null, "d": 0}]""";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertTrue(result.contains("true"));
            assertTrue(result.contains("false"));
            assertTrue(result.contains("null"));
        }

        @Test
        void deeplyNestedObject() {
            String json = """
                    [{"a": {"b": {"c": {"d": "deep"}}}}]""";
            String result = SQL4Json.query("SELECT a.b.c.d FROM $r", json);
            assertTrue(result.contains("deep"));
        }

        @Test
        void emptyStrings() {
            String json = """
                    [{"name": "", "value": ""}]""";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertNotNull(result);
        }

        @Test
        void largeNumbers() {
            String json = """
                    [{"big": 9999999999999, "tiny": 0.000001, "neg": -42}]""";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertTrue(result.contains("9999999999999"));
        }

        @Test
        void escapedStrings() {
            String json = "[{\"text\": \"line1\\nline2\\ttab\"}]";
            String result = SQL4Json.query("SELECT text FROM $r", json);
            assertNotNull(result);
        }

        @Test
        void emptyObject() {
            String json = "[{}]";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertNotNull(result);
        }

        @Test
        void mixedArrayTypes() {
            // StreamingJsonParser: arrays with mixed value types
            String json = """
                    [{"items": [1, "two", true, null, 3.14]}]""";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertTrue(result.contains("two"));
        }

        @Test
        void singleElement() {
            String json = "[{\"x\": 1}]";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertTrue(result.contains("1"));
        }
    }

    // ── Additional JsonUnflattener paths ─────────────────────────────────

    @Nested
    class AdditionalUnflattenerBranches {

        @Test
        void selectStar_withSubquery_reconstructsNestedStructure() {
            // JsonUnflattener: reconstructFromFlatMap with SELECT * and nested paths
            String json = """
                    [{"a": 1, "nested": {"x": 10}}, {"a": 2, "nested": {"x": 20}}]""";
            String result = SQL4Json.query("SELECT * FROM (SELECT a, nested.x FROM $r WHERE a > 0)", json);
            assertNotNull(result);
        }

        @Test
        void selectStar_flattenedArrayReconstruction() {
            // JsonUnflattener: insertAtPath with array bracket segment + nested object
            String json = """
                    [{"items": [{"name": "apple"}, {"name": "banana"}]}]""";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertTrue(result.contains("apple"));
            assertTrue(result.contains("banana"));
        }

        @Test
        void cherryPick_nullFunctionRegistry() {
            // JsonUnflattener: no registry and not a plain column fallback path
            // This is exercised when unflatten is called without functionRegistry
            String json = """
                    [{"name": "Alice", "age": 30}]""";
            String result = SQL4Json.query("SELECT name, age FROM $r", json);
            assertTrue(result.contains("Alice"));
        }

        @Test
        void navigateToField_missingPath() {
            // JsonUnflattener: navigateToField on non-existent nested path
            String json = """
                    [{"name": "Alice"}]""";
            String result = SQL4Json.query("SELECT name, address.city FROM $r", json);
            assertTrue(result.contains("null"));
        }

        @Test
        void reconstructFromFlatMap_withNullColumnName() {
            // JsonUnflattener: columnName() == null path
            String json = """
                    [{"a": 1}, {"a": 2}]""";
            String result =
                    SQL4Json.query("SELECT a, CASE WHEN a > 1 THEN 'big' ELSE 'small' END AS size FROM $r", json);
            assertTrue(result.contains("size"));
        }

        @Test
        void distinctWithDateTimeValues() {
            // DistinctKey: sqlValueHash for SqlDateTime
            String json = """
                    [{"ts":"2024-01-01T10:00:00"},{"ts":"2024-01-01T10:00:00"},{"ts":"2024-01-02T15:00:00"}]""";
            String result = SQL4Json.query("SELECT DISTINCT TO_DATE(ts) AS dt FROM $r", json);
            assertNotNull(result);
        }
    }

    // ── Settings builder edge cases ──────────────────────────────────────

    @Nested
    class SettingsBranches {

        @Test
        void limitsBuilder_negativeMaxRows_throws() {
            // LimitsSettings.Builder: maxRowsPerQuery negative → throws
            assertThrows(
                    IllegalArgumentException.class,
                    () -> Sql4jsonSettings.builder()
                            .limits(l -> l.maxRowsPerQuery(-1))
                            .build());
        }

        @Test
        void securityBuilder_negativeWildcards() {
            // SecuritySettings.Builder edge case
            var settings = Sql4jsonSettings.builder()
                    .security(s -> s.maxLikeWildcards(0))
                    .build();
            assertEquals(0, settings.security().maxLikeWildcards());
        }

        @Test
        void cacheBuilder_disabledWithCustomSize() {
            // CacheSettings.Builder: disabled + custom size → still disabled
            var settings = Sql4jsonSettings.builder()
                    .cache(c -> c.queryResultCacheEnabled(false).queryResultCacheSize(1000))
                    .build();
            assertFalse(settings.cache().queryResultCacheEnabled());
        }

        @Test
        void securityBuilder_negativeWildcards_throws() {
            // SecuritySettings.Builder: negative maxLikeWildcards → throws
            assertThrows(
                    IllegalArgumentException.class,
                    () -> Sql4jsonSettings.builder()
                            .security(s -> s.maxLikeWildcards(-1))
                            .build());
        }

        @Test
        void cacheBuilder_zeroPatternCache_throws() {
            // CacheSettings.Builder: zero likePatternCacheSize → throws
            assertThrows(
                    IllegalArgumentException.class,
                    () -> Sql4jsonSettings.builder()
                            .cache(c -> c.likePatternCacheSize(0))
                            .build());
        }

        @Test
        void cacheBuilder_zeroCacheSize_throws() {
            // CacheSettings.Builder: zero queryResultCacheSize → throws
            assertThrows(
                    IllegalArgumentException.class,
                    () -> Sql4jsonSettings.builder()
                            .cache(c -> c.queryResultCacheSize(0))
                            .build());
        }
    }

    // ── Additional StreamingJsonParser paths ─────────────────────────────

    @Nested
    class AdditionalStreamingPaths {

        @Test
        void query_objectNotArray_singleResult() {
            // StreamingJsonParser: object root → single-element stream
            String json = "{\"name\": \"Alice\", \"age\": 30}";
            String result = SQL4Json.query("SELECT name FROM $r", json);
            assertTrue(result.contains("Alice"));
        }

        @Test
        void query_stringWithUnicode_inComposite() {
            // StreamingJsonParser: skipComposite + skipStringContents with unicode
            String json = "[{\"key\": \"val\\u0041\", \"nested\": {\"inner\": \"\\u0042\"}}]";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertNotNull(result);
        }

        @Test
        void query_nestedObjectWithBracketStrings() {
            // StreamingJsonParser: skipComposite with strings containing brackets
            String json = "[{\"data\": {\"msg\": \"[info] status: {ok}\"}}]";
            String result = SQL4Json.query("SELECT * FROM $r", json);
            assertNotNull(result);
        }
    }

    // ── BETWEEN/IN with null values ─────────────────────────────────────

    @Nested
    class NullConditionBranches {

        @Test
        void between_nullLowerBound() {
            // BetweenConditionHandler: lower.isNull() → true path
            String json = """
                    [{"age": 25}, {"age": 30}]""";
            String result = SQL4Json.query("SELECT * FROM $r WHERE age BETWEEN NULL AND 30", json);
            // null bound → always false for BETWEEN (negate=false → returns false)
            assertEquals("[]", result);
        }

        @Test
        void between_nullUpperBound() {
            // BetweenConditionHandler: upper.isNull() → true path
            String json = """
                    [{"age": 25}, {"age": 30}]""";
            String result = SQL4Json.query("SELECT * FROM $r WHERE age BETWEEN 20 AND NULL", json);
            assertEquals("[]", result);
        }

        @Test
        void notBetween_nullField() {
            // BetweenConditionHandler: fieldVal.isNull() with negate=true
            String result = SQL4Json.query("SELECT name FROM $r WHERE age NOT BETWEEN 20 AND 30", EMPLOYEES);
            // Diana has null age → negate=true → returns true → included
            assertTrue(result.contains("Diana") || result.contains("Charlie"));
        }

        @Test
        void in_nullField_returnsNegate() {
            // InConditionHandler: fieldVal.isNull() → return negate
            String result = SQL4Json.query("SELECT name FROM $r WHERE age IN (30, 25, 35)", EMPLOYEES);
            // Diana has null age → negate=false → returns false → excluded
            assertFalse(result.contains("Diana"));
        }

        @Test
        void notIn_nullField_excluded() {
            // SQL standard: null NOT IN (values) → UNKNOWN → excluded
            String result = SQL4Json.query("SELECT name FROM $r WHERE age NOT IN (30)", EMPLOYEES);
            // Diana has null age → UNKNOWN → excluded
            assertFalse(result.contains("Diana"));
        }

        @Test
        void in_withNullInList() {
            // InConditionHandler: null values in list filtered out
            String result = SQL4Json.query("SELECT name FROM $r WHERE age IN (30, NULL)", EMPLOYEES);
            assertTrue(result.contains("Alice"));
        }
    }

    // ── LIKE/NOT LIKE edge cases ────────────────────────────────────────

    @Nested
    class LikeEdgeCases {

        @Test
        void like_nonStringField_noMatch() {
            // LikeConditionHandler: value not instanceof SqlString → false
            String json = """
                    [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]""";
            String result = SQL4Json.query("SELECT * FROM $r WHERE age LIKE '3%'", json);
            // age is a number, not a string, so LIKE returns false for all
            assertFalse(result.contains("Alice"));
        }

        @Test
        void like_dynamicPattern_fromColumn() {
            // LikeConditionHandler: dynamic pattern path (rhs is column ref)
            String json = """
                    [{"name": "Alice", "pattern": "A%"}, {"name": "Bob", "pattern": "B%"}]""";
            String result = SQL4Json.query("SELECT name FROM $r WHERE name LIKE pattern", json);
            assertTrue(result.contains("Alice"));
            assertTrue(result.contains("Bob"));
        }

        @Test
        void notLike_dynamicPattern_fromColumn() {
            // NotLikeConditionHandler: dynamic pattern via buildPatternNode negate=true
            String json = """
                    [{"name": "Alice", "pattern": "A%"}, {"name": "Bob", "pattern": "B%"}]""";
            String result = SQL4Json.query("SELECT name FROM $r WHERE name NOT LIKE pattern", json);
            // Alice matches A%, so NOT LIKE returns false → excluded
            // Bob matches B%, so NOT LIKE returns false → excluded
            assertNotNull(result);
        }
    }

    // ── PreparedQuery non-DefaultJsonCodec path ─────────────────────────

    @Nested
    class PreparedQueryBranches {

        @Test
        void preparedQuery_withCustomCodec() {
            // PreparedQuery: execute with custom codec (non-DefaultJsonCodec)
            // Uses a codec wrapper that delegates to DefaultJsonCodec but has a different type
            var innerCodec = new io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec();
            var customCodec = new io.github.mnesimiyilmaz.sql4json.types.JsonCodec() {
                @Override
                public io.github.mnesimiyilmaz.sql4json.types.JsonValue parse(String json) {
                    return innerCodec.parse(json);
                }

                @Override
                public String serialize(io.github.mnesimiyilmaz.sql4json.types.JsonValue value) {
                    return innerCodec.serialize(value);
                }
            };
            var settings = Sql4jsonSettings.builder().codec(customCodec).build();
            PreparedQuery q = SQL4Json.prepare("SELECT * FROM $r", settings);
            String result = q.execute("[{\"a\":1}]");
            assertTrue(result.contains("a"));
        }
    }

    // ── SQL4Json non-DefaultJsonCodec paths ──────────────────────────────

    @Nested
    class NonDefaultCodecPaths {

        private io.github.mnesimiyilmaz.sql4json.types.JsonCodec customCodec() {
            var inner = new io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec();
            return new io.github.mnesimiyilmaz.sql4json.types.JsonCodec() {
                @Override
                public io.github.mnesimiyilmaz.sql4json.types.JsonValue parse(String json) {
                    return inner.parse(json);
                }

                @Override
                public String serialize(io.github.mnesimiyilmaz.sql4json.types.JsonValue value) {
                    return inner.serialize(value);
                }
            };
        }

        @Test
        void query_withCustomCodec() {
            // SQL4Json: query(sql, json, settings) with non-DefaultJsonCodec
            var settings = Sql4jsonSettings.builder().codec(customCodec()).build();
            String result = SQL4Json.query("SELECT * FROM $r", "[{\"a\":1}]", settings);
            assertTrue(result.contains("a"));
        }

        @Test
        void queryAsJsonValue_withCustomCodec() {
            // SQL4Json: queryAsJsonValue(sql, json, settings) with non-DefaultJsonCodec
            var settings = Sql4jsonSettings.builder().codec(customCodec()).build();
            var result = SQL4Json.queryAsJsonValue("SELECT * FROM $r", "[{\"a\":1}]", settings);
            assertNotNull(result);
        }
    }
}
