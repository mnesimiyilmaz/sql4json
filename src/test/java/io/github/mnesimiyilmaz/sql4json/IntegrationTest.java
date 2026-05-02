// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.json.JsonParser;
import io.github.mnesimiyilmaz.sql4json.json.JsonSerializer;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IntegrationTest {

    // ══════════════════════════════════════════════════════════════════════
    // CoverageTests (from CoverageIntegrationTest.java)
    // ══════════════════════════════════════════════════════════════════════

    @Nested
    class CoverageTests {

        private static final String DATA = """
                [
                    {"name":"Alice","age":30,"active":true,"hire_date":"2021-03-15","salary":75000},
                    {"name":"Bob","age":20,"active":false,"hire_date":"2019-07-01","salary":60000},
                    {"name":"Charlie","age":35,"active":true,"hire_date":"2018-01-20","salary":90000}
                ]
                """;

        private JsonValue query(String sql) {
            return SQL4Json.queryAsJsonValue(sql, DATA);
        }

        /** Helper: get array elements from a JsonValue. */
        private static List<JsonValue> arr(JsonValue v) {
            return v.asArray().orElseThrow();
        }

        /** Helper: get object fields from a JsonValue. */
        private static Map<String, JsonValue> obj(JsonValue v) {
            return v.asObject().orElseThrow();
        }

        /** Helper: get string value. */
        private static String str(JsonValue v) {
            return v.asString().orElseThrow();
        }

        /** Helper: get int value. */
        private static int num(JsonValue v) {
            return v.asNumber().orElseThrow().intValue();
        }

        /** Helper: get a field from a row. */
        private static JsonValue f(JsonValue row, String key) {
            return obj(row).get(key);
        }

        // ── ORDER BY on various types (covers SqlValueComparator Date/DateTime/Boolean/String paths) ──

        @Test
        void orderBy_booleanField() {
            JsonValue result = query("SELECT name, active FROM $r ORDER BY active ASC");
            // false < true → Bob first
            assertEquals("Bob", str(f(arr(result).get(0), "name")));
        }

        @Test
        void orderBy_stringField() {
            JsonValue result = query("SELECT name FROM $r ORDER BY name ASC");
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
            assertEquals("Bob", str(f(arr(result).get(1), "name")));
            assertEquals("Charlie", str(f(arr(result).get(2), "name")));
        }

        @Test
        void orderBy_dateField() {
            JsonValue result = query("SELECT name FROM $r ORDER BY hire_date ASC");
            // 2018-01-20 < 2019-07-01 < 2021-03-15 (string comparison, same format)
            assertEquals("Charlie", str(f(arr(result).get(0), "name")));
            assertEquals("Bob", str(f(arr(result).get(1), "name")));
            assertEquals("Alice", str(f(arr(result).get(2), "name")));
        }

        // ── WHERE with RHS function call (covers parser RHS scalar function path) ──

        @Test
        void where_rhsFunctionCall() {
            JsonValue result = query("SELECT name FROM $r WHERE UPPER(name) = UPPER('alice')");
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
        }

        // ── WHERE with CAST (covers parser CAST in condition) ──

        @Test
        void where_castInCondition() {
            JsonValue result = query("SELECT name FROM $r WHERE CAST(age AS STRING) = '30'");
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
        }

        // ── GROUP BY with SELECT * (covers GroupAggregator SELECT * path) ──

        @Test
        void groupBy_selectStar() {
            JsonValue result = query("SELECT * FROM $r GROUP BY active ORDER BY active ASC");
            // Two groups: active=false and active=true
            assertEquals(2, arr(result).size());
        }

        // ── Syntax error in SQL (covers QueryParser error paths) ──

        @Test
        void invalidSql_throwsParseException() {
            assertThrows(SQL4JsonParseException.class, () -> SQL4Json.query("SELECT FROM", DATA));
        }

        @Test
        void invalidSql_missingFrom_throwsParseException() {
            assertThrows(SQL4JsonParseException.class, () -> SQL4Json.query("SELECT name WHERE age > 1", DATA));
        }

        // ── DATE_ADD/DATE_DIFF via SQL (covers integration path) ──

        @Test
        void dateAdd_inSelect() {
            JsonValue result = query("SELECT name, DATE_ADD(TO_DATE(hire_date), 30, 'DAY') AS future FROM $r LIMIT 1");
            assertEquals(1, arr(result).size());
            assertEquals("2021-04-14", str(f(arr(result).get(0), "future")));
        }

        @Test
        void dateDiff_inSelect() {
            String dateData = """
                    [{"d1":"2024-01-11","d2":"2024-01-01"}]
                    """;
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT DATE_DIFF(TO_DATE(d1), TO_DATE(d2), 'DAY') AS diff FROM $r", dateData);
            assertEquals(1, arr(result).size());
            assertEquals(10, num(f(arr(result).get(0), "diff")));
        }

        // ── HOUR/MINUTE/SECOND on dates via SQL ──

        @Test
        void hourMinuteSecond_onDateField() {
            JsonValue result = query("""
                    SELECT name,
                           HOUR(TO_DATE(hire_date)) AS h,
                           MINUTE(TO_DATE(hire_date)) AS m,
                           SECOND(TO_DATE(hire_date)) AS s
                    FROM $r
                    WHERE name = 'Alice'
                    """);
            assertEquals(1, arr(result).size());
            assertEquals(0, num(f(arr(result).get(0), "h")));
            assertEquals(0, num(f(arr(result).get(0), "m")));
            assertEquals(0, num(f(arr(result).get(0), "s")));
        }

        // ── CAST various types via SQL ──

        @Test
        void cast_dateTimeToDate_inSelect() {
            String dtData = "[{\"ts\":\"2024-06-15T14:30:00\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT CAST(TO_DATE(ts) AS DATE) AS d FROM $r", dtData);
            assertEquals("2024-06-15", str(f(arr(result).get(0), "d")));
        }

        @Test
        void cast_dateToDateTime_inSelect() {
            String dData = "[{\"dt\":\"2024-06-15\"}]";
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT CAST(TO_DATE(dt) AS DATETIME) AS result FROM $r", dData);
            assertEquals("2024-06-15T00:00", str(f(arr(result).get(0), "result")));
        }

        @Test
        void cast_booleanToInteger_inSelect() {
            JsonValue result = query("SELECT CAST(active AS INTEGER) AS val FROM $r WHERE name = 'Alice'");
            assertEquals(1, arr(result).size());
            assertEquals(1, num(f(arr(result).get(0), "val")));
        }

        @Test
        void cast_booleanToString_inSelect() {
            JsonValue result = query("SELECT CAST(active AS STRING) AS val FROM $r WHERE name = 'Alice'");
            assertEquals("true", str(f(arr(result).get(0), "val")));
        }

        // ── WHERE with nested RHS function (covers evaluateRhsScalarFunction) ──

        @Test
        void where_rhsNestedFunction() {
            JsonValue result = query("SELECT name FROM $r WHERE name = UPPER('alice')");
            // UPPER('alice') = 'ALICE', but name is 'Alice' → no match
            assertEquals(0, arr(result).size());
        }

        // ── GROUP BY literal in SELECT (covers evaluateAggregate LiteralVal/ColumnRef paths) ──

        @Test
        void groupBy_withLiteralInSelect() {
            JsonValue result = query("SELECT active, COUNT(*) AS cnt FROM $r GROUP BY active ORDER BY active ASC");
            assertEquals(2, arr(result).size());
            // false group: Bob
            assertEquals(1, num(f(arr(result).get(0), "cnt")));
            // true group: Alice, Charlie
            assertEquals(2, num(f(arr(result).get(1), "cnt")));
        }

        // ── WHERE with NULL value (covers toSqlValue NULL path in parser) ──

        @Test
        void where_isNull() {
            String data = "[{\"name\":\"Alice\",\"x\":null},{\"name\":\"Bob\",\"x\":1}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT name FROM $r WHERE x IS NULL", data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
        }

        // ── ORDER BY DESC (covers OrderByColumnDef direction) ──

        @Test
        void orderBy_desc() {
            JsonValue result = query("SELECT name FROM $r ORDER BY name DESC");
            assertEquals("Charlie", str(f(arr(result).get(0), "name")));
            assertEquals("Bob", str(f(arr(result).get(1), "name")));
            assertEquals("Alice", str(f(arr(result).get(2), "name")));
        }

        // ── LIMIT and OFFSET (covers parser LIMIT/OFFSET paths) ──

        @Test
        void limit_offset() {
            JsonValue result = query("SELECT name FROM $r ORDER BY name ASC LIMIT 1 OFFSET 1");
            assertEquals(1, arr(result).size());
            assertEquals("Bob", str(f(arr(result).get(0), "name")));
        }

        // ── DISTINCT (covers DistinctStage paths) ──

        @Test
        void distinct_deduplicates() {
            String data = "[{\"x\":1},{\"x\":1},{\"x\":2}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT DISTINCT x FROM $r ORDER BY x ASC", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void distinct_nullValue() {
            String data = "[{\"x\":null},{\"x\":null},{\"x\":1}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT DISTINCT x FROM $r", data);
            assertEquals(2, arr(result).size());
        }

        // ── WHERE boolean literal (covers parser BOOLEAN value path) ──

        @Test
        void where_booleanLiteral() {
            JsonValue result = query("SELECT name FROM $r WHERE active = true ORDER BY name ASC");
            assertEquals(2, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
            assertEquals("Charlie", str(f(arr(result).get(1), "name")));
        }

        // ── CAST in SELECT with various types from SQL ──

        @Test
        void cast_numberToString_inSelect() {
            JsonValue result = query("SELECT CAST(age AS STRING) AS age_str FROM $r WHERE name = 'Alice'");
            // JSON integer 30 → SqlNumber(Integer(30)) → toString → "30"
            assertEquals("30", str(f(arr(result).get(0), "age_str")));
        }

        @Test
        void cast_stringToDate_inSelect() {
            JsonValue result = query("SELECT CAST(hire_date AS DATE) AS d FROM $r WHERE name = 'Alice'");
            assertEquals("2021-03-15", str(f(arr(result).get(0), "d")));
        }

        // ── WHERE with IN list containing function calls ──

        @Test
        void where_inList() {
            JsonValue result = query("SELECT name FROM $r WHERE name IN ('Alice', 'Charlie') ORDER BY name ASC");
            assertEquals(2, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
            assertEquals("Charlie", str(f(arr(result).get(1), "name")));
        }

        @Test
        void where_notIn() {
            JsonValue result = query("SELECT name FROM $r WHERE name NOT IN ('Alice', 'Charlie')");
            assertEquals(1, arr(result).size());
            assertEquals("Bob", str(f(arr(result).get(0), "name")));
        }

        @Test
        void where_between() {
            // ages: Alice=30, Bob=20, Charlie=35
            JsonValue result = query("SELECT name FROM $r WHERE age BETWEEN 20 AND 30 ORDER BY name ASC");
            assertEquals(2, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
            assertEquals("Bob", str(f(arr(result).get(1), "name")));
        }

        @Test
        void where_notBetween() {
            // ages: Alice=30, Bob=20, Charlie=35 → NOT BETWEEN 20 AND 30 → Charlie
            JsonValue result = query("SELECT name FROM $r WHERE age NOT BETWEEN 20 AND 30");
            assertEquals(1, arr(result).size());
            assertEquals("Charlie", str(f(arr(result).get(0), "name")));
        }

        @Test
        void where_notLike() {
            JsonValue result = query("SELECT name FROM $r WHERE name NOT LIKE 'A%' ORDER BY name ASC");
            assertEquals(2, arr(result).size());
            assertEquals("Bob", str(f(arr(result).get(0), "name")));
            assertEquals("Charlie", str(f(arr(result).get(1), "name")));
        }

        @Test
        void where_isNotNull() {
            JsonValue result = query("SELECT name FROM $r WHERE salary IS NOT NULL");
            assertEquals(3, arr(result).size());
        }

        // ── SELECT with CAST in expression (covers parser buildExpression CastExprColumnContext) ──

        @Test
        void select_castExpression() {
            JsonValue result = query("SELECT CAST(salary AS STRING) AS sal_str FROM $r WHERE name = 'Alice'");
            assertEquals("75000", str(f(arr(result).get(0), "sal_str")));
        }

        // ── GROUP BY with scalar function in SELECT (covers aggFunction search through scalar) ──

        @Test
        void groupBy_roundAvg_inSelect() {
            JsonValue result = query(
                    "SELECT active, ROUND(AVG(salary), 0) AS avg_sal FROM $r GROUP BY active ORDER BY active ASC");
            assertEquals(2, arr(result).size());
            assertEquals(60000, num(f(arr(result).get(0), "avg_sal")));
        }

        // ── Nested subquery with WHERE in both levels ──

        @Test
        void nestedSubquery_coversParsePaths() {
            JsonValue result = query("""
                    SELECT name, salary
                    FROM (SELECT * FROM $r WHERE age > 15)
                    WHERE salary >= 75000
                    ORDER BY salary DESC
                    """);
            assertEquals(2, arr(result).size());
            assertEquals("Charlie", str(f(arr(result).get(0), "name")));
            assertEquals("Alice", str(f(arr(result).get(1), "name")));
        }

        // ── RHS CAST expression in WHERE (covers parser lines 353-358) ──

        @Test
        void where_rhsCastExpression() {
            // CAST(TRIM('  30  ') AS NUMBER) on the RHS — function inside CAST on RHS
            // Covers: RhsCastExprContext path (lines 353-358) AND
            // evaluateRhsColumnExpr FunctionCallExprContext path (lines 369-371)
            JsonValue result = query("SELECT name FROM $r WHERE age = CAST(TRIM(' 30 ') AS NUMBER)");
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
        }

        @Test
        void where_rhsCastWithFunction() {
            // CAST(UPPER('alice') AS STRING) — function inside CAST on RHS
            JsonValue result = query("SELECT name FROM $r WHERE name = CAST(UPPER('Alice') AS STRING)");
            // UPPER('Alice') = 'ALICE', CAST(... AS STRING) = 'ALICE', name='Alice' → no match
            assertEquals(0, arr(result).size());
        }

        // ── WHERE with NULL value (covers toSqlValue NULL path, line 430) ──

        @Test
        void where_comparisonWithNull() {
            String data = "[{\"name\":\"Alice\",\"x\":null},{\"name\":\"Bob\",\"x\":1}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT name FROM $r WHERE x = NULL", data);
            // SQL semantics: NULL = NULL → false; use IS NULL instead
            assertEquals(0, arr(result).size());
        }

        // ── WHERE with RHS nested function with expression arg (covers lines 411-418) ──

        @Test
        void where_rhsFunctionWithNestedFunctionArg() {
            // CONCAT(UPPER('al'), LOWER('ICE')) — nested function calls on RHS
            // evaluateRhsFunctionArg → ExprFunctionArgContext → evaluateRhsColumnExpr
            JsonValue result = query("SELECT name FROM $r WHERE name = CONCAT(UPPER('al'), LOWER('ICE'))");
            // UPPER('al')='AL', LOWER('ICE')='ice' → CONCAT='ALice'
            // name='Alice' vs 'ALice' → no match (case diff)
            assertEquals(0, arr(result).size());
        }

        @Test
        void where_rhsReplace() {
            // REPLACE('Alica', 'a', 'e') on RHS — function with multiple value args
            JsonValue result = query("SELECT name FROM $r WHERE name = REPLACE('Alica', 'a', 'e')");
            // REPLACE('Alica', 'a', 'e') = 'Alice' → matches Alice
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // FullPipelineTests (from FullPipelineIntegrationTest.java)
    // ══════════════════════════════════════════════════════════════════════

    @Nested
    class FullPipelineTests {

        private static final String DATA = """
                [
                    {
                        "name": "Alice",
                        "age": 29,
                        "department": {"name": "Engineering", "floor": 3, "manager": {"name": "Grace", "level": "Director"}},
                        "address": {"city": "Istanbul", "country": "Turkey", "zip": "34000"},
                        "salary": 75000,
                        "rating": 4.5,
                        "hire_date": "2021-03-15",
                        "active": true
                    },
                    {
                        "name": "Bob",
                        "age": 35,
                        "department": {"name": "Engineering", "floor": 3, "manager": {"name": "Grace", "level": "Director"}},
                        "address": {"city": "Ankara", "country": "Turkey", "zip": "06000"},
                        "salary": 92000,
                        "rating": 3.8,
                        "hire_date": "2019-07-01",
                        "active": true
                    },
                    {
                        "name": "Charlie",
                        "age": 42,
                        "department": {"name": "Sales", "floor": 1, "manager": {"name": "Hank", "level": "VP"}},
                        "address": {"city": "Berlin", "country": "Germany", "zip": "10115"},
                        "salary": 68000,
                        "rating": 4.1,
                        "hire_date": "2018-01-20",
                        "active": false
                    },
                    {
                        "name": "Diana",
                        "age": 31,
                        "department": {"name": "Sales", "floor": 1, "manager": {"name": "Hank", "level": "VP"}},
                        "address": {"city": "Munich", "country": "Germany", "zip": "80331"},
                        "salary": 71000,
                        "rating": 4.7,
                        "hire_date": "2020-11-10",
                        "active": true
                    },
                    {
                        "name": "Eve",
                        "age": 26,
                        "department": {"name": "Engineering", "floor": 3, "manager": {"name": "Grace", "level": "Director"}},
                        "address": {"city": "Istanbul", "country": "Turkey", "zip": "34100"},
                        "salary": 62000,
                        "rating": 4.9,
                        "hire_date": "2023-06-01",
                        "active": true
                    },
                    {
                        "name": "Frank",
                        "age": 38,
                        "department": {"name": "HR", "floor": 2, "manager": {"name": "Ivy", "level": "Director"}},
                        "address": {"city": "Paris", "country": "France", "zip": "75001"},
                        "salary": 65000,
                        "rating": 3.5,
                        "hire_date": "2017-04-12",
                        "active": true
                    },
                    {
                        "name": "Grace",
                        "age": 45,
                        "department": {"name": "Engineering", "floor": 3, "manager": {"name": "Zara", "level": "CTO"}},
                        "address": {"city": "Ankara", "country": "Turkey", "zip": "06100"},
                        "salary": 120000,
                        "rating": 4.3,
                        "hire_date": "2015-09-01",
                        "active": true
                    },
                    {
                        "name": "Hank",
                        "age": 50,
                        "department": {"name": "Sales", "floor": 1, "manager": {"name": "Zara", "level": "CTO"}},
                        "address": {"city": "Berlin", "country": "Germany", "zip": "10117"},
                        "salary": 110000,
                        "rating": 3.9,
                        "hire_date": "2016-02-28",
                        "active": true
                    },
                    {
                        "name": "Ivy",
                        "age": 40,
                        "department": {"name": "HR", "floor": 2, "manager": {"name": "Zara", "level": "CTO"}},
                        "address": {"city": "Paris", "country": "France", "zip": "75002"},
                        "salary": 95000,
                        "rating": 4.0,
                        "hire_date": "2016-08-15",
                        "active": true
                    },
                    {
                        "name": "Jack",
                        "age": 27,
                        "department": {"name": "Engineering", "floor": 3, "manager": {"name": "Grace", "level": "Director"}},
                        "address": {"city": "Munich", "country": "Germany", "zip": "80333"},
                        "salary": 58000,
                        "rating": 3.2,
                        "hire_date": "2024-01-10",
                        "active": true
                    },
                    {
                        "name": "Karen",
                        "age": 33,
                        "department": {"name": "Sales", "floor": 1, "manager": {"name": "Hank", "level": "VP"}},
                        "address": {"city": "Istanbul", "country": "Turkey", "zip": "34200"},
                        "salary": 72000,
                        "rating": 4.4,
                        "hire_date": "2020-05-22",
                        "active": false
                    },
                    {
                        "name": "Leo",
                        "age": 29,
                        "department": {"name": "HR", "floor": 2, "manager": {"name": "Ivy", "level": "Director"}},
                        "address": {"city": "Berlin", "country": "Germany", "zip": "10119"},
                        "salary": 60000,
                        "rating": 3.7,
                        "hire_date": "2022-03-01",
                        "active": true
                    }
                ]
                """;

        private JsonValue query(String sql) {
            return SQL4Json.queryAsJsonValue(sql, DATA);
        }

        /** Helper: get array element at index from a JsonValue array. */
        private static List<JsonValue> arr(JsonValue v) {
            return v.asArray().orElseThrow();
        }

        /** Helper: get object field map from a JsonValue object. */
        private static Map<String, JsonValue> obj(JsonValue v) {
            return v.asObject().orElseThrow();
        }

        /** Helper: get a nested object field. */
        private static JsonValue field(JsonValue v, String... keys) {
            JsonValue current = v;
            for (String key : keys) {
                current = obj(current).get(key);
            }
            return current;
        }

        /** Helper: get string value. */
        private static String str(JsonValue v) {
            return v.asString().orElseThrow();
        }

        /** Helper: get int value. */
        private static int num(JsonValue v) {
            return v.asNumber().orElseThrow().intValue();
        }

        /** Helper: get double value. */
        private static double dbl(JsonValue v) {
            return v.asNumber().orElseThrow().doubleValue();
        }

        /**
         * Nested query filters active employees, then outer groups by department with aggregates and filters groups via
         * HAVING, ordered by total salary.
         *
         * <p>Features: nested query, WHERE (inner), nested field select + alias, GROUP BY, HAVING, COUNT, SUM, AVG,
         * ORDER BY aggregate DESC
         */
        @Test
        void nestedQuery_groupBy_having_orderByAggregate() {
            JsonValue result = query("""
                    SELECT
                        dept.name                   AS dept.info.name,
                        COUNT(*)                    AS dept.stats.headcnt,
                        SUM(dept.salary)            AS dept.stats.total_salary,
                        AVG(dept.salary)            AS dept.stats.mean_salary
                    FROM (
                        SELECT
                            department.name     AS dept.name,
                            salary              AS dept.salary,
                            rating              AS dept.rating
                        FROM $r
                        WHERE active = true
                    )
                    GROUP BY dept.name
                    HAVING dept.stats.headcnt >= 3
                    ORDER BY dept.stats.total_salary DESC
                    """);

            // Active employees by dept:
            //   Engineering: Alice(75k), Bob(92k), Eve(62k), Grace(120k), Jack(58k) → 5, total=407000
            //   HR:          Frank(65k), Ivy(95k), Leo(60k)                         → 3, total=220000
            //   Sales:       Diana(71k), Hank(110k)                                 → 2, filtered by HAVING
            assertEquals(2, arr(result).size());

            JsonValue eng = arr(result).get(0);
            assertEquals("Engineering", str(field(eng, "dept", "info", "name")));
            assertEquals(5, num(field(eng, "dept", "stats", "headcnt")));
            assertEquals(407000, num(field(eng, "dept", "stats", "total_salary")));
            assertEquals(81400.0, dbl(field(eng, "dept", "stats", "mean_salary")), 0.01);

            JsonValue hr = arr(result).get(1);
            assertEquals("HR", str(field(hr, "dept", "info", "name")));
            assertEquals(3, num(field(hr, "dept", "stats", "headcnt")));
            assertEquals(220000, num(field(hr, "dept", "stats", "total_salary")));
        }

        /**
         * Two-level nested query. Innermost filters by salary, middle filters by country, outer re-projects with nested
         * aliases, applies ORDER BY and LIMIT.
         *
         * <p>Features: 2-level nested queries, WHERE at each level, nested field access (address.city,
         * address.country), nested output aliases, ORDER BY, LIMIT
         */
        @Test
        void twoLevelNestedQuery_filtersAtEachLevel_orderBy_limit() {
            JsonValue result = query("""
                    SELECT
                        nm      AS result.name,
                        ct      AS result.city,
                        sal     AS result.salary
                    FROM (
                        SELECT nm, ct, cntry, sal
                        FROM (
                            SELECT
                                name            AS nm,
                                address.city    AS ct,
                                address.country AS cntry,
                                salary          AS sal
                            FROM $r
                            WHERE salary >= 60000
                        )
                        WHERE cntry = 'Turkey'
                    )
                    ORDER BY sal DESC
                    LIMIT 3
                    """);

            // Inner: salary >= 60000 → 11 rows (Jack excluded at 58k)
            // Middle: cntry = Turkey → Alice(Istanbul,75k), Bob(Ankara,92k),
            //         Grace(Ankara,120k), Karen(Istanbul,72k)
            // Outer: ORDER BY sal DESC, LIMIT 3 → Grace(120k), Bob(92k), Alice(75k)
            assertEquals(3, arr(result).size());

            assertEquals("Grace", str(field(arr(result).get(0), "result", "name")));
            assertEquals("Ankara", str(field(arr(result).get(0), "result", "city")));
            assertEquals(120000, num(field(arr(result).get(0), "result", "salary")));

            assertEquals("Bob", str(field(arr(result).get(1), "result", "name")));
            assertEquals(92000, num(field(arr(result).get(1), "result", "salary")));

            assertEquals("Alice", str(field(arr(result).get(2), "result", "name")));
            assertEquals(75000, num(field(arr(result).get(2), "result", "salary")));
        }

        /**
         * Nested query pre-computes UPPER on a nested field, then outer query groups by it, applies NOT LIKE filter,
         * multiple aggregates, ORDER BY, and LIMIT.
         *
         * <p>Features: nested query, UPPER, NOT LIKE, nested field access (address.country), GROUP BY, COUNT, SUM, AVG,
         * MAX, MIN, ORDER BY aggregate, LIMIT, nested output aliases
         */
        @Test
        void stringFunctions_groupBy_aggregates_orderBy_limit() {
            JsonValue result = query("""
                    SELECT
                        country                         AS stats.country,
                        COUNT(*)                        AS stats.emp_cnt,
                        SUM(sal)                        AS stats.total_salary,
                        AVG(sal)                        AS stats.mean_salary,
                        MAX(rat)                        AS stats.best_rating,
                        MIN(rat)                        AS stats.worst_rating
                    FROM (
                        SELECT
                            UPPER(address.country)  AS country,
                            salary                  AS sal,
                            rating                  AS rat,
                            name                    AS nm
                        FROM $r
                        WHERE name NOT LIKE 'J%' AND salary > 55000
                    )
                    GROUP BY country
                    ORDER BY stats.total_salary DESC
                    LIMIT 2
                    """);

            // Inner WHERE: exclude Jack (J%), all remaining have salary > 55000 → 11 rows
            // UPPER(country) pre-computed in inner SELECT
            // GROUP BY country:
            //   TURKEY:  Alice(75k,4.5), Bob(92k,3.8), Eve(62k,4.9), Grace(120k,4.3), Karen(72k,4.4)
            //            → cnt=5, sum=421000, max_rat=4.9, min_rat=3.8
            //   GERMANY: Charlie(68k,4.1), Diana(71k,4.7), Hank(110k,3.9), Leo(60k,3.7)
            //            → cnt=4, sum=309000, max_rat=4.7, min_rat=3.7
            //   FRANCE:  Frank(65k,3.5), Ivy(95k,4.0)
            //            → cnt=2, sum=160000, max_rat=4.0, min_rat=3.5
            // ORDER BY total DESC, LIMIT 2 → TURKEY, GERMANY
            assertEquals(2, arr(result).size());

            JsonValue turkey = arr(result).get(0);
            assertEquals("TURKEY", str(field(turkey, "stats", "country")));
            assertEquals(5, num(field(turkey, "stats", "emp_cnt")));
            assertEquals(421000, num(field(turkey, "stats", "total_salary")));
            assertEquals(4.9, dbl(field(turkey, "stats", "best_rating")), 0.01);
            assertEquals(3.8, dbl(field(turkey, "stats", "worst_rating")), 0.01);

            JsonValue germany = arr(result).get(1);
            assertEquals("GERMANY", str(field(germany, "stats", "country")));
            assertEquals(4, num(field(germany, "stats", "emp_cnt")));
            assertEquals(309000, num(field(germany, "stats", "total_salary")));
        }

        /**
         * Nested query uses LEFT to extract year from date string. Outer query applies BETWEEN pre-filter, GROUP BY on
         * the extracted year, HAVING, ORDER BY.
         *
         * <p>Features: LEFT, BETWEEN, nested query, GROUP BY, HAVING, COUNT, AVG, ORDER BY, nested aliases
         */
        @Test
        void dateFunctions_between_groupByYear_having_orderBy() {
            JsonValue result = query("""
                    SELECT
                        yr              AS hire.yr,
                        COUNT(*)        AS hire.cnt,
                        AVG(sal)        AS hire.mean_salary
                    FROM (
                        SELECT
                            LEFT(hire_date, 4)  AS yr,
                            salary              AS sal
                        FROM $r
                        WHERE hire_date BETWEEN '2016-01-01' AND '2021-12-31'
                    )
                    GROUP BY yr
                    HAVING hire.cnt >= 2
                    ORDER BY yr ASC
                    """);

            // Inner: BETWEEN 2016-01-01 AND 2021-12-31, LEFT(hire_date, 4) → year string
            //   2016: Hank(110k), Ivy(95k)         → cnt=2, avg=102500
            //   2017: Frank(65k)                    → cnt=1, filtered
            //   2018: Charlie(68k)                  → cnt=1, filtered
            //   2019: Bob(92k)                      → cnt=1, filtered
            //   2020: Diana(71k), Karen(72k)        → cnt=2, avg=71500
            //   2021: Alice(75k)                    → cnt=1, filtered
            assertEquals(2, arr(result).size());

            assertEquals("2016", str(field(arr(result).get(0), "hire", "yr")));
            assertEquals(2, num(field(arr(result).get(0), "hire", "cnt")));
            assertEquals(102500.0, dbl(field(arr(result).get(0), "hire", "mean_salary")), 0.01);

            assertEquals("2020", str(field(arr(result).get(1), "hire", "yr")));
            assertEquals(2, num(field(arr(result).get(1), "hire", "cnt")));
            assertEquals(71500.0, dbl(field(arr(result).get(1), "hire", "mean_salary")), 0.01);
        }

        /**
         * Complex WHERE with IN, NOT LIKE, AND/OR with parentheses, combined with DISTINCT, ORDER BY, LIMIT, and
         * OFFSET. Nested field access throughout.
         *
         * <p>Pipeline order: WHERE → ORDER BY → LIMIT/OFFSET → SELECT → DISTINCT
         *
         * <p>Features: IN, NOT LIKE, AND, OR, parentheses, DISTINCT, ORDER BY, LIMIT, OFFSET, nested field select +
         * alias
         */
        @Test
        void in_notLike_compoundWhere_distinct_orderBy_limitOffset() {
            JsonValue result = query("""
                    SELECT DISTINCT
                        address.country     AS geo.country,
                        address.city        AS geo.city
                    FROM $r
                    WHERE (address.country IN ('Turkey', 'Germany') AND name NOT LIKE '%a%')
                       OR department.name = 'HR'
                    ORDER BY address.country ASC, address.city ASC
                    LIMIT 3
                    OFFSET 2
                    """);

            // WHERE evaluation (LIKE is case-sensitive, '%a%' matches lowercase 'a'):
            //   Alice:   Turkey ✓, "Alice" has no 'a' → left=TRUE  → MATCH
            //   Bob:     Turkey ✓, "Bob" no 'a'       → left=TRUE  → MATCH
            //   Charlie: Germany ✓, "Charlie" has 'a'  → left=FALSE; dept=Sales  → NO
            //   Diana:   Germany ✓, "Diana" has 'a'    → left=FALSE; dept=Sales  → NO
            //   Eve:     Turkey ✓, "Eve" no 'a'        → left=TRUE  → MATCH
            //   Frank:   France ✗                      → left=FALSE; dept=HR     → MATCH (OR)
            //   Grace:   Turkey ✓, "Grace" has 'a'     → left=FALSE; dept=Eng   → NO
            //   Hank:    Germany ✓, "Hank" has 'a'     → left=FALSE; dept=Sales → NO
            //   Ivy:     France ✗                      → left=FALSE; dept=HR    → MATCH (OR)
            //   Jack:    Germany ✓, "Jack" has 'a'     → left=FALSE; dept=Eng   → NO
            //   Karen:   Turkey ✓, "Karen" has 'a'     → left=FALSE; dept=Sales → NO
            //   Leo:     Germany ✓, "Leo" no 'a'       → left=TRUE  → MATCH
            //
            // Matched (6): Alice(TR,Istanbul), Bob(TR,Ankara), Eve(TR,Istanbul),
            //              Frank(FR,Paris), Ivy(FR,Paris), Leo(DE,Berlin)
            //
            // ORDER BY address.country ASC, address.city ASC:
            //   1. Frank  (France, Paris)
            //   2. Ivy    (France, Paris)
            //   3. Leo    (Germany, Berlin)
            //   4. Bob    (Turkey, Ankara)
            //   5. Alice  (Turkey, Istanbul)
            //   6. Eve    (Turkey, Istanbul)
            //
            // OFFSET 2 LIMIT 3 → rows 3,4,5: Leo, Bob, Alice
            // SELECT projects: (Germany,Berlin), (Turkey,Ankara), (Turkey,Istanbul)
            // DISTINCT: all unique → 3 rows
            assertEquals(3, arr(result).size());

            assertEquals("Germany", str(field(arr(result).get(0), "geo", "country")));
            assertEquals("Berlin", str(field(arr(result).get(0), "geo", "city")));

            assertEquals("Turkey", str(field(arr(result).get(1), "geo", "country")));
            assertEquals("Ankara", str(field(arr(result).get(1), "geo", "city")));

            assertEquals("Turkey", str(field(arr(result).get(2), "geo", "country")));
            assertEquals("Istanbul", str(field(arr(result).get(2), "geo", "city")));
        }

        /**
         * The kitchen sink: nested query with 3-level deep source field access → WHERE inner + outer → GROUP BY
         * multiple columns → HAVING → ORDER BY → LIMIT. Nested aliases produce deeply structured JSON output.
         *
         * <p>Features: nested query, WHERE (inner + outer), IS NOT NULL, nested field access (department.manager.name —
         * 3 levels), deeply nested output aliases, GROUP BY multiple columns, HAVING, COUNT, SUM, AVG, MAX, MIN, ORDER
         * BY, LIMIT
         */
        @Test
        void kitchenSink_nestedQuery_functions_groupBy_having_orderBy_limit() {
            JsonValue result = query("""
                    SELECT
                        team.dept                   AS report.department,
                        team.mgr                    AS report.manager,
                        COUNT(*)                    AS report.metrics.headcnt,
                        AVG(team.pay)               AS report.metrics.mean_salary,
                        MAX(team.pay)               AS report.metrics.max_salary,
                        MIN(team.pay)               AS report.metrics.min_salary,
                        SUM(team.pay)               AS report.metrics.total_cost
                    FROM (
                        SELECT
                            department.name             AS team.dept,
                            department.manager.name     AS team.mgr,
                            department.manager.level    AS team.mgr_level,
                            salary                      AS team.pay
                        FROM $r
                        WHERE active = true AND salary >= 60000
                    )
                    WHERE team.mgr IS NOT NULL
                    GROUP BY team.dept, team.mgr
                    HAVING report.metrics.headcnt >= 2
                    ORDER BY report.metrics.mean_salary DESC
                    LIMIT 3
                    """);

            // Inner: active=true AND salary>=60000 →
            //   Alice(Eng,Grace,75k), Bob(Eng,Grace,92k), Diana(Sales,Hank,71k),
            //   Eve(Eng,Grace,62k), Frank(HR,Ivy,65k), Grace(Eng,Zara,120k),
            //   Hank(Sales,Zara,110k), Ivy(HR,Zara,95k), Leo(HR,Ivy,60k)
            //
            // Outer WHERE: IS NOT NULL → all pass
            //
            // GROUP BY (dept, mgr):
            //   (Engineering, Grace): Alice(75k), Bob(92k), Eve(62k) → cnt=3, sum=229000, avg≈76333.33
            //   (Engineering, Zara):  Grace(120k)                    → cnt=1, filtered
            //   (Sales, Hank):        Diana(71k)                     → cnt=1, filtered
            //   (Sales, Zara):        Hank(110k)                     → cnt=1, filtered
            //   (HR, Ivy):            Frank(65k), Leo(60k)           → cnt=2, sum=125000, avg=62500
            //   (HR, Zara):           Ivy(95k)                       → cnt=1, filtered
            //
            // HAVING cnt>=2: (Engineering,Grace), (HR,Ivy)
            // ORDER BY mean_salary DESC: Engineering(76333.33), HR(62500)
            assertEquals(2, arr(result).size());

            JsonValue eng = arr(result).get(0);
            assertEquals("Engineering", str(field(eng, "report", "department")));
            assertEquals("Grace", str(field(eng, "report", "manager")));
            assertEquals(3, num(field(eng, "report", "metrics", "headcnt")));
            assertEquals(76333.33, dbl(field(eng, "report", "metrics", "mean_salary")), 0.01);
            assertEquals(92000, num(field(eng, "report", "metrics", "max_salary")));
            assertEquals(62000, num(field(eng, "report", "metrics", "min_salary")));
            assertEquals(229000, num(field(eng, "report", "metrics", "total_cost")));

            JsonValue hr = arr(result).get(1);
            assertEquals("HR", str(field(hr, "report", "department")));
            assertEquals("Ivy", str(field(hr, "report", "manager")));
            assertEquals(2, num(field(hr, "report", "metrics", "headcnt")));
            assertEquals(62500.0, dbl(field(hr, "report", "metrics", "mean_salary")), 0.01);
            assertEquals(65000, num(field(hr, "report", "metrics", "max_salary")));
            assertEquals(60000, num(field(hr, "report", "metrics", "min_salary")));
        }

        /**
         * COALESCE and NULLIF in a nested query, NOT BETWEEN and NOT IN in the outer, GROUP BY with HAVING on aggregate
         * alias.
         *
         * <p>Features: nested query, COALESCE, NULLIF, NOT BETWEEN, NOT IN, GROUP BY, HAVING, COUNT, AVG, nested
         * aliases, ORDER BY
         */
        @Test
        void coalesce_nullif_notBetween_notIn_groupBy_having() {
            JsonValue result = query("""
                    SELECT
                        emp.region                              AS summary.region,
                        COUNT(*)                                AS summary.cnt,
                        AVG(emp.pay)                            AS summary.mean_pay
                    FROM (
                        SELECT
                            COALESCE(address.country, 'Unknown')    AS emp.region,
                            salary                                  AS emp.pay,
                            NULLIF(rating, 3.2)                     AS emp.adj_rating
                        FROM $r
                        WHERE age NOT BETWEEN 40 AND 50
                    )
                    WHERE emp.region NOT IN ('France')
                    GROUP BY emp.region
                    HAVING summary.cnt >= 2
                    ORDER BY summary.mean_pay DESC
                    """);

            // Inner: age NOT BETWEEN 40 AND 50 → excludes Charlie(42), Grace(45), Hank(50), Ivy(40)
            // Remaining: Alice(29,Turkey,75k), Bob(35,Turkey,92k), Diana(31,Germany,71k),
            //            Eve(26,Turkey,62k), Frank(38,France,65k), Jack(27,Germany,58k),
            //            Karen(33,Turkey,72k), Leo(29,Germany,60k)
            //
            // Outer WHERE: region NOT IN ('France') → excludes Frank
            //
            // GROUP BY region:
            //   Turkey:  Alice(75k), Bob(92k), Eve(62k), Karen(72k) → cnt=4, avg=75250
            //   Germany: Diana(71k), Jack(58k), Leo(60k)            → cnt=3, avg=63000
            //
            // ORDER BY mean_pay DESC: Turkey(75250), Germany(63000)
            assertEquals(2, arr(result).size());

            assertEquals("Turkey", str(field(arr(result).get(0), "summary", "region")));
            assertEquals(4, num(field(arr(result).get(0), "summary", "cnt")));
            assertEquals(75250.0, dbl(field(arr(result).get(0), "summary", "mean_pay")), 0.01);

            assertEquals("Germany", str(field(arr(result).get(1), "summary", "region")));
            assertEquals(3, num(field(arr(result).get(1), "summary", "cnt")));
            assertEquals(63000.0, dbl(field(arr(result).get(1), "summary", "mean_pay")), 1.0);
        }

        /**
         * Deeply nested output aliases producing a 4-level JSON structure. Nested query with LENGTH function, grouped
         * by a nested source field, HAVING, ORDER BY.
         *
         * <p>Features: 4-level deep nested output aliases (org.level.stats.X), nested source field access
         * (department.manager.level), LENGTH function, nested query, GROUP BY, HAVING, COUNT, SUM, MAX, ORDER BY deeply
         * nested alias
         */
        @Test
        void deeplyNestedOutputAliases_groupByNestedField_functions() {
            JsonValue result = query("""
                    SELECT
                        mgr_level                                       AS org.level.name,
                        COUNT(*)                                        AS org.level.stats.cnt,
                        SUM(emp_salary)                                 AS org.level.stats.budget,
                        MAX(name_len)                                   AS org.level.stats.longest_name
                    FROM (
                        SELECT
                            department.manager.level     AS mgr_level,
                            salary                       AS emp_salary,
                            LENGTH(name)                 AS name_len
                        FROM $r
                        WHERE rating >= 3.5
                    )
                    GROUP BY mgr_level
                    HAVING org.level.stats.cnt >= 2
                    ORDER BY org.level.stats.budget DESC
                    """);

            // rating >= 3.5 → excludes Jack(3.2). Remaining 11 with manager.level:
            //   Director: Alice(75k,len=5), Bob(92k,3), Eve(62k,3), Frank(65k,5), Leo(60k,3)
            //             → cnt=5, budget=354000, max_len=5
            //   VP:       Charlie(68k,7), Diana(71k,5), Karen(72k,5)
            //             → cnt=3, budget=211000, max_len=7
            //   CTO:      Grace(120k,5), Hank(110k,4), Ivy(95k,3)
            //             → cnt=3, budget=325000, max_len=5
            //
            // All pass HAVING (cnt >= 2)
            // ORDER BY budget DESC: Director(354000), CTO(325000), VP(211000)
            assertEquals(3, arr(result).size());

            JsonValue director = arr(result).get(0);
            assertEquals("Director", str(field(director, "org", "level", "name")));
            assertEquals(5, num(field(director, "org", "level", "stats", "cnt")));
            assertEquals(354000, num(field(director, "org", "level", "stats", "budget")));
            assertEquals(5, num(field(director, "org", "level", "stats", "longest_name")));

            JsonValue cto = arr(result).get(1);
            assertEquals("CTO", str(field(cto, "org", "level", "name")));
            assertEquals(3, num(field(cto, "org", "level", "stats", "cnt")));
            assertEquals(325000, num(field(cto, "org", "level", "stats", "budget")));

            JsonValue vp = arr(result).get(2);
            assertEquals("VP", str(field(vp, "org", "level", "name")));
            assertEquals(3, num(field(vp, "org", "level", "stats", "cnt")));
            assertEquals(211000, num(field(vp, "org", "level", "stats", "budget")));
            assertEquals(7, num(field(vp, "org", "level", "stats", "longest_name")));
        }

        // ── Nested function call integration tests ──────────────────────────

        @Test
        void avgNullif_skipsZerosInAggregation() {
            String data = """
                    [{"dept":"A","salary":100},{"dept":"A","salary":0},
                     {"dept":"A","salary":200},{"dept":"B","salary":50}]
                    """;
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT dept, AVG(NULLIF(salary, 0)) AS avg_sal FROM $r GROUP BY dept ORDER BY dept ASC", data);
            // Dept A: AVG of [100, NULL, 200] = 150
            assertEquals(150.0, dbl(field(arr(result).get(0), "avg_sal")), 0.01);
            assertEquals(50.0, dbl(field(arr(result).get(1), "avg_sal")), 0.01);
        }

        @Test
        void roundAvg_scalarWrappingAggregate() {
            String data = """
                    [{"dept":"X","val":10},{"dept":"X","val":20},{"dept":"X","val":30}]
                    """;
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT ROUND(AVG(val), 2) AS result FROM $r GROUP BY dept", data);
            assertEquals(20.0, dbl(field(arr(result).get(0), "result")), 0.01);
        }

        @Test
        void deeplyNestedScalarChain_lpadTrimNullif() {
            String data = """
                    [{"name":"  hi  "},{"name":""},{"name":"  world  "}]
                    """;
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT LPAD(TRIM(NULLIF(name, '')), 10, '*') AS padded FROM $r ORDER BY name ASC", data);
            // Row with "" → NULLIF returns NULL → TRIM(NULL) → LPAD(NULL) → NULL
            assertTrue(field(arr(result).get(0), "padded").isNull());
            assertEquals("********hi", str(field(arr(result).get(1), "padded")));
            assertEquals("*****world", str(field(arr(result).get(2), "padded")));
        }

        @Test
        void nestedFunctionsInWhere_trimNullif() {
            String data = """
                    [{"name":"  Alice  "},{"name":""},{"name":"  Bob  "}]
                    """;
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE TRIM(NULLIF(name, '')) = 'Alice'", data);
            assertEquals(1, arr(result).size());
            assertEquals("  Alice  ", str(field(arr(result).get(0), "name")));
        }

        @Test
        void nestedFunctionsInOrderBy_lengthTrim() {
            String data = """
                    [{"name":"  short  "},{"name":"  very long name  "},{"name":"  mid  "}]
                    """;
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT name FROM $r ORDER BY LENGTH(TRIM(name)) ASC", data);
            assertEquals("  mid  ", str(field(arr(result).get(0), "name")));
            assertEquals("  short  ", str(field(arr(result).get(1), "name")));
            assertEquals("  very long name  ", str(field(arr(result).get(2), "name")));
        }

        @Test
        void nestedFunctionsInHaving_roundAvg() {
            String data = """
                    [{"dept":"A","val":10},{"dept":"A","val":20},
                     {"dept":"B","val":100},{"dept":"B","val":200}]
                    """;
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT dept, AVG(val) AS mean FROM $r GROUP BY dept HAVING ROUND(AVG(val), 0) > 50", data);
            // Dept A: AVG=15, ROUND=15 → filtered. Dept B: AVG=150, ROUND=150 → passes
            assertEquals(1, arr(result).size());
            assertEquals("B", str(field(arr(result).get(0), "dept")));
        }

        @Test
        void roundAvgNullif_fullChainInGroupBy() {
            JsonValue result = query("""
                    SELECT
                        department.name                             AS dept,
                        ROUND(AVG(NULLIF(rating, 3.2)), 1)          AS adj_rating
                    FROM $r
                    GROUP BY department.name
                    ORDER BY department.name ASC
                    """);
            // Engineering: ratings [4.5, 3.8, 4.9, 4.3, 3.2→NULL] → AVG(4.5,3.8,4.9,4.3) = 4.375 → ROUND 4.4
            // HR: ratings [3.5, 4.0, 3.7] → no 3.2 → AVG = 3.733 → ROUND 3.7
            // Sales: ratings [4.1, 4.7, 3.9, 4.4] → AVG = 4.275 → ROUND 4.3
            assertEquals(3, arr(result).size());
            assertEquals(4.4, dbl(field(arr(result).get(0), "adj_rating")), 0.01);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // SmallFeaturesTests (from SmallFeaturesIntegrationTest.java)
    // ══════════════════════════════════════════════════════════════════════

    @Nested
    class SmallFeaturesTests {

        private static final String DATA = """
                [
                    {"name": "Alice",   "age": 25, "dept": "IT",   "salary": 50000, "created_at": "2024-01-15"},
                    {"name": "Bob",     "age": 30, "dept": "IT",   "salary": 60000, "created_at": "2024-03-20"},
                    {"name": "Charlie", "age": 35, "dept": "HR",   "salary": 55000, "created_at": "2024-06-01"},
                    {"name": "Diana",   "age": 28, "dept": "HR",   "salary": 52000, "created_at": "2024-09-10"},
                    {"name": "Eve",     "age": 25, "dept": "IT",   "salary": 48000, "created_at": "2024-12-01"},
                    {"name": "Frank",   "age": 40, "dept": "Sales","salary": 70000, "created_at": "2023-05-15"}
                ]
                """;

        private JsonValue query(String sql) {
            return SQL4Json.queryAsJsonValue(sql, DATA);
        }

        @Test
        void distinct_between_orderBy_limit() {
            // Pipeline order: WHERE -> ORDER BY -> LIMIT -> SELECT -> DISTINCT
            // BETWEEN 25 AND 35 yields: Alice(IT), Bob(IT), Charlie(HR), Diana(HR), Eve(IT) -- 5 rows
            // ORDER BY dept ASC: Charlie(HR), Diana(HR), Alice(IT), Bob(IT), Eve(IT)
            // LIMIT 2: Charlie(HR), Diana(HR)
            // DISTINCT on projected dept: just "HR"
            var result = query("SELECT DISTINCT dept FROM $r WHERE age BETWEEN 25 AND 35 ORDER BY dept ASC LIMIT 2");
            var rows = result.asArray().orElseThrow();
            assertEquals(1, rows.size());
            assertEquals(
                    "HR",
                    rows.get(0).asObject().orElseThrow().get("dept").asString().orElseThrow());
        }

        @Test
        void cast_upper_notIn() {
            var result = query("SELECT CAST(age AS STRING), UPPER(name) FROM $r WHERE dept NOT IN ('Sales', 'HR')");
            assertEquals(3, result.asArray().orElseThrow().size()); // Alice, Bob, Eve (IT dept)
        }

        @Test
        void limit_offset_orderBy() {
            var result = query("SELECT name, salary FROM $r ORDER BY name ASC LIMIT 3 OFFSET 1");
            var rows = result.asArray().orElseThrow();
            assertEquals(3, rows.size());
            // Offset 1 from alphabetical: Alice(0), Bob(1), Charlie(2), Diana(3) -> Bob, Charlie, Diana
            assertEquals(
                    "Bob",
                    rows.get(0).asObject().orElseThrow().get("name").asString().orElseThrow());
        }

        @Test
        void subquery_in_notLike_limit() {
            var result = query(
                    "SELECT * FROM (SELECT * FROM $r WHERE age IN (25, 30, 35)) WHERE name NOT LIKE '%e%' LIMIT 2");
            assertTrue(result.asArray().orElseThrow().size() <= 2);
        }

        @Test
        void between_withStringDates() {
            var result = query("SELECT * FROM $r WHERE created_at BETWEEN '2024-01-01' AND '2024-06-30'");
            // Alice(Jan), Bob(Mar), Charlie(Jun) = 3
            assertEquals(3, result.asArray().orElseThrow().size());
        }

        @Test
        void castInWhere() {
            var result = query("SELECT * FROM $r WHERE CAST(age AS STRING) = '25'");
            assertEquals(2, result.asArray().orElseThrow().size()); // Alice and Eve
        }

        @Test
        void in_combinedWithOr() {
            var result = query("SELECT * FROM $r WHERE dept IN ('IT') OR age > 35");
            // IT: Alice, Bob, Eve + age>35: Frank = 4
            assertEquals(4, result.asArray().orElseThrow().size());
        }

        @Test
        void distinct_withGroupBy() {
            var result = query("SELECT DISTINCT dept, COUNT(*) AS cnt FROM $r GROUP BY dept");
            assertEquals(3, result.asArray().orElseThrow().size()); // IT, HR, Sales
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // ComprehensiveE2ETests (from ComprehensiveE2ETest.java)
    // ══════════════════════════════════════════════════════════════════════

    @Nested
    class ComprehensiveE2ETests {

        // ── Shared data ─────────────────────────────────────────────────────

        static final String EMPLOYEES = """
                [
                  {"id": 1,  "name": "Alice",   "dept": "Engineering", "salary": 95000, "rating": 4.5, "hire_date": "2019-03-15", "active": true},
                  {"id": 2,  "name": "Bob",     "dept": "Engineering", "salary": 88000, "rating": 3.8, "hire_date": "2020-07-01", "active": true},
                  {"id": 3,  "name": "Charlie", "dept": "Marketing",   "salary": 72000, "rating": 4.2, "hire_date": "2018-01-10", "active": false},
                  {"id": 4,  "name": "Diana",   "dept": "Marketing",   "salary": 85000, "rating": 4.7, "hire_date": "2021-06-20", "active": true},
                  {"id": 5,  "name": "Eve",     "dept": "Engineering", "salary": 105000,"rating": 4.9, "hire_date": "2017-11-01", "active": true},
                  {"id": 6,  "name": "Frank",   "dept": "Sales",       "salary": 78000, "rating": 3.5, "hire_date": "2022-02-14", "active": true},
                  {"id": 7,  "name": "Grace",   "dept": "Sales",       "salary": 82000, "rating": 4.1, "hire_date": "2020-09-30", "active": true},
                  {"id": 8,  "name": "Hank",    "dept": "Engineering", "salary": 92000, "rating": 3.9, "hire_date": "2021-01-15", "active": true},
                  {"id": 9,  "name": "Ivy",     "dept": "Marketing",   "salary": 68000, "rating": 3.6, "hire_date": "2023-04-01", "active": true},
                  {"id": 10, "name": "Jack",    "dept": "Sales",       "salary": 75000, "rating": 4.0, "hire_date": "2019-08-22", "active": false}
                ]""";

        static final String ORDERS = """
                [
                  {"order_id": 101, "emp_id": 1, "amount": 5000,  "region": "North", "status": "completed"},
                  {"order_id": 102, "emp_id": 1, "amount": 3200,  "region": "South", "status": "completed"},
                  {"order_id": 103, "emp_id": 2, "amount": 4500,  "region": "North", "status": "pending"},
                  {"order_id": 104, "emp_id": 4, "amount": 7800,  "region": "East",  "status": "completed"},
                  {"order_id": 105, "emp_id": 6, "amount": 2100,  "region": "West",  "status": "completed"},
                  {"order_id": 106, "emp_id": 6, "amount": 6300,  "region": "North", "status": "completed"},
                  {"order_id": 107, "emp_id": 7, "amount": 4100,  "region": "South", "status": "pending"},
                  {"order_id": 108, "emp_id": 7, "amount": 5500,  "region": "East",  "status": "completed"},
                  {"order_id": 109, "emp_id": 10,"amount": 1800,  "region": "West",  "status": "completed"},
                  {"order_id": 110, "emp_id": 99,"amount": 900,   "region": "North", "status": "completed"}
                ]""";

        static final String DEPARTMENTS = """
                [
                  {"dept_name": "Engineering", "budget": 500000, "location": "Building A"},
                  {"dept_name": "Marketing",   "budget": 300000, "location": "Building B"},
                  {"dept_name": "Sales",       "budget": 250000, "location": "Building C"},
                  {"dept_name": "HR",          "budget": 150000, "location": "Building D"}
                ]""";

        // ── Helpers ──────────────────────────────────────────────────────────

        private static List<JsonValue> arr(JsonValue v) {
            return v.asArray().orElseThrow();
        }

        private static Map<String, JsonValue> obj(JsonValue v) {
            return v.asObject().orElseThrow();
        }

        private static String str(JsonValue v, String key) {
            return obj(v).get(key).asString().orElseThrow();
        }

        private static double dbl(JsonValue v, String key) {
            return obj(v).get(key).asNumber().orElseThrow().doubleValue();
        }

        private static int num(JsonValue v, String key) {
            return obj(v).get(key).asNumber().orElseThrow().intValue();
        }

        private static boolean isNull(JsonValue v, String key) {
            return obj(v).get(key).isNull();
        }

        // ── Tests ───────────────────────────────────────────────────────────

        /**
         * JOIN + GROUP BY + window RANK on aggregated result + WHERE + ORDER BY + LIMIT. Joins employees to orders,
         * filters completed orders, groups by employee, ranks by total sales, returns top 3.
         *
         * <p>Pipeline: JOIN → WHERE → GROUP BY → WINDOW (RANK on alias) → ORDER BY → LIMIT → SELECT
         *
         * <p>Features: INNER JOIN, WHERE, GROUP BY, SUM aggregate, window RANK (references alias), ORDER BY window
         * result, LIMIT
         */
        @Test
        void join_window_rank_where_orderBy_limit() {
            String sql = """
                    SELECT e.name AS name, e.dept AS dept,
                        SUM(o.amount) AS total_sales,
                        RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
                    FROM employees e
                    JOIN orders o ON e.id = o.emp_id
                    WHERE o.status = 'completed'
                    GROUP BY e.name, e.dept
                    ORDER BY sales_rank
                    LIMIT 3""";

            JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("employees", EMPLOYEES, "orders", ORDERS));

            // Completed orders per employee:
            //   Alice: 5000+3200=8200, Frank: 2100+6300=8400, Grace: 5500,
            //   Diana: 7800, Jack: 1800
            // Sorted by total DESC: Frank(8400), Alice(8200), Diana(7800), Grace(5500), Jack(1800)
            // Rank: Frank=1, Alice=2, Diana=3, Grace=4, Jack=5
            // LIMIT 3 → Frank, Alice, Diana
            var rows = arr(result);
            assertEquals(3, rows.size());

            assertEquals("Frank", str(rows.get(0), "name"));
            assertEquals(1, num(rows.get(0), "sales_rank"));
            assertEquals(8400.0, dbl(rows.get(0), "total_sales"));

            assertEquals("Alice", str(rows.get(1), "name"));
            assertEquals(2, num(rows.get(1), "sales_rank"));

            assertEquals("Diana", str(rows.get(2), "name"));
            assertEquals(3, num(rows.get(2), "sales_rank"));
        }

        /**
         * Window functions with PARTITION BY + JOIN + aggregate windows + WHERE. Computes per-department salary stats
         * alongside each employee's own data.
         *
         * <p>Features: window SUM/COUNT/AVG OVER PARTITION BY, WHERE, ORDER BY
         */
        @Test
        void window_aggregate_partitioned_with_where() {
            String sql = """
                    SELECT name, dept, salary,
                        SUM(salary)   OVER (PARTITION BY dept) AS dept_total,
                        COUNT(*)      OVER (PARTITION BY dept) AS dept_size,
                        AVG(salary)   OVER (PARTITION BY dept) AS dept_avg
                    FROM $r
                    WHERE active = true
                    ORDER BY dept, salary DESC""";

            JsonValue result = SQL4Json.queryAsJsonValue(sql, EMPLOYEES);

            // Active employees:
            //   Engineering: Alice(95k), Bob(88k), Eve(105k), Hank(92k) → total=380000, size=4, avg=95000
            //   Marketing:   Diana(85k), Ivy(68k)                       → total=153000, size=2, avg=76500
            //   Sales:       Frank(78k), Grace(82k)                     → total=160000, size=2, avg=80000
            var rows = arr(result);
            assertEquals(8, rows.size()); // 8 active employees, no row collapse

            // First row: Engineering ordered by salary DESC → Eve(105k)
            assertEquals("Eve", str(rows.get(0), "name"));
            assertEquals(380000.0, dbl(rows.get(0), "dept_total"));
            assertEquals(4, num(rows.get(0), "dept_size"));
            assertEquals(95000.0, dbl(rows.get(0), "dept_avg"));
        }

        /**
         * LEFT JOIN + GROUP BY + window ROW_NUMBER + PARTITION BY + NULL handling. Shows all employees with their order
         * counts, numbered within department.
         *
         * <p>Pipeline: LEFT JOIN → GROUP BY → WINDOW (ROW_NUMBER partitioned by dept) → ORDER BY → SELECT
         *
         * <p>Features: LEFT JOIN, COUNT aggregate, GROUP BY, window ROW_NUMBER OVER PARTITION BY ORDER BY, NULL from
         * unmatched join
         */
        @Test
        void leftJoin_window_rowNumber_nullHandling() {
            String sql = """
                    SELECT e.name AS name, e.dept AS dept,
                        COUNT(o.order_id) AS order_count,
                        ROW_NUMBER() OVER (PARTITION BY e.dept ORDER BY order_count DESC) AS dept_rank
                    FROM employees e
                    LEFT JOIN orders o ON e.id = o.emp_id
                    GROUP BY e.name, e.dept
                    ORDER BY dept, dept_rank""";

            JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("employees", EMPLOYEES, "orders", ORDERS));

            // All 10 employees appear (LEFT JOIN). Order counts:
            //   Alice:2, Bob:1, Charlie:0, Diana:1, Eve:0, Frank:2, Grace:2, Hank:0, Ivy:0, Jack:1
            var rows = arr(result);
            assertEquals(10, rows.size());

            // Engineering dept_rank=1 should have highest order count (Alice with 2)
            var engFirst = rows.stream()
                    .filter(r -> "Engineering".equals(str(r, "dept")) && num(r, "dept_rank") == 1)
                    .findFirst()
                    .orElseThrow();
            assertEquals("Alice", str(engFirst, "name"));
            assertEquals(2, num(engFirst, "order_count"));
        }

        /**
         * Three-way chained JOIN + window DENSE_RANK + WHERE + ORDER BY. Joins employees → orders → departments, ranks
         * by order amount within location.
         *
         * <p>Features: chained JOIN (3 tables), WHERE, window DENSE_RANK, PARTITION BY, ORDER BY
         */
        @Test
        void chainedJoin_window_denseRank() {
            String sql = """
                    SELECT e.name AS emp, d.location AS location, o.amount AS amount,
                        DENSE_RANK() OVER (PARTITION BY d.location ORDER BY o.amount DESC) AS loc_rank
                    FROM employees e
                    JOIN orders o ON e.id = o.emp_id
                    JOIN departments d ON e.dept = d.dept_name
                    WHERE o.status = 'completed'
                    ORDER BY location, loc_rank""";

            JsonValue result = SQL4Json.queryAsJsonValue(
                    sql, Map.of("employees", EMPLOYEES, "orders", ORDERS, "departments", DEPARTMENTS));

            var rows = arr(result);
            assertTrue(rows.size() > 0);

            // Building A (Engineering): Alice 5000, Alice 3200 → ranks 1, 2
            // Building B (Marketing): Diana 7800 → rank 1
            // Building C (Sales): Frank 6300, Grace 5500, Frank 2100, Jack 1800 → ranks 1,2,3,4
            var buildingAFirst = rows.stream()
                    .filter(r -> "Building A".equals(str(r, "location")) && num(r, "loc_rank") == 1)
                    .findFirst()
                    .orElseThrow();
            assertEquals(5000.0, dbl(buildingAFirst, "amount"));
        }

        /**
         * Window LAG/LEAD + scalar functions + WHERE + ORDER BY. Shows salary progression — previous and next salary by
         * hire date.
         *
         * <p>Features: window LAG, window LEAD, UPPER scalar function, WHERE, ORDER BY
         */
        @Test
        void window_lag_lead_scalarFunctions_where() {
            String sql = """
                    SELECT UPPER(name) AS emp_name, salary,
                        LAG(salary)  OVER (ORDER BY hire_date ASC) AS prev_salary,
                        LEAD(salary) OVER (ORDER BY hire_date ASC) AS next_salary
                    FROM $r
                    WHERE active = true
                    ORDER BY hire_date ASC""";

            JsonValue result = SQL4Json.queryAsJsonValue(sql, EMPLOYEES);

            // Active employees by hire_date ASC:
            //   Eve(2017-11-01, 105k), Alice(2019-03-15, 95k), Bob(2020-07-01, 88k),
            //   Grace(2020-09-30, 82k), Hank(2021-01-15, 92k), Diana(2021-06-20, 85k),
            //   Frank(2022-02-14, 78k), Ivy(2023-04-01, 68k)
            var rows = arr(result);
            assertEquals(8, rows.size());

            // First row: Eve — no previous, next is Alice's salary
            assertEquals("EVE", str(rows.get(0), "emp_name"));
            assertTrue(isNull(rows.get(0), "prev_salary"));
            assertEquals(95000.0, dbl(rows.get(0), "next_salary"));

            // Last row: Ivy — previous is Frank's salary, no next
            assertEquals("IVY", str(rows.get(7), "emp_name"));
            assertEquals(78000.0, dbl(rows.get(7), "prev_salary"));
            assertTrue(isNull(rows.get(7), "next_salary"));
        }

        /**
         * Window NTILE + DISTINCT + subquery + LIMIT. Divides active employees into salary quartiles, then selects
         * distinct quartiles.
         *
         * <p>Features: window NTILE, DISTINCT, subquery, ORDER BY, LIMIT
         */
        @Test
        void window_ntile_distinct_subquery() {
            String sql = """
                    SELECT DISTINCT quartile
                    FROM (
                        SELECT name, salary,
                            NTILE(4) OVER (ORDER BY salary DESC) AS quartile
                        FROM $r
                        WHERE active = true
                    )
                    ORDER BY quartile""";

            JsonValue result = SQL4Json.queryAsJsonValue(sql, EMPLOYEES);

            // 8 active employees into 4 quartiles → each quartile gets 2 employees
            // DISTINCT quartile values: 1, 2, 3, 4
            var rows = arr(result);
            assertEquals(4, rows.size());
            assertEquals(1, num(rows.get(0), "quartile"));
            assertEquals(2, num(rows.get(1), "quartile"));
            assertEquals(3, num(rows.get(2), "quartile"));
            assertEquals(4, num(rows.get(3), "quartile"));
        }

        /**
         * Multiple window functions with different specs + JOIN + WHERE. Ranks employees globally and within
         * department, shows department average.
         *
         * <p>Features: multiple window functions (3 different OVER specs), INNER JOIN, WHERE, ORDER BY, LIMIT
         */
        @Test
        void multipleWindowSpecs_join_where_orderBy() {
            String sql = """
                    SELECT e.name AS name, d.location AS location, e.salary AS salary,
                        RANK()      OVER (ORDER BY e.salary DESC)                                AS global_rank,
                        ROW_NUMBER() OVER (PARTITION BY e.dept ORDER BY e.salary DESC)            AS dept_rank,
                        AVG(e.salary) OVER (PARTITION BY e.dept)                                  AS dept_avg
                    FROM employees e
                    JOIN departments d ON e.dept = d.dept_name
                    WHERE e.active = true
                    ORDER BY global_rank
                    LIMIT 5""";

            JsonValue result =
                    SQL4Json.queryAsJsonValue(sql, Map.of("employees", EMPLOYEES, "departments", DEPARTMENTS));

            // Active employees sorted by salary DESC:
            //   Eve(105k,Eng), Alice(95k,Eng), Hank(92k,Eng), Bob(88k,Eng),
            //   Diana(85k,Mkt), Grace(82k,Sales), Frank(78k,Sales), Ivy(68k,Mkt)
            // Top 5 by global rank
            var rows = arr(result);
            assertEquals(5, rows.size());

            // Eve is #1 globally, #1 in Engineering
            assertEquals("Eve", str(rows.get(0), "name"));
            assertEquals("Building A", str(rows.get(0), "location"));
            assertEquals(1, num(rows.get(0), "global_rank"));
            assertEquals(1, num(rows.get(0), "dept_rank"));

            // Alice is #2 globally, #2 in Engineering
            assertEquals("Alice", str(rows.get(1), "name"));
            assertEquals(2, num(rows.get(1), "global_rank"));
            assertEquals(2, num(rows.get(1), "dept_rank"));
        }

        /**
         * JOIN + GROUP BY + HAVING + window RANK on aggregated result + ORDER BY. Groups orders by employee, filters
         * high-value sellers, ranks them.
         *
         * <p>Pipeline: JOIN → GROUP BY → HAVING → WINDOW (RANK on alias) → ORDER BY → SELECT
         *
         * <p>Features: INNER JOIN, GROUP BY, SUM/COUNT aggregates, HAVING, window RANK (references alias), ORDER BY
         */
        @Test
        void join_groupBy_having_windowRank() {
            String sql = """
                    SELECT e.name AS name, e.dept AS dept,
                        SUM(o.amount) AS total_sales,
                        COUNT(*) AS order_count,
                        RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
                    FROM employees e
                    JOIN orders o ON e.id = o.emp_id
                    GROUP BY e.name, e.dept
                    HAVING total_sales >= 4000
                    ORDER BY sales_rank""";

            JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("employees", EMPLOYEES, "orders", ORDERS));

            // All orders per employee (no status filter):
            //   Alice: 5000+3200=8200 (2 orders), Bob: 4500 (1), Diana: 7800 (1),
            //   Frank: 2100+6300=8400 (2), Grace: 4100+5500=9600 (2), Jack: 1800 (1)
            // HAVING total_sales >= 4000: Alice(8200), Bob(4500), Diana(7800), Frank(8400), Grace(9600)
            // Ranked: Grace(1), Frank(2), Alice(3), Diana(4), Bob(5)
            var rows = arr(result);
            assertEquals(5, rows.size());

            assertEquals("Grace", str(rows.get(0), "name"));
            assertEquals(1, num(rows.get(0), "sales_rank"));
            assertEquals(9600.0, dbl(rows.get(0), "total_sales"));

            assertEquals("Frank", str(rows.get(1), "name"));
            assertEquals(2, num(rows.get(1), "sales_rank"));

            assertEquals("Bob", str(rows.get(4), "name"));
            assertEquals(5, num(rows.get(4), "sales_rank"));
        }

        /**
         * Window MIN/MAX OVER + WHERE with IN + ORDER BY + OFFSET. Shows each employee's salary relative to department
         * min/max.
         *
         * <p>Features: window MIN, window MAX, WHERE IN, ORDER BY, LIMIT, OFFSET
         */
        @Test
        void window_minMax_whereIn_orderBy_offset() {
            String sql = """
                    SELECT name, dept, salary,
                        MIN(salary) OVER (PARTITION BY dept) AS dept_min,
                        MAX(salary) OVER (PARTITION BY dept) AS dept_max
                    FROM $r
                    WHERE dept IN ('Engineering', 'Sales')
                    ORDER BY salary DESC
                    LIMIT 3
                    OFFSET 1""";

            JsonValue result = SQL4Json.queryAsJsonValue(sql, EMPLOYEES);

            // Engineering: Alice(95k), Bob(88k), Eve(105k), Hank(92k) → min=88k, max=105k
            // Sales: Frank(78k), Grace(82k), Jack(75k) → min=75k, max=82k
            // All 7 sorted by salary DESC: Eve(105k), Alice(95k), Hank(92k), Bob(88k), Grace(82k), Frank(78k),
            // Jack(75k)
            // OFFSET 1 LIMIT 3 → Alice, Hank, Bob
            var rows = arr(result);
            assertEquals(3, rows.size());

            assertEquals("Alice", str(rows.get(0), "name"));
            assertEquals(88000.0, dbl(rows.get(0), "dept_min")); // Engineering min
            assertEquals(105000.0, dbl(rows.get(0), "dept_max")); // Engineering max

            assertEquals("Hank", str(rows.get(1), "name"));
            assertEquals("Bob", str(rows.get(2), "name"));
        }

        /**
         * Window function + LAG with custom offset + BETWEEN filter. Shows salary 2 positions back in the hire-date
         * ordering.
         *
         * <p>Features: window LAG with offset=2, WHERE BETWEEN, ORDER BY
         */
        @Test
        void window_lagCustomOffset_between() {
            String sql = """
                    SELECT name, salary, hire_date,
                        LAG(salary, 2) OVER (ORDER BY hire_date ASC) AS salary_two_hires_ago
                    FROM $r
                    WHERE salary BETWEEN 70000 AND 100000
                    ORDER BY hire_date ASC""";

            JsonValue result = SQL4Json.queryAsJsonValue(sql, EMPLOYEES);

            // salary BETWEEN 70000 AND 100000:
            //   Alice(95k, 2019-03-15), Bob(88k, 2020-07-01), Charlie(72k, 2018-01-10),
            //   Diana(85k, 2021-06-20), Frank(78k, 2022-02-14), Grace(82k, 2020-09-30),
            //   Hank(92k, 2021-01-15), Jack(75k, 2019-08-22)
            // Sorted by hire_date ASC:
            //   Charlie(72k), Alice(95k), Jack(75k), Bob(88k), Grace(82k), Hank(92k), Diana(85k), Frank(78k)
            // LAG(salary, 2):
            //   Charlie→null, Alice→null, Jack→72000, Bob→95000, Grace→75000,
            //   Hank→88000, Diana→82000, Frank→92000
            var rows = arr(result);
            assertEquals(8, rows.size());

            // First two have null (no 2 rows behind)
            assertTrue(isNull(rows.get(0), "salary_two_hires_ago"));
            assertTrue(isNull(rows.get(1), "salary_two_hires_ago"));

            // Third row (Jack) looks back 2 → Charlie's salary
            assertEquals(72000.0, dbl(rows.get(2), "salary_two_hires_ago"));
        }

        /**
         * Engine API + PreparedQuery with window functions and JOINs. Validates that window functions work through all
         * public API surfaces.
         *
         * <p>Features: Engine API with named sources, JOIN, GROUP BY, window RANK, WHERE, ORDER BY, LIMIT,
         * PreparedQuery with window functions
         */
        @Test
        void engine_and_preparedQuery_with_window_and_join() {
            // Engine API with named sources — JOIN + GROUP BY + window in single level
            SQL4JsonEngine engine = SQL4Json.engine()
                    .data("employees", EMPLOYEES)
                    .data("orders", ORDERS)
                    .build();

            String engineResult = engine.query("""
                    SELECT e.name AS name,
                        SUM(o.amount) AS total,
                        RANK() OVER (ORDER BY total DESC) AS rnk
                    FROM employees e
                    JOIN orders o ON e.id = o.emp_id
                    WHERE o.status = 'completed'
                    GROUP BY e.name
                    ORDER BY rnk
                    LIMIT 2""");

            // Completed totals: Alice(8200), Diana(7800), Frank(8400), Grace(5500), Jack(1800)
            // Top 2: Frank(8400, rank 1), Alice(8200, rank 2)
            assertTrue(engineResult.contains("\"name\":\"Frank\""));
            assertTrue(engineResult.contains("\"rnk\":1"));

            // PreparedQuery API with window functions (single source).
            // The outer ORDER BY rnk is required — a window's OVER (ORDER BY ...) only
            // governs ranking, not the final output ordering, so without it the LIMIT 3
            // could return any 3 of the matched rows in any order.
            PreparedQuery pq = SQL4Json.prepare("""
                    SELECT name, salary,
                        ROW_NUMBER() OVER (ORDER BY salary DESC) AS rnk
                    FROM $r
                    WHERE active = true
                    ORDER BY rnk
                    LIMIT 3""");

            String pqResult = pq.execute(EMPLOYEES);
            assertTrue(pqResult.contains("\"rnk\":1"));
            assertTrue(pqResult.contains("\"rnk\":2"));
            assertTrue(pqResult.contains("\"rnk\":3"));
        }

        /**
         * The kitchen sink: 3-table chained JOIN + WHERE + GROUP BY + HAVING + window RANK + ROUND scalar + ORDER BY +
         * LIMIT — all in one query level.
         *
         * <p>Scenario: Find departments where total completed sales exceed 5000, rank them, show location from
         * departments table.
         *
         * <p>Pipeline: 3-way JOIN → WHERE → GROUP BY → HAVING → WINDOW (RANK on alias) → ORDER BY → LIMIT → SELECT
         *
         * <p>Features: 3-table chained JOIN, WHERE, GROUP BY, SUM/COUNT/AVG aggregates, HAVING, window RANK (references
         * alias), ROUND scalar wrapping aggregate, ORDER BY, LIMIT
         */
        @Test
        void kitchenSink_chainedJoin_groupBy_having_window_scalar_orderBy_limit() {
            String sql = """
                    SELECT e.dept AS dept, d.location AS location,
                        SUM(o.amount)               AS total_sales,
                        COUNT(*)                    AS order_count,
                        ROUND(AVG(o.amount), 2)     AS avg_sale,
                        RANK() OVER (ORDER BY total_sales DESC) AS dept_rank
                    FROM employees e
                    JOIN orders o ON e.id = o.emp_id
                    JOIN departments d ON e.dept = d.dept_name
                    WHERE o.status = 'completed'
                    GROUP BY e.dept, d.location
                    HAVING total_sales > 5000
                    ORDER BY dept_rank""";

            JsonValue result = SQL4Json.queryAsJsonValue(
                    sql, Map.of("employees", EMPLOYEES, "orders", ORDERS, "departments", DEPARTMENTS));

            // Completed orders by dept:
            //   Engineering (Building A): Alice(5000+3200)=8200 → total=8200, cnt=2, avg=4100
            //   Marketing (Building B):   Diana(7800) → total=7800, cnt=1, avg=7800
            //   Sales (Building C):       Frank(2100+6300)+Grace(5500)+Jack(1800)=15700 → total=15700, cnt=4, avg=3925
            // HAVING total > 5000: all 3 pass
            // Ranked: Sales(15700)=1, Engineering(8200)=2, Marketing(7800)=3
            var rows = arr(result);
            assertEquals(3, rows.size());

            assertEquals("Sales", str(rows.get(0), "dept"));
            assertEquals("Building C", str(rows.get(0), "location"));
            assertEquals(1, num(rows.get(0), "dept_rank"));
            assertEquals(15700.0, dbl(rows.get(0), "total_sales"));
            assertEquals(4, num(rows.get(0), "order_count"));
            assertEquals(3925.0, dbl(rows.get(0), "avg_sale"));

            assertEquals("Engineering", str(rows.get(1), "dept"));
            assertEquals(2, num(rows.get(1), "dept_rank"));

            assertEquals("Marketing", str(rows.get(2), "dept"));
            assertEquals(3, num(rows.get(2), "dept_rank"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // FunctionCoverageTests (from FunctionCoverageTest.java)
    // ══════════════════════════════════════════════════════════════════════

    @Nested
    class FunctionCoverageTests {

        private static final String DATA = """
                [{"name":"  Alice  ","age":30,"salary":75000.50,"dept":"IT","score":-5,"zero":0}]
                """;

        private JsonValue query(String sql) {
            return SQL4Json.queryAsJsonValue(sql, DATA);
        }

        private JsonValue first(String sql) {
            return query(sql)
                    .asArray()
                    .orElseThrow()
                    .getFirst()
                    .asObject()
                    .orElseThrow()
                    .values()
                    .iterator()
                    .next();
        }

        // ── String Functions ────────────────────────────────────────────

        @Test
        void concat_two_strings() {
            var r = first("SELECT CONCAT(name, dept) AS v FROM $r");
            assertEquals("  Alice  IT", r.asString().orElseThrow());
        }

        @Test
        void concat_with_literal() {
            var r = first("SELECT CONCAT(name, ' - ', dept) AS v FROM $r");
            assertEquals("  Alice   - IT", r.asString().orElseThrow());
        }

        @Test
        void substring_with_start_and_length() {
            var r = first("SELECT SUBSTRING(TRIM(name), 1, 3) AS v FROM $r");
            assertEquals("Ali", r.asString().orElseThrow());
        }

        @Test
        void substring_with_start_only() {
            var r = first("SELECT SUBSTRING(TRIM(name), 4) AS v FROM $r");
            assertEquals("ce", r.asString().orElseThrow());
        }

        @Test
        void trim_removes_whitespace() {
            var r = first("SELECT TRIM(name) AS v FROM $r");
            assertEquals("Alice", r.asString().orElseThrow());
        }

        @Test
        void length_of_string() {
            var r = first("SELECT LENGTH(TRIM(name)) AS v FROM $r");
            assertEquals(5, r.asNumber().orElseThrow().intValue());
        }

        @Test
        void replace_string() {
            var r = first("SELECT REPLACE(TRIM(name), 'li', 'LI') AS v FROM $r");
            assertEquals("ALIce", r.asString().orElseThrow());
        }

        @Test
        void left_function() {
            var r = first("SELECT LEFT(TRIM(name), 3) AS v FROM $r");
            assertEquals("Ali", r.asString().orElseThrow());
        }

        @Test
        void right_function() {
            var r = first("SELECT RIGHT(TRIM(name), 3) AS v FROM $r");
            assertEquals("ice", r.asString().orElseThrow());
        }

        @Test
        void lpad_function() {
            var r = first("SELECT LPAD(dept, 5, '*') AS v FROM $r");
            assertEquals("***IT", r.asString().orElseThrow());
        }

        @Test
        void rpad_function() {
            var r = first("SELECT RPAD(dept, 5, '*') AS v FROM $r");
            assertEquals("IT***", r.asString().orElseThrow());
        }

        @Test
        void reverse_function() {
            var r = first("SELECT REVERSE(dept) AS v FROM $r");
            assertEquals("TI", r.asString().orElseThrow());
        }

        @Test
        void position_function_found() {
            var r = first("SELECT POSITION('li', TRIM(name)) AS v FROM $r");
            assertEquals(2, r.asNumber().orElseThrow().intValue());
        }

        @Test
        void position_function_not_found() {
            var r = first("SELECT POSITION('xyz', TRIM(name)) AS v FROM $r");
            assertEquals(0, r.asNumber().orElseThrow().intValue());
        }

        // ── Math Functions ──────────────────────────────────────────────

        @Test
        void abs_of_negative() {
            var r = first("SELECT ABS(score) AS v FROM $r");
            assertEquals(5, r.asNumber().orElseThrow().intValue());
        }

        @Test
        void round_with_decimals() {
            var r = first("SELECT ROUND(salary, 0) AS v FROM $r");
            assertNotNull(r.asNumber().orElseThrow());
        }

        @Test
        void round_without_decimals() {
            var r = first("SELECT ROUND(salary) AS v FROM $r");
            assertNotNull(r.asNumber().orElseThrow());
        }

        @Test
        void ceil_function() {
            var r = first("SELECT CEIL(salary) AS v FROM $r");
            assertEquals(75001, r.asNumber().orElseThrow().longValue());
        }

        @Test
        void floor_function() {
            var r = first("SELECT FLOOR(salary) AS v FROM $r");
            assertEquals(75000, r.asNumber().orElseThrow().longValue());
        }

        @Test
        void mod_function() {
            var r = first("SELECT MOD(age, 7) AS v FROM $r");
            assertEquals(2, r.asNumber().orElseThrow().intValue());
        }

        @Test
        void mod_by_zero_returns_null() {
            var r = first("SELECT MOD(age, zero) AS v FROM $r");
            assertTrue(r.isNull());
        }

        @Test
        void power_function() {
            var r = first("SELECT POWER(age, 2) AS v FROM $r");
            assertEquals(900.0, r.asNumber().orElseThrow().doubleValue());
        }

        @Test
        void sqrt_function() {
            var r = first("SELECT SQRT(age) AS v FROM $r");
            double val = r.asNumber().orElseThrow().doubleValue();
            assertTrue(val > 5.47 && val < 5.48);
        }

        @Test
        void sqrt_of_negative_returns_null() {
            var r = first("SELECT SQRT(score) AS v FROM $r");
            assertTrue(r.isNull());
        }

        @Test
        void sign_positive() {
            var r = first("SELECT SIGN(age) AS v FROM $r");
            assertEquals(1, r.asNumber().orElseThrow().intValue());
        }

        @Test
        void sign_negative() {
            var r = first("SELECT SIGN(score) AS v FROM $r");
            assertEquals(-1, r.asNumber().orElseThrow().intValue());
        }

        @Test
        void sign_zero() {
            var r = first("SELECT SIGN(zero) AS v FROM $r");
            assertEquals(0, r.asNumber().orElseThrow().intValue());
        }

        // ── Locale-based functions ──────────────────────────────────────

        @Test
        void lower_with_locale() {
            String data = "[{\"name\":\"ISTANBUL\"}]";
            var r = SQL4Json.queryAsJsonValue("SELECT LOWER(name, 'tr-TR') AS v FROM $r", data)
                    .asArray()
                    .orElseThrow()
                    .getFirst()
                    .asObject()
                    .orElseThrow()
                    .get("v");
            // Turkish lowercase of I is \u0131 (dotless i)
            assertTrue(r.asString().orElseThrow().contains("\u0131"));
        }

        @Test
        void upper_with_locale() {
            String data = "[{\"name\":\"istanbul\"}]";
            var r = SQL4Json.queryAsJsonValue("SELECT UPPER(name, 'tr-TR') AS v FROM $r", data)
                    .asArray()
                    .orElseThrow()
                    .getFirst()
                    .asObject()
                    .orElseThrow()
                    .get("v");
            // Turkish uppercase of i is \u0130 (dotted I)
            assertTrue(r.asString().orElseThrow().contains("\u0130"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // StreamingTests (from StreamingIntegrationTest.java)
    // ══════════════════════════════════════════════════════════════════════

    @Nested
    class StreamingTests {

        private static final String EMPLOYEES = """
                [
                  {"name":"Alice","age":30,"dept":"Engineering","salary":90000},
                  {"name":"Bob","age":25,"dept":"Marketing","salary":60000},
                  {"name":"Carol","age":35,"dept":"Engineering","salary":110000},
                  {"name":"Dave","age":28,"dept":"Marketing","salary":65000},
                  {"name":"Eve","age":40,"dept":"Engineering","salary":120000}
                ]""";

        /**
         * Helper: run a query via tree path (parse to JsonValue first) and via the default string-in API (now
         * streaming). Results must match.
         */
        private void assertStreamingMatchesTree(String sql, String json) {
            // Tree path: parse -> execute -> serialize
            JsonValue data = JsonParser.parse(json);
            JsonValue treeResult = SQL4Json.queryAsJsonValue(sql, data);
            String treeString = JsonSerializer.serialize(treeResult);

            // Streaming path (default API)
            String streamString = SQL4Json.query(sql, json);

            assertEquals(treeString, streamString, "Streaming result differs from tree result for query: " + sql);
        }

        @Test
        void select_all() {
            assertStreamingMatchesTree("SELECT * FROM $r", EMPLOYEES);
        }

        @Test
        void select_columns() {
            assertStreamingMatchesTree("SELECT name, age FROM $r", EMPLOYEES);
        }

        @Test
        void where_filter() {
            assertStreamingMatchesTree("SELECT * FROM $r WHERE age > 30", EMPLOYEES);
        }

        @Test
        void where_with_limit() {
            assertStreamingMatchesTree("SELECT * FROM $r WHERE age > 25 LIMIT 2", EMPLOYEES);
        }

        @Test
        void where_with_offset_and_limit() {
            assertStreamingMatchesTree("SELECT * FROM $r WHERE age > 25 LIMIT 2 OFFSET 1", EMPLOYEES);
        }

        @Test
        void order_by() {
            assertStreamingMatchesTree("SELECT * FROM $r ORDER BY age DESC", EMPLOYEES);
        }

        @Test
        void order_by_with_limit() {
            assertStreamingMatchesTree("SELECT * FROM $r ORDER BY salary DESC LIMIT 3", EMPLOYEES);
        }

        @Test
        void group_by() {
            assertStreamingMatchesTree(
                    "SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM $r GROUP BY dept ORDER BY dept",
                    EMPLOYEES);
        }

        @Test
        void group_by_having() {
            assertStreamingMatchesTree(
                    "SELECT dept, AVG(salary) AS avg_sal FROM $r GROUP BY dept HAVING avg_sal > 70000", EMPLOYEES);
        }

        @Test
        void distinct() {
            assertStreamingMatchesTree("SELECT DISTINCT dept FROM $r", EMPLOYEES);
        }

        @Test
        void subquery() {
            assertStreamingMatchesTree("SELECT name FROM (SELECT * FROM $r WHERE age > 30)", EMPLOYEES);
        }

        @Test
        void nested_path() {
            String json = "{\"data\":{\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}]}}";
            assertStreamingMatchesTree("SELECT * FROM $r.data.items WHERE id > 1", json);
        }

        @Test
        void scalar_functions() {
            assertStreamingMatchesTree("SELECT UPPER(name) AS upper_name, LENGTH(name) AS name_len FROM $r", EMPLOYEES);
        }

        @Test
        void like_filter() {
            assertStreamingMatchesTree("SELECT * FROM $r WHERE name LIKE 'A%'", EMPLOYEES);
        }

        @Test
        void in_filter() {
            assertStreamingMatchesTree("SELECT * FROM $r WHERE dept IN ('Engineering')", EMPLOYEES);
        }

        @Test
        void between_filter() {
            assertStreamingMatchesTree("SELECT * FROM $r WHERE age BETWEEN 28 AND 35", EMPLOYEES);
        }

        @Test
        void is_null_filter() {
            String json = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":null}]";
            assertStreamingMatchesTree("SELECT * FROM $r WHERE age IS NOT NULL", json);
        }

        @Test
        void preparedQuery_uses_streaming() {
            PreparedQuery prepared = SQL4Json.prepare("SELECT * FROM $r WHERE age > 30");
            String result = prepared.execute(EMPLOYEES);

            String expected = SQL4Json.query("SELECT * FROM $r WHERE age > 30", EMPLOYEES);
            assertEquals(expected, result);
        }

        @Test
        void preparedQuery_reuse() {
            PreparedQuery prepared = SQL4Json.prepare("SELECT name FROM $r WHERE age > 25");
            String r1 = prepared.execute(EMPLOYEES);
            String r2 = prepared.execute("[{\"name\":\"Zoe\",\"age\":50}]");

            assertTrue(r1.contains("Alice"));
            assertTrue(r2.contains("Zoe"));
        }

        @Test
        void custom_codec_uses_tree_path() {
            JsonCodec custom = new JsonCodec() {
                private final DefaultJsonCodec inner = new DefaultJsonCodec();

                @Override
                public JsonValue parse(String json) {
                    return inner.parse(json);
                }

                @Override
                public String serialize(JsonValue value) {
                    return inner.serialize(value);
                }
            };

            var settings = Sql4jsonSettings.builder().codec(custom).build();
            String result = SQL4Json.query("SELECT * FROM $r WHERE age > 30", EMPLOYEES, settings);
            assertTrue(result.contains("Carol"));
            assertTrue(result.contains("Eve"));
            assertFalse(result.contains("Bob"));
        }

        @Test
        void queryAsJsonValue_streaming_path() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE age > 30", EMPLOYEES);
            assertTrue(result.isArray());
            assertEquals(2, result.asArray().orElseThrow().size());
        }

        @Test
        void object_root_streaming() {
            String json = "{\"name\":\"Alice\",\"age\":30}";
            assertStreamingMatchesTree("SELECT * FROM $r", json);
        }

        @Test
        void empty_array_streaming() {
            assertStreamingMatchesTree("SELECT * FROM $r", "[]");
        }

        @Test
        void nested_objects_in_elements() {
            String json = "[{\"user\":{\"name\":\"Alice\",\"address\":{\"city\":\"Istanbul\"}}}]";
            assertStreamingMatchesTree("SELECT * FROM $r", json);
        }

        @ParameterizedTest
        @ValueSource(
                strings = {
                    "SELECT * FROM $r LIMIT 1",
                    "SELECT * FROM $r WHERE age > 0 LIMIT 2",
                    "SELECT * FROM $r LIMIT 3 OFFSET 1"
                })
        void early_termination_queries(String sql) {
            assertStreamingMatchesTree(sql, EMPLOYEES);
        }
    }
}
