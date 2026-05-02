// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GrammarTest {

    // ── shared helpers ──────────────────────────────────────────────────

    private static List<JsonValue> arr(JsonValue v) {
        return v.asArray().orElseThrow();
    }

    private static Map<String, JsonValue> obj(JsonValue v) {
        return v.asObject().orElseThrow();
    }

    private static String str(JsonValue v) {
        return v.asString().orElseThrow();
    }

    private static Number num(JsonValue v) {
        return v.asNumber().orElseThrow();
    }

    private static JsonValue f(JsonValue row, String key) {
        return obj(row).get(key);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Parse tests — basic grammar validation
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    class ParseTests {

        private final String sampleData = """
                [{"name":"Alice","age":25,"cat":"A","val":100}]
                """;

        private JsonValue execute(String sql) {
            return SQL4Json.queryAsJsonValue(sql, sampleData);
        }

        // --- Valid queries ---

        @Test
        void validQuery_selectAsterisk() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r"));
        }

        @Test
        void validQuery_selectSpecificColumns() {
            assertDoesNotThrow(() -> execute("SELECT name, age FROM $r"));
        }

        @Test
        void validQuery_countWithGroupBy() {
            assertDoesNotThrow(() -> execute("SELECT COUNT(*) AS cnt FROM $r GROUP BY name"));
        }

        @Test
        void validQuery_whereWithOrderBy() {
            assertDoesNotThrow(() -> execute("SELECT name FROM $r WHERE age > 10 ORDER BY name ASC"));
        }

        @Test
        void validQuery_whereLike() {
            assertDoesNotThrow(() -> execute("SELECT name FROM $r WHERE name LIKE '%test%'"));
        }

        @Test
        void validQuery_whereIsNull() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r WHERE name IS NULL"));
        }

        @Test
        void validQuery_whereIsNotNull() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r WHERE name IS NOT NULL"));
        }

        @Test
        void validQuery_lowerFunction() {
            assertDoesNotThrow(() -> execute("SELECT LOWER(name,'en-US') FROM $r"));
        }

        @Test
        void validQuery_sumWithGroupByAndHaving() {
            assertDoesNotThrow(() -> execute("SELECT SUM(val) AS total FROM $r GROUP BY cat HAVING total > 10"));
        }

        // --- Invalid queries ---

        @ParameterizedTest(name = "invalidQuery_throwsParseException[{0}]")
        @ValueSource(strings = {"select from $r", "SELECT * FROM", "SELECT * WHERE name = 'a'", "not a query at all"})
        void invalidQuery_throwsParseException(String sql) {
            assertThrows(SQL4JsonParseException.class, () -> execute(sql));
        }

        @Test
        void invalidQuery_emptyQuery() {
            assertThrows(SQL4JsonException.class, () -> SQL4Json.queryAsJsonValue("", sampleData));
        }

        // --- New grammar feature tests ---

        @Test
        void when_negative_number_in_where_then_query_succeeds() {
            assertDoesNotThrow(() -> {
                String json = "[{\"temp\": -5}, {\"temp\": 10}]";
                SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE temp > -10", json);
            });
        }

        @Test
        void when_escaped_single_quote_in_string_then_value_preserved() {
            String json = "[{\"name\": \"it's\"}]";
            var result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE name = 'it''s'", json);
            assertEquals(1, arr(result).size());
        }

        @Test
        void when_generic_scalar_function_in_where_then_query_succeeds() {
            String json = "[{\"name\": \"Alice\"}, {\"name\": \"bob\"}]";
            var result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE LOWER(name) = 'alice'", json);
            assertEquals(1, arr(result).size());
        }

        @Test
        void when_and_has_higher_precedence_than_or_then_correct_rows_returned() {
            String json =
                    "[{\"a\":\"x\",\"b\":\"no\",\"c\":\"no\"},{\"a\":\"no\",\"b\":\"y\",\"c\":\"z\"},{\"a\":\"x\",\"b\":\"no\",\"c\":\"z\"}]";
            var result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a = 'x' OR b = 'y' AND c = 'z'", json);
            assertEquals(3, arr(result).size());
        }

        // --- DISTINCT ---

        @Test
        void validQuery_selectDistinct() {
            assertDoesNotThrow(() -> execute("SELECT DISTINCT name FROM $r"));
        }

        @Test
        void validQuery_selectDistinctMultiColumn() {
            assertDoesNotThrow(() -> execute("SELECT DISTINCT name, age FROM $r"));
        }

        // --- LIMIT/OFFSET ---

        @Test
        void validQuery_limit() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r LIMIT 10"));
        }

        @Test
        void validQuery_limitWithOffset() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r LIMIT 10 OFFSET 5"));
        }

        @Test
        void validQuery_limitZero() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r LIMIT 0"));
        }

        // --- CAST (in SELECT — no handler needed) ---

        @Test
        void validQuery_castInSelect() {
            assertDoesNotThrow(() -> execute("SELECT CAST(age AS STRING) FROM $r"));
        }

        // --- IN / NOT IN ---

        @Test
        void validQuery_whereIn() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r WHERE name IN ('Alice', 'Bob')"));
        }

        @Test
        void validQuery_whereNotIn() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r WHERE name NOT IN ('Alice')"));
        }

        // --- BETWEEN / NOT BETWEEN ---

        @Test
        void validQuery_whereBetween() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r WHERE age BETWEEN 10 AND 30"));
        }

        @Test
        void validQuery_whereNotBetween() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r WHERE age NOT BETWEEN 10 AND 20"));
        }

        @Test
        void validQuery_betweenAndDisambiguation() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r WHERE age BETWEEN 10 AND 30 AND name = 'Alice'"));
        }

        // --- NOT LIKE / CAST in WHERE ---

        @Test
        void validQuery_whereNotLike() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r WHERE name NOT LIKE '%test%'"));
        }

        @Test
        void validQuery_castInWhere() {
            assertDoesNotThrow(() -> execute("SELECT * FROM $r WHERE CAST(age AS STRING) = '25'"));
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Fix tests — regression tests for specific bug fixes
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    class FixTests {

        private final String data = """
                [
                    {"name":"Alice","age":30,"dept":"Engineering","pattern":"%li%","hire_date":"2021-03-15"},
                    {"name":"Bob","age":20,"dept":"Sales","pattern":"%ob","hire_date":"2019-07-01"},
                    {"name":"Charlie","age":35,"dept":"Engineering","pattern":"%ar%","hire_date":"2018-01-20"}
                ]
                """;

        private JsonValue query(String sql) {
            return SQL4Json.queryAsJsonValue(sql, data);
        }

        private double dbl(JsonValue v) {
            return v.asNumber().orElseThrow().doubleValue();
        }

        // ── Fix 1: CAST with literals ──────────────────────────────────────

        @Test
        void cast_stringLiteralToNumber() {
            JsonValue result = query("SELECT CAST('42' AS NUMBER) AS val FROM $r LIMIT 1");
            assertEquals(42.0, dbl(f(arr(result).get(0), "val")), 0.01);
        }

        @Test
        void cast_stringLiteralToDate() {
            JsonValue result = query("SELECT CAST('2024-06-15' AS DATE) AS d FROM $r LIMIT 1");
            assertEquals("2024-06-15", str(f(arr(result).get(0), "d")));
        }

        @Test
        void cast_numberLiteralToString() {
            // Since 1.2.0 whole-number literals normalize to int, so CAST(42 AS STRING) → "42"
            // (was "42.0" while literals went through Double.parseDouble unconditionally).
            JsonValue result = query("SELECT CAST(42 AS STRING) AS val FROM $r LIMIT 1");
            assertEquals("42", str(f(arr(result).get(0), "val")));
        }

        @Test
        void cast_stringLiteralToDateTime() {
            JsonValue result = query("SELECT CAST('2024-06-15T10:30:00' AS DATETIME) AS dt FROM $r LIMIT 1");
            assertEquals("2024-06-15T10:30", str(f(arr(result).get(0), "dt")));
        }

        @Test
        void where_rhsCastLiteral() {
            JsonValue result = query("SELECT name FROM $r WHERE age = CAST('30' AS NUMBER)");
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
        }

        @Test
        void where_rhsCastLiteralToDate() {
            JsonValue result = query("SELECT name FROM $r WHERE TO_DATE(hire_date) > CAST('2020-01-01' AS DATE)");
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
        }

        // ── Fix 2: Column-to-column comparison ─────────────────────────────

        @Test
        void where_columnEqualsColumn() {
            String json = "[{\"a\":1,\"b\":1},{\"a\":2,\"b\":3}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a = b", json);
            assertEquals(1, arr(result).size());
            assertEquals(1, num(f(arr(result).get(0), "a")).intValue());
        }

        @Test
        void where_columnLessThanColumn() {
            String json = "[{\"start\":10,\"end\":20},{\"start\":30,\"end\":25}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE start < end", json);
            assertEquals(1, arr(result).size());
            assertEquals(10, num(f(arr(result).get(0), "start")).intValue());
        }

        @Test
        void where_columnNotEqualsColumn() {
            String json = "[{\"x\":\"a\",\"y\":\"a\"},{\"x\":\"a\",\"y\":\"b\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE x != y", json);
            assertEquals(1, arr(result).size());
            assertEquals("b", str(f(arr(result).get(0), "y")));
        }

        @Test
        void where_nestedFieldColumnComparison() {
            String json = "[{\"a\":{\"x\":1},\"b\":{\"x\":1}},{\"a\":{\"x\":2},\"b\":{\"x\":3}}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a.x = b.x", json);
            assertEquals(1, arr(result).size());
        }

        // ── Fix 3: HAVING with aggregate expressions ───────────────────────

        @Test
        void having_countStar() {
            JsonValue result =
                    query("SELECT dept, COUNT(*) AS cnt FROM $r GROUP BY dept HAVING COUNT(*) >= 2 ORDER BY dept ASC");
            assertEquals(1, arr(result).size());
            assertEquals("Engineering", str(f(arr(result).get(0), "dept")));
        }

        @Test
        void having_sumExpression() {
            JsonValue result = query("SELECT dept, SUM(age) AS total FROM $r GROUP BY dept HAVING SUM(age) > 50");
            assertEquals(1, arr(result).size());
            assertEquals("Engineering", str(f(arr(result).get(0), "dept")));
        }

        @Test
        void having_avgExpression() {
            JsonValue result = query("SELECT dept, AVG(age) AS avg_age FROM $r GROUP BY dept HAVING AVG(age) < 25");
            assertEquals(1, arr(result).size());
            assertEquals("Sales", str(f(arr(result).get(0), "dept")));
        }

        // ── Fix 4: ORDER BY with aggregate expressions ─────────────────────

        @Test
        void orderBy_countDesc() {
            JsonValue result = query("SELECT dept, COUNT(*) AS cnt FROM $r GROUP BY dept ORDER BY COUNT(*) DESC");
            assertEquals(2, arr(result).size());
            assertEquals("Engineering", str(f(arr(result).get(0), "dept")));
            assertEquals("Sales", str(f(arr(result).get(1), "dept")));
        }

        @Test
        void orderBy_sumAsc() {
            JsonValue result = query("SELECT dept, SUM(age) AS total FROM $r GROUP BY dept ORDER BY SUM(age) ASC");
            assertEquals(2, arr(result).size());
            assertEquals("Sales", str(f(arr(result).get(0), "dept")));
            assertEquals("Engineering", str(f(arr(result).get(1), "dept")));
        }

        @Test
        void orderBy_maxDesc() {
            JsonValue result = query("SELECT dept, MAX(age) AS oldest FROM $r GROUP BY dept ORDER BY MAX(age) DESC");
            assertEquals(2, arr(result).size());
            assertEquals("Engineering", str(f(arr(result).get(0), "dept")));
        }

        // ── Fix 5: LIKE with expressions ───────────────────────────────────

        @Test
        void like_functionOnRhs() {
            JsonValue result = query("SELECT name FROM $r WHERE name LIKE UPPER('%li%') ORDER BY name ASC");
            assertEquals(2, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
            assertEquals("Charlie", str(f(arr(result).get(1), "name")));
        }

        @Test
        void like_columnPattern() {
            JsonValue result = query("SELECT name FROM $r WHERE name LIKE pattern ORDER BY name ASC");
            assertEquals(3, arr(result).size());
        }

        @Test
        void like_columnPattern_partial() {
            String json = """
                    [{"name":"Alice","pat":"%z%"},{"name":"Bob","pat":"%ob"}]
                    """;
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT name FROM $r WHERE name LIKE pat", json);
            assertEquals(1, arr(result).size());
            assertEquals("Bob", str(f(arr(result).get(0), "name")));
        }

        @Test
        void notLike_functionOnRhs() {
            JsonValue result = query("SELECT name FROM $r WHERE name NOT LIKE UPPER('%li%')");
            assertEquals(1, arr(result).size());
            assertEquals("Bob", str(f(arr(result).get(0), "name")));
        }

        @Test
        void notLike_columnPattern() {
            String json = """
                    [{"name":"Alice","pat":"%z%"},{"name":"Bob","pat":"%ob"}]
                    """;
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT name FROM $r WHERE name NOT LIKE pat", json);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).get(0), "name")));
        }

        // ── Combined: multiple fixes in one query ──────────────────────────

        @Test
        void combined_castLiteral_and_columnCompare_and_havingAggregate() {
            String json = """
                    [{"dept":"A","val":10,"min":5},{"dept":"A","val":20,"min":5},
                     {"dept":"B","val":3,"min":5},{"dept":"B","val":4,"min":5}]
                    """;
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT dept, SUM(val) AS total
                    FROM $r
                    WHERE val > CAST('2' AS NUMBER)
                    GROUP BY dept
                    HAVING SUM(val) > CAST('15' AS INTEGER)
                    ORDER BY SUM(val) DESC
                    """, json);
            assertEquals(1, arr(result).size());
            assertEquals("A", str(f(arr(result).get(0), "dept")));
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Edge case tests — boundary inputs and uncommon SQL constructs
    // ═══════════════════════════════════════════════════════════════════

    @Nested
    class StringLiteralEdgeCases {

        @Test
        void emptyStringLiteral_inWhere() {
            String data = """
                    [{"name":"Alice"},{"name":""}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE name = ''", data);
            assertEquals(1, arr(result).size());
            assertEquals("", str(f(arr(result).getFirst(), "name")));
        }

        @Test
        void emptyStringLiteral_inSelect() {
            String data = "[{\"x\":1}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT '' AS empty_val FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals("", str(f(arr(result).getFirst(), "empty_val")));
        }

        @Test
        void multipleConsecutiveEscapedQuotes() {
            String data = "[{\"val\":\"a'b'c\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE val = 'a''b''c'", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void stringWithOnlyEscapedQuote() {
            String data = "[{\"val\":\"'\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE val = ''''", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void stringWithSpaces() {
            String data = "[{\"name\":\"  spaces  \"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE name = '  spaces  '", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void stringWithSpecialCharacters() {
            String data = "[{\"val\":\"hello\\nworld\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE val IS NOT NULL", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void likeWithEmptyPattern() {
            String data = """
                    [{"name":"Alice"},{"name":""}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE name LIKE ''", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void likeWithOnlyWildcard() {
            String data = """
                    [{"name":"Alice"},{"name":"Bob"},{"name":""}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE name LIKE '%'", data);
            assertEquals(3, arr(result).size());
        }

        @Test
        void likeWithUnderscoreWildcard() {
            String data = """
                    [{"code":"AB"},{"code":"A"},{"code":"ABC"}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE code LIKE 'A_'", data);
            assertEquals(1, arr(result).size());
            assertEquals("AB", str(f(arr(result).getFirst(), "code")));
        }

        @Test
        void inWithEmptyString() {
            String data = """
                    [{"name":"Alice"},{"name":""},{"name":"Bob"}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE name IN ('', 'Bob')", data);
            assertEquals(2, arr(result).size());
        }
    }

    @Nested
    class NumberLiteralEdgeCases {

        @Test
        void zeroInWhere() {
            String data = "[{\"val\":0},{\"val\":1},{\"val\":-1}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE val = 0", data);
            assertEquals(1, arr(result).size());
            assertEquals(0, num(f(arr(result).getFirst(), "val")).intValue());
        }

        @Test
        void decimalNumberInWhere() {
            String data = "[{\"price\":1.5},{\"price\":2.0},{\"price\":0.99}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE price > 0.99 AND price < 2.0", data);
            assertEquals(1, arr(result).size());
            assertEquals(1.5, num(f(arr(result).getFirst(), "price")).doubleValue(), 0.01);
        }

        @Test
        void negativeDecimalInWhere() {
            String data = "[{\"temp\":-0.5},{\"temp\":0.5}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE temp = -0.5", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void largeNumberInWhere() {
            String data = "[{\"big\":999999999999}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE big > 999999999998", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void negativeInBetween() {
            String data = "[{\"val\":-5},{\"val\":0},{\"val\":5}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE val BETWEEN -10 AND 0", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void zeroAsLimitReturnsEmpty() {
            String data = "[{\"a\":1},{\"a\":2}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r LIMIT 0", data);
            assertEquals(0, arr(result).size());
        }

        @Test
        void numberLiteralInSelect() {
            String data = "[{\"x\":1}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT 42 AS answer, -1 AS neg FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals(42, num(f(arr(result).getFirst(), "answer")).intValue());
            assertEquals(-1, num(f(arr(result).getFirst(), "neg")).intValue());
        }

        @Test
        void booleanLiteralInSelect() {
            String data = "[{\"x\":1}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT true AS flag FROM $r", data);
            assertEquals(1, arr(result).size());
            assertTrue(f(arr(result).getFirst(), "flag").asBoolean().orElseThrow());
        }
    }

    @Nested
    class IdentifierEdgeCases {

        @Test
        void identifierWithHyphen() {
            String data = "[{\"first-name\":\"Alice\",\"last-name\":\"Smith\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT first-name, last-name FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).getFirst(), "first-name")));
            assertEquals("Smith", str(f(arr(result).getFirst(), "last-name")));
        }

        @Test
        void identifierWithUnderscore() {
            String data = "[{\"_private\":1,\"__dunder\":2}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT _private, __dunder FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals(1, num(f(arr(result).getFirst(), "_private")).intValue());
            assertEquals(2, num(f(arr(result).getFirst(), "__dunder")).intValue());
        }

        @Test
        void singleCharIdentifier() {
            String data = "[{\"x\":1,\"y\":2,\"z\":3}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT x, y, z FROM $r WHERE x = 1", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void keywordsAsFieldNames_extended() {
            String data = """
                    [{"inner":1,"left":2,"right":3,"join":4,"on":5,"over":6}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT inner, left, right, join, on, over FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals(1, num(f(arr(result).getFirst(), "inner")).intValue());
            assertEquals(4, num(f(arr(result).getFirst(), "join")).intValue());
        }

        @Test
        void caseWhenKeywordsAsFieldNames() {
            String data = """
                    [{"case":10,"when":20,"then":30,"else":40,"end":50}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT case, when, then, else, end FROM $r", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void identifierWithDigits() {
            String data = "[{\"field1\":\"a\",\"field2b\":\"b\",\"f3ield\":\"c\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT field1, field2b, f3ield FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals("a", str(f(arr(result).getFirst(), "field1")));
        }

        @Test
        void aliasContainingDots_createsNestedOutput() {
            String data = "[{\"name\":\"Alice\",\"age\":30}]";
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT name AS person.info.name, age AS person.info.age FROM $r", data);
            var row = arr(result).getFirst();
            var person = obj(f(row, "person"));
            var info = obj(person.get("info"));
            assertEquals("Alice", str(info.get("name")));
        }
    }

    @Nested
    class CaseSensitivity {

        private final String data = "[{\"name\":\"Alice\",\"age\":25}]";

        @ParameterizedTest(name = "mixedCaseKeywords[{0}]")
        @ValueSource(strings = {"select * from $r", "SELECT * FROM $r", "Select * From $r", "sElEcT * fRoM $r"})
        void mixedCaseKeywords_allValid(String sql) {
            assertDoesNotThrow(() -> SQL4Json.queryAsJsonValue(sql, data));
        }

        @ParameterizedTest(name = "mixedCaseFunctions[{0}]")
        @ValueSource(
                strings = {
                    "SELECT lower(name) AS n FROM $r",
                    "SELECT LOWER(name) AS n FROM $r",
                    "SELECT Lower(name) AS n FROM $r"
                })
        void mixedCaseFunctionNames_allValid(String sql) {
            JsonValue result = SQL4Json.queryAsJsonValue(sql, data);
            assertEquals("alice", str(f(arr(result).getFirst(), "n")));
        }

        @ParameterizedTest(name = "mixedCaseClauseKeywords[{0}]")
        @ValueSource(
                strings = {
                    "SELECT * FROM $r where age > 10",
                    "SELECT * FROM $r WHERE age > 10",
                    "SELECT * FROM $r Where age > 10"
                })
        void mixedCaseWhereKeyword(String sql) {
            JsonValue result = SQL4Json.queryAsJsonValue(sql, data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void mixedCaseAggFunctions() {
            String data = "[{\"g\":\"x\",\"a\":1},{\"g\":\"x\",\"a\":2},{\"g\":\"x\",\"a\":3}]";
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT count(*) AS cnt, sum(a) AS total, avg(a) AS average FROM $r GROUP BY g", data);
            assertEquals(1, arr(result).size());
            assertEquals(3, num(f(arr(result).getFirst(), "cnt")).intValue());
        }

        @Test
        void mixedCaseOrderDirection() {
            String data = "[{\"a\":1},{\"a\":2}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r ORDER BY a desc", data);
            assertEquals(2, num(f(arr(result).getFirst(), "a")).intValue());
        }

        @Test
        void mixedCaseDistinctLimitOffset() {
            String data = "[{\"a\":1},{\"a\":1},{\"a\":2}]";
            JsonValue result =
                    SQL4Json.queryAsJsonValue("select distinct a from $r order by a asc limit 1 offset 0", data);
            assertEquals(1, arr(result).size());
            assertEquals(1, num(f(arr(result).getFirst(), "a")).intValue());
        }

        @Test
        void mixedCaseIsNullIsNotNull() {
            String data = "[{\"a\":null},{\"a\":1}]";
            JsonValue r1 = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a is null", data);
            JsonValue r2 = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a Is Not Null", data);
            assertEquals(1, arr(r1).size());
            assertEquals(1, arr(r2).size());
        }

        @Test
        void mixedCaseBetweenInLike() {
            String data = "[{\"a\":5,\"name\":\"Alice\"}]";
            JsonValue r1 = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a between 1 and 10", data);
            JsonValue r2 = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE name like 'Ali%'", data);
            JsonValue r3 = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a in (5, 6)", data);
            assertEquals(1, arr(r1).size());
            assertEquals(1, arr(r2).size());
            assertEquals(1, arr(r3).size());
        }
    }

    @Nested
    class ComplexConditions {

        private final String data = """
                [
                    {"a":1,"b":2,"c":3,"name":"Alice","status":"active"},
                    {"a":4,"b":5,"c":6,"name":"Bob","status":"inactive"},
                    {"a":7,"b":8,"c":9,"name":"Charlie","status":"active"},
                    {"a":10,"b":11,"c":12,"name":"Diana","status":null}
                ]
                """;

        @Test
        void deeplyNestedParentheses() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE ((((a = 1))))", data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).getFirst(), "name")));
        }

        @Test
        void multiLevelAndOrWithParentheses() {
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE (a = 1 OR a = 4) AND (b = 2 OR b = 5)", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void threeWayOrWithAndInside() {
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT * FROM $r WHERE (a = 1 AND b = 2) OR (a = 4 AND b = 5) OR (a = 7 AND c = 9)", data);
            assertEquals(3, arr(result).size());
        }

        @Test
        void allConditionTypesCombined() {
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT * FROM $r WHERE a > 0 AND name LIKE 'A%' AND status IS NOT NULL AND a IN (1, 2) AND b BETWEEN 1 AND 5",
                    data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).getFirst(), "name")));
        }

        @Test
        void notBetweenCombinedWithNotIn() {
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT * FROM $r WHERE a NOT BETWEEN 1 AND 6 AND name NOT IN ('Diana')", data);
            assertEquals(1, arr(result).size());
            assertEquals("Charlie", str(f(arr(result).getFirst(), "name")));
        }

        @Test
        void notLikeCombinedWithNotIn() {
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT * FROM $r WHERE name NOT LIKE 'A%' AND name NOT IN ('Diana') AND status IS NOT NULL", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void andOrPrecedenceWithoutParens_threeTerms() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a = 1 OR b = 5 AND c = 6", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void parenthesesOverrideNaturalPrecedence() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE (a = 1 OR b = 5) AND c = 6", data);
            assertEquals(1, arr(result).size());
            assertEquals("Bob", str(f(arr(result).getFirst(), "name")));
        }
    }

    @Nested
    class DeepFunctionNesting {

        @Test
        void tripleNestedStringFunctions() {
            String data = "[{\"name\":\"  Alice  \"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT UPPER(TRIM(LOWER(name))) AS processed FROM $r", data);
            assertEquals("ALICE", str(f(arr(result).getFirst(), "processed")));
        }

        @Test
        void functionInsideCastInsideFunction() {
            String data = "[{\"val\":\"42.7\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT ROUND(CAST(val AS NUMBER)) AS rounded FROM $r", data);
            assertEquals(43, num(f(arr(result).getFirst(), "rounded")).intValue());
        }

        @Test
        void concatWithNestedFunctions() {
            String data = "[{\"first\":\"alice\",\"last\":\"SMITH\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT CONCAT(UPPER(first), ' ', LOWER(last)) AS full_name FROM $r", data);
            assertEquals("ALICE smith", str(f(arr(result).getFirst(), "full_name")));
        }

        @Test
        void nestedFunctionInWhere() {
            String data = "[{\"name\":\"  Alice  \"},{\"name\":\"  Bob  \"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE UPPER(TRIM(name)) = 'ALICE'", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void nestedFunctionInOrderBy() {
            String data = "[{\"name\":\"Bob\"},{\"name\":\"Alice\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r ORDER BY LOWER(name) ASC", data);
            assertEquals("Alice", str(f(arr(result).getFirst(), "name")));
        }

        @Test
        void nestedFunctionInGroupBy() {
            String data = """
                    [{"name":"alice","val":1},{"name":"Alice","val":2},{"name":"ALICE","val":3}]""";
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT LOWER(name) AS n, SUM(val) AS total FROM $r GROUP BY LOWER(name)", data);
            assertEquals(1, arr(result).size());
            assertEquals(6, num(f(arr(result).getFirst(), "total")).intValue());
        }

        @Test
        void lengthOfConcatResult() {
            String data = "[{\"a\":\"hello\",\"b\":\"world\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT LENGTH(CONCAT(a, b)) AS len FROM $r", data);
            assertEquals(10, num(f(arr(result).getFirst(), "len")).intValue());
        }
    }

    @Nested
    class ClauseCombinations {

        private final String data = """
                [
                    {"name":"Alice","dept":"eng","salary":90000,"rating":4.5},
                    {"name":"Bob","dept":"eng","salary":80000,"rating":3.8},
                    {"name":"Charlie","dept":"hr","salary":70000,"rating":4.2},
                    {"name":"Diana","dept":"hr","salary":65000,"rating":3.5},
                    {"name":"Eve","dept":"sales","salary":60000,"rating":4.0}
                ]
                """;

        @Test
        void allClausesTogether() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal
                    FROM $r
                    WHERE salary > 60000
                    GROUP BY dept
                    HAVING cnt >= 2
                    ORDER BY avg_sal DESC
                    LIMIT 1 OFFSET 0
                    """, data);
            assertEquals(1, arr(result).size());
            assertEquals("eng", str(f(arr(result).getFirst(), "dept")));
        }

        @Test
        void distinctWithAllClauses() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT DISTINCT dept
                    FROM $r
                    WHERE salary > 60000
                    ORDER BY dept ASC
                    LIMIT 10
                    """, data);
            assertEquals(2, arr(result).size());
            assertEquals("eng", str(f(arr(result).getFirst(), "dept")));
            assertEquals("hr", str(f(arr(result).get(1), "dept")));
        }

        @Test
        void castInOrderBy() {
            String data = "[{\"val\":\"100\"},{\"val\":\"20\"},{\"val\":\"3\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT val FROM $r ORDER BY CAST(val AS NUMBER) ASC", data);
            assertEquals("3", str(f(arr(result).getFirst(), "val")));
            assertEquals("20", str(f(arr(result).get(1), "val")));
            assertEquals("100", str(f(arr(result).get(2), "val")));
        }

        @Test
        void castInGroupBy() {
            String data = """
                    [{"code":"1","val":10},{"code":"01","val":20},{"code":"1.0","val":30}]""";
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT CAST(code AS INTEGER) AS code_int, SUM(val) AS total FROM $r GROUP BY CAST(code AS INTEGER)",
                    data);
            assertEquals(1, arr(result).size());
            assertEquals(60, num(f(arr(result).getFirst(), "total")).intValue());
        }

        @Test
        void betweenWithFunctionOnColumn() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE LENGTH(name) BETWEEN 3 AND 4", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void functionInBothSidesOfComparison() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE LOWER(name) = LOWER('ALICE')", data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).getFirst(), "name")));
        }

        @Test
        void multipleAggregatesWithGroupBy() {
            String singleGroupData = """
                    [
                        {"g":"all","name":"Alice","dept":"eng","salary":90000,"rating":4.5},
                        {"g":"all","name":"Bob","dept":"eng","salary":80000,"rating":3.8},
                        {"g":"all","name":"Charlie","dept":"hr","salary":70000,"rating":4.2},
                        {"g":"all","name":"Diana","dept":"hr","salary":65000,"rating":3.5},
                        {"g":"all","name":"Eve","dept":"sales","salary":60000,"rating":4.0}
                    ]
                    """;
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT COUNT(*) AS cnt, SUM(salary) AS total, AVG(salary) AS average, MIN(salary) AS low, MAX(salary) AS high FROM $r GROUP BY g",
                    singleGroupData);
            assertEquals(1, arr(result).size());
            assertEquals(5, num(f(arr(result).getFirst(), "cnt")).intValue());
        }

        @Test
        void havingWithMultipleAggregateConditions() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal
                    FROM $r
                    GROUP BY dept
                    HAVING COUNT(*) >= 2 AND AVG(salary) > 70000
                    """, data);
            assertEquals(1, arr(result).size());
            assertEquals("eng", str(f(arr(result).getFirst(), "dept")));
        }

        @Test
        void orderByMultipleColumnsAndDirections() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r ORDER BY dept ASC, salary DESC", data);
            var rows = arr(result);
            assertEquals("Alice", str(f(rows.getFirst(), "name")));
            assertEquals("Bob", str(f(rows.get(1), "name")));
        }
    }

    @Nested
    class SubqueryEdgeCases {

        @Test
        void nestedSubquery_twoLevels() {
            String data = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]";
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT n FROM (SELECT name AS n, age AS a FROM (SELECT * FROM $r WHERE age > 10))", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void subqueryWithOrderByAndLimit() {
            String data =
                    "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20},{\"name\":\"Charlie\",\"age\":25}]";
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT * FROM (SELECT name, age FROM $r ORDER BY age ASC LIMIT 2)", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void subqueryWithAggregation() {
            String data = """
                    [{"dept":"eng","salary":100},{"dept":"eng","salary":200},{"dept":"hr","salary":150}]""";
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT * FROM (SELECT dept, SUM(salary) AS total FROM $r GROUP BY dept) WHERE total > 150", data);
            assertEquals(1, arr(result).size());
            assertEquals("eng", str(f(arr(result).getFirst(), "dept")));
        }

        @Test
        void subqueryWithWhereOnBothLevels() {
            String data = "[{\"a\":1,\"b\":10},{\"a\":2,\"b\":20},{\"a\":3,\"b\":30}]";
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT * FROM (SELECT a, b FROM $r WHERE a > 1) WHERE b < 30", data);
            assertEquals(1, arr(result).size());
            assertEquals(2, num(f(arr(result).getFirst(), "a")).intValue());
        }
    }

    @Nested
    class FromClauseEdgeCases {

        @Test
        void deepNestedJsonPath() {
            String data = "{\"a\":{\"b\":{\"c\":{\"d\":[{\"val\":1}]}}}}";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT val FROM $r.a.b.c.d", data);
            assertEquals(1, arr(result).size());
            assertEquals(1, num(f(arr(result).getFirst(), "val")).intValue());
        }

        @Test
        void rootOnSingleObject() {
            String data = "{\"name\":\"Alice\",\"age\":30}";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).getFirst(), "name")));
        }

        @Test
        void rootOnNestedSingleObject() {
            String data = "{\"data\":{\"name\":\"Alice\"}}";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT name FROM $r.data", data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).getFirst(), "name")));
        }
    }

    @Nested
    class SemicolonAndWhitespace {

        private final String data = "[{\"a\":1}]";

        @Test
        void queryWithTrailingSemicolon() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r;", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void queryWithExtraWhitespace() {
            JsonValue result = SQL4Json.queryAsJsonValue("  SELECT   *   FROM   $r  ", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void queryWithTabs() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT\t*\tFROM\t$r", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void queryWithNewlines() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT\n*\nFROM\n$r", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void multilineComplexQuery() {
            String data = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]";
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT
                        name,
                        age
                    FROM
                        $r
                    WHERE
                        age > 10
                    ORDER BY
                        name ASC
                    LIMIT
                        10
                    """, data);
            assertEquals(2, arr(result).size());
        }
    }

    @Nested
    class CaseExpressionEdgeCases {

        @Test
        void caseWithAllConditionTypes() {
            String data = """
                    [{"name":"Alice","age":25,"status":"active"},
                     {"name":"Bob","age":35,"status":null}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT name,
                        CASE
                            WHEN age BETWEEN 20 AND 30 THEN 'young'
                            WHEN age > 30 THEN 'senior'
                            ELSE 'unknown'
                        END AS category
                    FROM $r
                    """, data);
            assertEquals(2, arr(result).size());
            assertEquals("young", str(f(arr(result).getFirst(), "category")));
            assertEquals("senior", str(f(arr(result).get(1), "category")));
        }

        @Test
        void caseWithLikeCondition() {
            String data = """
                    [{"name":"Alice"},{"name":"Bob"},{"name":"Charlie"}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT name,
                        CASE
                            WHEN name LIKE 'A%' THEN 'team_a'
                            WHEN name LIKE 'B%' THEN 'team_b'
                            ELSE 'team_other'
                        END AS team
                    FROM $r
                    """, data);
            assertEquals("team_a", str(f(arr(result).getFirst(), "team")));
            assertEquals("team_b", str(f(arr(result).get(1), "team")));
            assertEquals("team_other", str(f(arr(result).get(2), "team")));
        }

        @Test
        void caseWithIsNullCondition() {
            String data = "[{\"val\":null},{\"val\":42}]";
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT CASE WHEN val IS NULL THEN 'missing' ELSE 'present' END AS status
                    FROM $r
                    """, data);
            assertEquals("missing", str(f(arr(result).getFirst(), "status")));
            assertEquals("present", str(f(arr(result).get(1), "status")));
        }

        @Test
        void simpleCaseWithManyBranches() {
            String data = """
                    [{"code":1},{"code":2},{"code":3},{"code":4},{"code":5},{"code":99}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT CASE code
                        WHEN 1 THEN 'one'
                        WHEN 2 THEN 'two'
                        WHEN 3 THEN 'three'
                        WHEN 4 THEN 'four'
                        WHEN 5 THEN 'five'
                        ELSE 'other'
                    END AS word FROM $r
                    """, data);
            assertEquals(6, arr(result).size());
            assertEquals("one", str(f(arr(result).getFirst(), "word")));
            assertEquals("five", str(f(arr(result).get(4), "word")));
            assertEquals("other", str(f(arr(result).get(5), "word")));
        }

        @Test
        void caseWithFunctionInThenBranch() {
            String data = "[{\"name\":\"alice\",\"flag\":true},{\"name\":\"BOB\",\"flag\":false}]";
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT CASE
                        WHEN flag = true THEN UPPER(name)
                        ELSE LOWER(name)
                    END AS normalized FROM $r
                    """, data);
            assertEquals("ALICE", str(f(arr(result).getFirst(), "normalized")));
            assertEquals("bob", str(f(arr(result).get(1), "normalized")));
        }

        @Test
        void caseUsedInOrderByWithAlias() {
            String data = """
                    [{"name":"Alice","priority":"low"},
                     {"name":"Bob","priority":"high"},
                     {"name":"Charlie","priority":"medium"}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT name, priority
                    FROM $r
                    ORDER BY CASE priority
                        WHEN 'high' THEN 1
                        WHEN 'medium' THEN 2
                        WHEN 'low' THEN 3
                        ELSE 4
                    END ASC
                    """, data);
            assertEquals("Bob", str(f(arr(result).getFirst(), "name")));
            assertEquals("Charlie", str(f(arr(result).get(1), "name")));
            assertEquals("Alice", str(f(arr(result).get(2), "name")));
        }
    }

    @Nested
    class WindowFunctionEdgeCases {

        @Test
        void windowFunctionWithNoPartitionNoOrder() {
            String data = "[{\"a\":1},{\"a\":2},{\"a\":3}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT a, SUM(a) OVER () AS total FROM $r", data);
            assertEquals(3, arr(result).size());
            for (JsonValue row : arr(result)) {
                assertEquals(6, num(f(row, "total")).intValue());
            }
        }

        @Test
        void multipleWindowFunctionsWithDifferentSpecs() {
            String data = "[{\"dept\":\"a\",\"val\":1},{\"dept\":\"a\",\"val\":2},{\"dept\":\"b\",\"val\":3}]";
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT dept, val,
                        ROW_NUMBER() OVER (ORDER BY val) AS global_rn,
                        ROW_NUMBER() OVER (PARTITION BY dept ORDER BY val) AS dept_rn,
                        SUM(val) OVER (PARTITION BY dept) AS dept_sum
                    FROM $r
                    """, data);
            var rows = arr(result);
            assertEquals(3, rows.size());
            for (JsonValue row : rows) {
                if ("b".equals(str(f(row, "dept")))) {
                    assertEquals(3, num(f(row, "dept_sum")).intValue());
                    assertEquals(1, num(f(row, "dept_rn")).intValue());
                }
            }
        }

        @Test
        void lagAndLeadTogether() {
            String data = "[{\"val\":10},{\"val\":20},{\"val\":30}]";
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT val,
                        LAG(val) OVER (ORDER BY val) AS prev_val,
                        LEAD(val) OVER (ORDER BY val) AS next_val
                    FROM $r
                    """, data);
            var rows = arr(result);
            assertTrue(f(rows.getFirst(), "prev_val").isNull());
            assertEquals(20, num(f(rows.getFirst(), "next_val")).intValue());
            assertEquals(20, num(f(rows.get(2), "prev_val")).intValue());
            assertTrue(f(rows.get(2), "next_val").isNull());
        }
    }

    @Nested
    class InvalidQueryEdgeCases {

        private final String data = "[{\"a\":1}]";

        @Test
        void selectWithoutFrom() {
            assertThrows(SQL4JsonParseException.class, () -> SQL4Json.queryAsJsonValue("SELECT *", data));
        }

        @Test
        void fromWithoutSelect() {
            assertThrows(SQL4JsonParseException.class, () -> SQL4Json.queryAsJsonValue("FROM $r", data));
        }

        @Test
        void whereWithoutCondition() {
            assertThrows(SQL4JsonParseException.class, () -> SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE", data));
        }

        @Test
        void groupByWithoutColumn() {
            assertThrows(
                    SQL4JsonParseException.class, () -> SQL4Json.queryAsJsonValue("SELECT * FROM $r GROUP BY", data));
        }

        @Test
        void orderByWithoutColumn() {
            assertThrows(
                    SQL4JsonParseException.class, () -> SQL4Json.queryAsJsonValue("SELECT * FROM $r ORDER BY", data));
        }

        @Test
        void hangingAnd() {
            assertThrows(
                    SQL4JsonParseException.class,
                    () -> SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a = 1 AND", data));
        }

        @Test
        void hangingOr() {
            assertThrows(
                    SQL4JsonParseException.class,
                    () -> SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a = 1 OR", data));
        }

        @Test
        void unmatchedParenthesis() {
            assertThrows(
                    SQL4JsonException.class, () -> SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE (a = 1", data));
        }

        @Test
        void doubleCommaInSelect() {
            assertThrows(SQL4JsonParseException.class, () -> SQL4Json.queryAsJsonValue("SELECT a,, b FROM $r", data));
        }

        @Test
        void whitespaceOnlyQuery() {
            assertThrows(SQL4JsonException.class, () -> SQL4Json.queryAsJsonValue("   ", data));
        }

        @Test
        void limitWithoutNumber() {
            assertThrows(SQL4JsonParseException.class, () -> SQL4Json.queryAsJsonValue("SELECT * FROM $r LIMIT", data));
        }

        @Test
        void havingWithoutGroupBy_parsesSuccessfully() {
            assertDoesNotThrow(() -> SQL4Json.queryAsJsonValue("SELECT * FROM $r HAVING a > 1", data));
        }

        @Test
        void offsetWithoutLimit_parsesSuccessfully() {
            assertDoesNotThrow(() -> SQL4Json.queryAsJsonValue("SELECT * FROM $r OFFSET 5", data));
        }
    }

    @Nested
    class NullSemantics {

        private final String data = """
                [{"a":1,"b":"x"},{"a":null,"b":"y"},{"a":3,"b":null}]
                """;

        @Test
        void nullComparison_neverMatches() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a = null", data);
            assertEquals(0, arr(result).size());
        }

        @Test
        void nullInBetween_excluded() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a BETWEEN 0 AND 10", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void nullInNotBetween_excluded() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a NOT BETWEEN 0 AND 2", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void nullInLike_excluded() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE b LIKE '%'", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void nullInIn_excluded() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a IN (1, 3)", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void nullInNotIn_excluded() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE a NOT IN (1)", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void coalesceHandlesNull() {
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT a, COALESCE(a, 0) AS val FROM $r ORDER BY val ASC", data);
            var rows = arr(result);
            assertEquals(3, rows.size());
            boolean foundCoalescedZero = false;
            for (JsonValue row : rows) {
                assertFalse(f(row, "val").isNull(), "COALESCE should never return null");
                if (f(row, "a").isNull()) {
                    assertEquals(0, num(f(row, "val")).intValue(), "COALESCE(null, 0) should produce 0");
                    foundCoalescedZero = true;
                }
            }
            assertTrue(foundCoalescedZero, "Should have found the null->0 coalesced row");
        }

        @Test
        void orderByWithNulls() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r ORDER BY a ASC", data);
            assertEquals(3, arr(result).size());
        }

        @Test
        void groupByOnNullableField() {
            String data = """
                    [{"grp":null,"val":1},{"grp":null,"val":2},{"grp":"a","val":3}]""";
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT grp, SUM(val) AS total FROM $r GROUP BY grp ORDER BY total ASC", data);
            assertEquals(2, arr(result).size());
        }

        @Test
        void distinctOnNullableField() {
            String data = "[{\"a\":null},{\"a\":null},{\"a\":1}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT DISTINCT a FROM $r", data);
            assertEquals(2, arr(result).size());
        }
    }

    @Nested
    class JoinGrammarEdgeCases {

        private final String users = """
                [{"id":1,"name":"Alice","dept_id":10},
                 {"id":2,"name":"Bob","dept_id":20},
                 {"id":3,"name":"Charlie","dept_id":10}]""";

        private final String depts = """
                [{"dept_id":10,"dept_name":"Engineering"},
                 {"dept_id":20,"dept_name":"Marketing"}]""";

        private final String orders = """
                [{"order_id":1,"user_id":1,"amount":100},
                 {"order_id":2,"user_id":1,"amount":200},
                 {"order_id":3,"user_id":2,"amount":150}]""";

        @Test
        void innerJoinWithTableAliases() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT u.name AS name, d.dept_name AS dept
                    FROM users u INNER JOIN depts d ON u.dept_id = d.dept_id
                    """, Map.of("users", users, "depts", depts));
            assertEquals(3, arr(result).size());
        }

        @Test
        void joinWithWhereAndOrderBy() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT u.name AS name, o.amount AS amount
                    FROM users u JOIN orders o ON u.id = o.user_id
                    WHERE o.amount > 100
                    ORDER BY o.amount DESC
                    """, Map.of("users", users, "orders", orders));
            assertEquals(2, arr(result).size());
            assertEquals(200, num(f(arr(result).getFirst(), "amount")).intValue());
        }

        @Test
        void joinWithGroupBy() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT u.name AS name, COUNT(*) AS order_count
                    FROM users u JOIN orders o ON u.id = o.user_id
                    GROUP BY u.name
                    ORDER BY order_count DESC
                    """, Map.of("users", users, "orders", orders));
            assertEquals(2, arr(result).size());
            assertEquals("Alice", str(f(arr(result).getFirst(), "name")));
            assertEquals(2, num(f(arr(result).getFirst(), "order_count")).intValue());
        }

        @Test
        void leftJoinWithNullCheck() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT u.name AS name, o.amount AS amount
                    FROM users u LEFT JOIN orders o ON u.id = o.user_id
                    WHERE o.amount IS NULL
                    """, Map.of("users", users, "orders", orders));
            assertEquals(1, arr(result).size());
            assertEquals("Charlie", str(f(arr(result).getFirst(), "name")));
        }

        @Test
        void joinWithMultipleOnConditions() {
            String products = """
                    [{"id":1,"cat":"A","name":"Widget"},{"id":2,"cat":"B","name":"Gadget"}]""";
            String inventory = """
                    [{"prod_id":1,"cat":"A","qty":100},{"prod_id":1,"cat":"B","qty":50},{"prod_id":2,"cat":"B","qty":200}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT p.name AS product, i.qty AS quantity
                    FROM products p JOIN inventory i ON p.id = i.prod_id AND p.cat = i.cat
                    """, Map.of("products", products, "inventory", inventory));
            assertEquals(2, arr(result).size());
        }
    }

    @Nested
    class MixedDataTypes {

        @Test
        void queryOnMixedTypeArray() {
            String data = """
                    [{"val":"text"},{"val":42},{"val":true},{"val":null}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE val IS NOT NULL", data);
            assertEquals(3, arr(result).size());
        }

        @Test
        void orderByOnMixedTypes() {
            String data = """
                    [{"name":"Alice","val":"text"},{"name":"Bob","val":42},{"name":"Charlie","val":null}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r ORDER BY name ASC", data);
            assertEquals(3, arr(result).size());
        }

        @Test
        void nestedObjectsPreserved() {
            String data = """
                    [{"name":"Alice","address":{"city":"Istanbul","coords":{"lat":41.0,"lon":29.0}}}]""";
            JsonValue result = SQL4Json.queryAsJsonValue(
                    "SELECT name, address.city AS city, address.coords.lat AS lat FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals("Istanbul", str(f(arr(result).getFirst(), "city")));
            assertEquals(41.0, num(f(arr(result).getFirst(), "lat")).doubleValue(), 0.01);
        }

        @Test
        void arrayFieldInData_selectStar() {
            String data = """
                    [{"name":"Alice","tags":["java","python"]},{"name":"Bob","tags":[]}]""";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r", data);
            assertEquals(2, arr(result).size());
        }
    }

    @Nested
    class AggregateEdgeCases {

        @Test
        void countStarOnEmptyArray() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT COUNT(*) AS cnt FROM $r GROUP BY x", "[]");
            assertEquals(0, arr(result).size());
        }

        @Test
        void sumOnNullValues() {
            String data = "[{\"g\":\"x\",\"val\":null},{\"g\":\"x\",\"val\":null}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT SUM(val) AS total FROM $r GROUP BY g", data);
            assertEquals(1, arr(result).size());
        }

        @Test
        void avgOnSingleRow() {
            String data = "[{\"g\":\"x\",\"val\":42}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT AVG(val) AS average FROM $r GROUP BY g", data);
            assertEquals(1, arr(result).size());
            assertEquals(42.0, num(f(arr(result).getFirst(), "average")).doubleValue(), 0.01);
        }

        @Test
        void minMaxOnSingleRow() {
            String data = "[{\"g\":\"x\",\"val\":42}]";
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT MIN(val) AS low, MAX(val) AS high FROM $r GROUP BY g", data);
            assertEquals(42, num(f(arr(result).getFirst(), "low")).intValue());
            assertEquals(42, num(f(arr(result).getFirst(), "high")).intValue());
        }

        @Test
        void groupByWithSingleGroup() {
            String data = "[{\"g\":\"a\",\"v\":1},{\"g\":\"a\",\"v\":2},{\"g\":\"a\",\"v\":3}]";
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT g, COUNT(*) AS cnt, SUM(v) AS total FROM $r GROUP BY g", data);
            assertEquals(1, arr(result).size());
            assertEquals(3, num(f(arr(result).getFirst(), "cnt")).intValue());
            assertEquals(6, num(f(arr(result).getFirst(), "total")).intValue());
        }

        @Test
        void groupByWhereAllRowsFiltered() {
            String data = "[{\"g\":\"a\",\"v\":1},{\"g\":\"b\",\"v\":2}]";
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT g, COUNT(*) AS cnt FROM $r WHERE v > 100 GROUP BY g", data);
            assertEquals(0, arr(result).size());
        }

        @Test
        void countStarVsCountColumn() {
            String data = "[{\"g\":\"x\",\"val\":1},{\"g\":\"x\",\"val\":null},{\"g\":\"x\",\"val\":3}]";
            JsonValue r1 = SQL4Json.queryAsJsonValue("SELECT COUNT(*) AS cnt FROM $r GROUP BY g", data);
            JsonValue r2 = SQL4Json.queryAsJsonValue("SELECT COUNT(val) AS cnt FROM $r GROUP BY g", data);
            assertEquals(3, num(f(arr(r1).getFirst(), "cnt")).intValue());
            assertEquals(2, num(f(arr(r2).getFirst(), "cnt")).intValue());
        }
    }

    @Nested
    class NullIfCoalesceEdgeCases {

        @Test
        void nullifInWhere() {
            String data = "[{\"a\":0,\"name\":\"Alice\"},{\"a\":1,\"name\":\"Bob\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r WHERE NULLIF(a, 0) IS NULL", data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(f(arr(result).getFirst(), "name")));
        }

        @Test
        void coalesceChainedWithMultipleFallbacks() {
            String data = "[{\"a\":null,\"b\":null,\"c\":\"found\"}]";
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT COALESCE(a, b, c) AS val FROM $r", data);
            assertEquals("found", str(f(arr(result).getFirst(), "val")));
        }

        @Test
        void nullifWithAggregateAvoidingZeroDivision() {
            String data = "[{\"g\":\"x\",\"val\":0},{\"g\":\"x\",\"val\":10},{\"g\":\"x\",\"val\":20}]";
            JsonValue result =
                    SQL4Json.queryAsJsonValue("SELECT AVG(NULLIF(val, 0)) AS avg_nonzero FROM $r GROUP BY g", data);
            assertEquals(1, arr(result).size());
            assertEquals(15.0, num(f(arr(result).getFirst(), "avg_nonzero")).doubleValue(), 0.01);
        }
    }

    @Nested
    class ComplexRealWorldQueries {

        private final String employees = """
                [
                    {"id":1,"name":"Alice","dept":"eng","salary":90000,"hire_date":"2020-01-15","manager_id":null},
                    {"id":2,"name":"Bob","dept":"eng","salary":80000,"hire_date":"2019-06-01","manager_id":1},
                    {"id":3,"name":"Charlie","dept":"marketing","salary":70000,"hire_date":"2021-03-10","manager_id":null},
                    {"id":4,"name":"Diana","dept":"marketing","salary":85000,"hire_date":"2018-11-20","manager_id":3},
                    {"id":5,"name":"Eve","dept":"eng","salary":95000,"hire_date":"2022-07-01","manager_id":1},
                    {"id":6,"name":"Frank","dept":"hr","salary":55000,"hire_date":"2023-01-15","manager_id":null},
                    {"id":7,"name":"Grace","dept":"hr","salary":52000,"hire_date":"2023-06-01","manager_id":6}
                ]
                """;

        @Test
        void salaryBandDistribution() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT
                        CASE
                            WHEN salary >= 90000 THEN 'high'
                            WHEN salary >= 70000 THEN 'mid'
                            ELSE 'low'
                        END AS band,
                        COUNT(*) AS headcount,
                        AVG(salary) AS avg_salary
                    FROM $r
                    GROUP BY CASE
                        WHEN salary >= 90000 THEN 'high'
                        WHEN salary >= 70000 THEN 'mid'
                        ELSE 'low'
                    END
                    ORDER BY avg_salary DESC
                    """, employees);
            assertEquals(3, arr(result).size());
            assertEquals("high", str(f(arr(result).getFirst(), "band")));
        }

        @Test
        void topEarnerPerDeptUsingWindowFunction() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT name, dept, salary, dept_rank
                    FROM (
                        SELECT name, dept, salary,
                            RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank
                        FROM $r
                    )
                    WHERE dept_rank = 1
                    ORDER BY salary DESC
                    """, employees);
            var rows = arr(result);
            assertEquals(3, rows.size());
            for (JsonValue row : rows) {
                assertEquals(1, num(f(row, "dept_rank")).intValue());
            }
        }

        @Test
        void managersVsIndividualContributors() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT manager_id, COUNT(*) AS direct_reports
                    FROM $r
                    WHERE manager_id IS NOT NULL
                    GROUP BY manager_id
                    HAVING COUNT(*) >= 1
                    ORDER BY direct_reports DESC
                    """, employees);
            assertEquals(3, arr(result).size());
            assertEquals(2, num(f(arr(result).getFirst(), "direct_reports")).intValue());
        }

        @Test
        void deptWithFunctionsEverywhere() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT
                        UPPER(dept) AS department,
                        COUNT(*) AS cnt,
                        ROUND(AVG(salary), 2) AS avg_salary
                    FROM $r
                    WHERE LENGTH(name) > 3
                    GROUP BY UPPER(dept)
                    ORDER BY ROUND(AVG(salary), 2) DESC
                    """, employees);
            var rows = arr(result);
            assertEquals(3, rows.size());
            assertEquals("ENG", str(f(rows.getFirst(), "department")));
            assertEquals(1, num(f(rows.getFirst(), "cnt")).intValue());
        }

        @Test
        void complexFilterWithMixedConditions() {
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT name, dept, salary
                    FROM $r
                    WHERE (dept IN ('eng', 'marketing') AND salary > 75000)
                       OR (dept = 'hr' AND manager_id IS NULL)
                    ORDER BY salary DESC
                    """, employees);
            var rows = arr(result);
            assertEquals(5, rows.size());
            assertEquals("Eve", str(f(rows.getFirst(), "name")));
        }

        @Test
        void subqueryWithWindowAndOuterFilter() {
            // Outer ORDER BY is required to guarantee row_num ordering — a window's
            // OVER (ORDER BY ...) only governs ranking, not the final result order.
            JsonValue result = SQL4Json.queryAsJsonValue("""
                    SELECT name, salary, row_num
                    FROM (
                        SELECT name, salary,
                            ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num
                        FROM $r
                    )
                    WHERE row_num <= 3
                    ORDER BY row_num
                    """, employees);
            assertEquals(3, arr(result).size());
            assertEquals(1, num(f(arr(result).getFirst(), "row_num")).intValue());
        }
    }
}
