package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SQLClauseTest {

    @Nested
    class DistinctTests {

        private final String data = """
                [
                    {"name": "Alice", "dept": "IT", "status": "active"},
                    {"name": "Bob",   "dept": "IT", "status": "active"},
                    {"name": "Charlie", "dept": "HR", "status": "inactive"},
                    {"name": "Diana", "dept": "HR", "status": "active"},
                    {"name": "Eve",   "dept": "IT", "status": "inactive"},
                    {"name": "Frank", "dept": "IT", "status": null}
                ]
                """;

        private JsonValue query(String sql) {
            return SQL4Json.queryAsJsonValue(sql, data);
        }

        @Test
        void distinct_singleColumn_deduplicates() {
            var result = query("SELECT DISTINCT dept FROM $r");
            assertEquals(2, result.asArray().orElseThrow().size());
        }

        @Test
        void distinct_multiColumn_deduplicates() {
            var result = query("SELECT DISTINCT dept, status FROM $r");
            assertEquals(5, result.asArray().orElseThrow().size());
        }

        @Test
        void distinct_allUnique_noChange() {
            var result = query("SELECT DISTINCT name FROM $r");
            assertEquals(6, result.asArray().orElseThrow().size());
        }

        @Test
        void distinct_withOrderBy() {
            var result = query("SELECT DISTINCT dept FROM $r ORDER BY dept ASC");
            var rows = result.asArray().orElseThrow();
            assertEquals(2, rows.size());
            assertEquals("HR", rows.get(0).asObject().orElseThrow().get("dept").asString().orElseThrow());
            assertEquals("IT", rows.get(1).asObject().orElseThrow().get("dept").asString().orElseThrow());
        }

        @Test
        void distinct_withLimit() {
            var result = query("SELECT DISTINCT dept FROM $r LIMIT 1");
            assertEquals(1, result.asArray().orElseThrow().size());
        }

        @Test
        void distinct_withNulls_treatsNullAsValue() {
            var result = query("SELECT DISTINCT status FROM $r");
            assertEquals(3, result.asArray().orElseThrow().size());
        }

        @Test
        void distinct_withGroupBy() {
            var result = query("SELECT DISTINCT dept, COUNT(*) AS cnt FROM $r GROUP BY dept");
            assertEquals(2, result.asArray().orElseThrow().size());
        }
    }

    @Nested
    class LimitOffsetTests {

        private final String data = """
                [
                    {"name": "Alice", "age": 25},
                    {"name": "Bob", "age": 30},
                    {"name": "Charlie", "age": 35},
                    {"name": "Diana", "age": 20},
                    {"name": "Eve", "age": 28}
                ]
                """;

        private JsonValue query(String sql) {
            return SQL4Json.queryAsJsonValue(sql, data);
        }

        @Test
        void limit_returnsFirstN() {
            var result = query("SELECT * FROM $r LIMIT 3");
            assertEquals(3, result.asArray().orElseThrow().size());
        }

        @Test
        void limit_withOrderBy_returnsTopN() {
            var result = query("SELECT * FROM $r ORDER BY age ASC LIMIT 2");
            var rows = result.asArray().orElseThrow();
            assertEquals(2, rows.size());
            assertEquals(20, rows.get(0).asObject().orElseThrow().get("age").asNumber().orElseThrow().intValue());
            assertEquals(25, rows.get(1).asObject().orElseThrow().get("age").asNumber().orElseThrow().intValue());
        }

        @Test
        void limit_withOffset_skipsAndTakes() {
            var result = query("SELECT * FROM $r ORDER BY age ASC LIMIT 2 OFFSET 2");
            var rows = result.asArray().orElseThrow();
            assertEquals(2, rows.size());
            assertEquals(28, rows.get(0).asObject().orElseThrow().get("age").asNumber().orElseThrow().intValue());
            assertEquals(30, rows.get(1).asObject().orElseThrow().get("age").asNumber().orElseThrow().intValue());
        }

        @Test
        void limit_zero_returnsEmpty() {
            var result = query("SELECT * FROM $r LIMIT 0");
            assertEquals(0, result.asArray().orElseThrow().size());
        }

        @Test
        void limit_largerThanData_returnsAll() {
            var result = query("SELECT * FROM $r LIMIT 100");
            assertEquals(5, result.asArray().orElseThrow().size());
        }

        @Test
        void limit_offsetBeyondData_returnsEmpty() {
            var result = query("SELECT * FROM $r LIMIT 10 OFFSET 100");
            assertEquals(0, result.asArray().orElseThrow().size());
        }

        @Test
        void limit_withWhereClause() {
            var result = query("SELECT * FROM $r WHERE age > 20 ORDER BY age ASC LIMIT 2");
            var rows = result.asArray().orElseThrow();
            assertEquals(2, rows.size());
            assertEquals(25, rows.get(0).asObject().orElseThrow().get("age").asNumber().orElseThrow().intValue());
        }

        @Test
        void limit_withoutOrderBy_returnsNRows() {
            var result = query("SELECT name FROM $r LIMIT 3");
            var rows = result.asArray().orElseThrow();
            assertEquals(3, rows.size());
            assertEquals(1, rows.get(0).asObject().orElseThrow().size());
        }
    }

    @Nested
    class InBetweenConditionTests {

        private final String data = """
                [
                    {"name": "Alice", "age": 25, "status": null},
                    {"name": "Bob", "age": 30, "status": "active"},
                    {"name": "Charlie", "age": 35, "status": "inactive"},
                    {"name": "Diana", "age": 20, "status": "active"}
                ]
                """;

        private JsonValue query(String sql) {
            return SQL4Json.queryAsJsonValue(sql, data);
        }

        @Test
        void in_withStringValues_returnsMatches() {
            var result = query("SELECT * FROM $r WHERE name IN ('Alice', 'Bob')");
            assertEquals(2, result.asArray().orElseThrow().size());
        }

        @Test
        void in_withNumericValues_returnsMatches() {
            var result = query("SELECT * FROM $r WHERE age IN (25, 35)");
            assertEquals(2, result.asArray().orElseThrow().size());
        }

        @Test
        void in_noMatches_returnsEmpty() {
            var result = query("SELECT * FROM $r WHERE name IN ('Nobody')");
            assertEquals(0, result.asArray().orElseThrow().size());
        }

        @Test
        void in_nullFieldValue_excluded() {
            var result = query("SELECT * FROM $r WHERE status IN ('active')");
            assertEquals(2, result.asArray().orElseThrow().size());
        }

        @Test
        void notIn_returnsNonMatches() {
            var result = query("SELECT * FROM $r WHERE name NOT IN ('Alice', 'Bob')");
            assertEquals(2, result.asArray().orElseThrow().size());
        }

        @Test
        void in_combinedWithAnd() {
            var result = query("SELECT * FROM $r WHERE age IN (25, 30) AND status = 'active'");
            assertEquals(1, result.asArray().orElseThrow().size());
        }

        // --- BETWEEN ---

        @Test
        void between_inclusive_returnsMatches() {
            var result = query("SELECT * FROM $r WHERE age BETWEEN 25 AND 35");
            assertEquals(3, result.asArray().orElseThrow().size());
        }

        @Test
        void between_exactBounds_returnsMatch() {
            var result = query("SELECT * FROM $r WHERE age BETWEEN 25 AND 25");
            assertEquals(1, result.asArray().orElseThrow().size());
        }

        @Test
        void between_noMatches() {
            var result = query("SELECT * FROM $r WHERE age BETWEEN 50 AND 100");
            assertEquals(0, result.asArray().orElseThrow().size());
        }

        @Test
        void notBetween_returnsNonMatches() {
            var result = query("SELECT * FROM $r WHERE age NOT BETWEEN 25 AND 35");
            assertEquals(1, result.asArray().orElseThrow().size());
        }

        @Test
        void between_nullFieldValue_excluded() {
            var result = query("SELECT * FROM $r WHERE status BETWEEN 'a' AND 'z'");
            assertEquals(3, result.asArray().orElseThrow().size());
        }

        @Test
        void between_combinedWithLogicalAnd() {
            var result = query("SELECT * FROM $r WHERE age BETWEEN 20 AND 30 AND status = 'active'");
            assertEquals(2, result.asArray().orElseThrow().size());
        }
    }

    @Nested
    class NotLikeTests {

        private final String data = """
                [
                    {"name": "Alice"},
                    {"name": "Bob"},
                    {"name": "Alicia"},
                    {"name": null}
                ]
                """;

        private JsonValue query(String sql) {
            return SQL4Json.queryAsJsonValue(sql, data);
        }

        @Test
        void notLike_excludesMatches() {
            var result = query("SELECT * FROM $r WHERE name NOT LIKE 'Ali%'");
            assertEquals(1, result.asArray().orElseThrow().size());
        }

        @Test
        void notLike_noExclusions() {
            var result = query("SELECT * FROM $r WHERE name NOT LIKE '%xyz%'");
            assertEquals(3, result.asArray().orElseThrow().size());
        }

        @Test
        void notLike_combinedWithAnd() {
            var result = query("SELECT * FROM $r WHERE name NOT LIKE 'A%' AND name IS NOT NULL");
            assertEquals(1, result.asArray().orElseThrow().size());
        }

        @Test
        void notLike_nullFieldExcluded() {
            var result = query("SELECT * FROM $r WHERE name NOT LIKE '%ob%'");
            assertEquals(2, result.asArray().orElseThrow().size());
        }
    }
}
