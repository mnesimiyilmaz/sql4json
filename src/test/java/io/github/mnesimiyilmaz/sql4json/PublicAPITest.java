// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PublicAPITest {

    @Nested
    class StaticApiTests {

        private final String jsonArray = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]";

        // ── query(String, String) ──────────────────────────────────────────

        @Test
        void query_selectAll_returnsJsonString() {
            String result = SQL4Json.query("SELECT * FROM $r", jsonArray);
            assertNotNull(result);
            assertTrue(result.contains("Alice"));
            assertTrue(result.contains("Bob"));
        }

        @Test
        void query_whereFilter_filtersCorrectly() {
            String result = SQL4Json.query("SELECT * FROM $r WHERE age > 25", jsonArray);
            assertTrue(result.contains("Alice"));
            assertFalse(result.contains("Bob"));
        }

        // ── queryAsJsonValue(String, String) ────────────────────────────────

        @Test
        void queryAsJsonValue_stringData_returnsJsonValue() {
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r", jsonArray);
            assertTrue(result.isArray());
            assertEquals(2, result.asArray().get().size());
        }

        // ── queryAsJsonValue(String, JsonValue) ─────────────────────────────

        @Test
        void queryAsJsonValue_jsonValueData_works() {
            JsonValue data = SQL4Json.queryAsJsonValue("SELECT * FROM $r", jsonArray);
            JsonValue result = SQL4Json.queryAsJsonValue("SELECT name FROM $r WHERE age > 25", data);
            assertTrue(result.isArray());
            assertEquals(1, result.asArray().get().size());
        }

        // ── Input validation ─────────────────────────────────────────────────

        @Test
        void query_nullSql_throws() {
            assertThrows(SQL4JsonException.class, () -> SQL4Json.query(null, jsonArray));
        }

        @Test
        void query_emptySql_throws() {
            assertThrows(SQL4JsonException.class, () -> SQL4Json.query("", jsonArray));
        }

        @Test
        void query_nullData_throws() {
            assertThrows(SQL4JsonException.class, () -> SQL4Json.query("SELECT * FROM $r", (String) null));
        }

        // ── FROM subquery works ──────────────────────────────────────────────

        @Test
        void query_fromSubquery_works() {
            String result = SQL4Json.query("SELECT name FROM (SELECT * FROM $r WHERE age > 25)", jsonArray);
            assertTrue(result.contains("Alice"));
            assertFalse(result.contains("Bob"));
        }
    }

    @Nested
    class PreparedQueryTests {

        @Test
        void executeString_sameAsDirectQuery() {
            String json = "[{\"name\":\"Alice\",\"age\":30}]";
            PreparedQuery prepared = SQL4Json.prepare("SELECT name FROM $r");

            String expected = SQL4Json.query("SELECT name FROM $r", json);
            String actual = prepared.execute(json);
            assertEquals(expected, actual);
        }

        @Test
        void executeJsonValue_returnsJsonValue() {
            String json = "[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]";
            PreparedQuery prepared = SQL4Json.prepare("SELECT * FROM $r");
            JsonValue data = SQL4Json.queryAsJsonValue("SELECT * FROM $r", json);

            JsonValue result = prepared.execute(data);
            assertTrue(result.isArray());
            assertEquals(2, result.asArray().get().size());
        }

        @Test
        void executeMultipleTimes_differentData() {
            PreparedQuery prepared = SQL4Json.prepare("SELECT * FROM $r WHERE age > 25");
            String json1 = "[{\"name\":\"Alice\",\"age\":30}]";
            String json2 = "[{\"name\":\"Bob\",\"age\":20},{\"name\":\"Carol\",\"age\":35}]";

            String r1 = prepared.execute(json1);
            assertTrue(r1.contains("Alice"));

            String r2 = prepared.execute(json2);
            assertTrue(r2.contains("Carol"));
            assertFalse(r2.contains("Bob"));
        }

        @Test
        void threadSafety_concurrentExecution() throws Exception {
            PreparedQuery prepared = SQL4Json.prepare("SELECT * FROM $r WHERE age > 25");
            int threadCount = 8;
            int iterationsPerThread = 50;

            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            List<Future<String>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                futures.add(pool.submit(() -> {
                    List<String> results = new ArrayList<>();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        String json =
                                "[{\"name\":\"T" + Thread.currentThread().threadId() + "\",\"age\":" + (26 + i) + "}]";
                        results.add(prepared.execute(json));
                    }
                    return String.join(",", results);
                }));
            }

            pool.shutdown();
            List<String> allResults = new ArrayList<>();
            for (Future<String> f : futures) {
                allResults.add(f.get());
            }

            assertEquals(threadCount, allResults.size());
            for (String batch : allResults) {
                assertFalse(batch.isEmpty());
            }
        }

        @Test
        void preparedQuery_withSubquery() {
            PreparedQuery prepared = SQL4Json.prepare("SELECT name FROM (SELECT * FROM $r WHERE age > 25)");
            String json = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]";
            String result = prepared.execute(json);
            assertTrue(result.contains("Alice"));
            assertFalse(result.contains("Bob"));
        }
    }

    @Nested
    class NonDeterministicDetectionTests {

        @Test
        void regularQuery_isDeterministic() {
            QueryDefinition qd = QueryParser.parse("SELECT * FROM $r WHERE age > 25");
            assertFalse(qd.containsNonDeterministic());
        }

        @Test
        void queryWithNow_isNonDeterministic() {
            QueryDefinition qd = QueryParser.parse("SELECT * FROM $r WHERE age > NOW()");
            assertTrue(qd.containsNonDeterministic());
        }

        @Test
        void selectAllQuery_isDeterministic() {
            QueryDefinition qd = QueryParser.parse("SELECT * FROM $r");
            assertFalse(qd.containsNonDeterministic());
        }

        @Test
        void queryWithNowInFunction_isNonDeterministic() {
            QueryDefinition qd = QueryParser.parse("SELECT * FROM $r WHERE created > DATE_ADD(NOW(), -1, 'DAY')");
            assertTrue(qd.containsNonDeterministic());
        }
    }
}
