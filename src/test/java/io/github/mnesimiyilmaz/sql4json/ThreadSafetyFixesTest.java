// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.mnesimiyilmaz.sql4json.json.JsonParser;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class ThreadSafetyFixesTest {

    // ── JsonValue immutability ──────────────────────────────────────────

    @Test
    void jsonObjectValue_fieldsAreUnmodifiable() {
        JsonValue parsed = JsonParser.parse("{\"name\":\"Alice\",\"age\":30}");
        Map<String, JsonValue> fields = parsed.asObject().get();
        assertThrows(UnsupportedOperationException.class, () -> fields.put("hack", null));
    }

    @Test
    void jsonArrayValue_elementsAreUnmodifiable() {
        JsonValue parsed = JsonParser.parse("[{\"name\":\"Alice\"}]");
        List<JsonValue> elements = parsed.asArray().get();
        assertThrows(UnsupportedOperationException.class, () -> elements.add(null));
    }

    @Test
    void nestedJsonValue_deeplyUnmodifiable() {
        JsonValue parsed = JsonParser.parse("{\"profile\":{\"address\":{\"city\":\"Istanbul\"}}}");

        // Top-level object
        Map<String, JsonValue> root = parsed.asObject().get();
        assertThrows(UnsupportedOperationException.class, () -> root.put("x", null));

        // Nested object
        Map<String, JsonValue> profile = root.get("profile").asObject().get();
        assertThrows(UnsupportedOperationException.class, () -> profile.put("x", null));

        // Deeply nested object
        Map<String, JsonValue> address = profile.get("address").asObject().get();
        assertThrows(UnsupportedOperationException.class, () -> address.put("x", null));
    }

    // ── LikeConditionHandler concurrency ────────────────────────────────

    @Test
    void likeConditionHandler_concurrentPatternCompilation() throws Exception {
        String json = """
                [{"name":"Alice"},{"name":"Bob"},{"name":"Charlie"},{"name":"Diana"}]
                """;

        int threadCount = 8;
        int patternsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            int threadIdx = t;
            futures.add(executor.submit(() -> {
                for (int p = 0; p < patternsPerThread; p++) {
                    // Each thread uses a mix of patterns to stress the shared cache
                    String pattern =
                            switch (p % 4) {
                                case 0 -> "Ali%";
                                case 1 -> "%ob";
                                case 2 -> "Char%";
                                default -> "%" + threadIdx + "_" + p + "%";
                            };
                    String sql = "SELECT * FROM $r WHERE name LIKE '" + pattern + "'";
                    assertDoesNotThrow(
                            () -> SQL4Json.query(sql, json), "Thread " + threadIdx + " pattern " + p + " failed");
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(); // propagates assertion errors
        }

        executor.shutdown();
    }
}
