package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.json.JsonNullValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class LruQueryResultCacheTest {

    @Test
    void get_cacheMiss_returnsNull() {
        QueryResultCache cache = new LruQueryResultCache(10);
        assertNull(cache.get("SELECT * FROM $r"));
    }

    @Test
    void putAndGet_returnsStoredValue() {
        QueryResultCache cache = new LruQueryResultCache(10);
        JsonValue value = new JsonStringValue("hello");
        cache.put("SELECT name FROM $r", value);
        assertSame(value, cache.get("SELECT name FROM $r"));
    }

    @Test
    void size_reflectsEntryCount() {
        QueryResultCache cache = new LruQueryResultCache(10);
        assertEquals(0, cache.size());

        cache.put("q1", new JsonStringValue("v1"));
        assertEquals(1, cache.size());

        cache.put("q2", new JsonStringValue("v2"));
        assertEquals(2, cache.size());

        cache.put("q3", new JsonStringValue("v3"));
        assertEquals(3, cache.size());
    }

    @Test
    void lruEviction_exceedingCapacity() {
        QueryResultCache cache = new LruQueryResultCache(3);

        cache.put("q1", new JsonStringValue("v1"));
        cache.put("q2", new JsonStringValue("v2"));
        cache.put("q3", new JsonStringValue("v3"));

        // Adding a 4th entry should evict q1 (the eldest)
        cache.put("q4", new JsonStringValue("v4"));

        assertEquals(3, cache.size());
        assertNull(cache.get("q1"), "q1 should have been evicted");
        assertNotNull(cache.get("q2"));
        assertNotNull(cache.get("q3"));
        assertNotNull(cache.get("q4"));
    }

    @Test
    void lruEviction_accessOrderUpdatesRecency() {
        QueryResultCache cache = new LruQueryResultCache(3);

        cache.put("q1", new JsonStringValue("v1"));
        cache.put("q2", new JsonStringValue("v2"));
        cache.put("q3", new JsonStringValue("v3"));

        // Access q1 to make it recently used; q2 is now the eldest
        cache.get("q1");

        // Adding q4 should evict q2 (eldest after q1 was accessed)
        cache.put("q4", new JsonStringValue("v4"));

        assertEquals(3, cache.size());
        assertNotNull(cache.get("q1"), "q1 should still be present after access");
        assertNull(cache.get("q2"), "q2 should have been evicted as eldest");
        assertNotNull(cache.get("q3"));
        assertNotNull(cache.get("q4"));
    }

    @Test
    void clear_emptiesCache() {
        QueryResultCache cache = new LruQueryResultCache(10);

        cache.put("q1", new JsonStringValue("v1"));
        cache.put("q2", JsonNullValue.INSTANCE);
        assertEquals(2, cache.size());

        cache.clear();

        assertEquals(0, cache.size());
        assertNull(cache.get("q1"));
        assertNull(cache.get("q2"));
    }

    @Test
    void concurrentAccess_noExceptions() throws Exception {
        QueryResultCache cache = new LruQueryResultCache(50);

        int threadCount = 8;
        int opsPerThread = 200;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            int threadIdx = t;
            futures.add(executor.submit(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    String key = "q_" + threadIdx + "_" + i;
                    JsonValue value = new JsonStringValue("v_" + threadIdx + "_" + i);

                    assertDoesNotThrow(() -> cache.put(key, value));
                    assertDoesNotThrow(() -> cache.get(key));
                    assertDoesNotThrow(cache::size);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(); // propagates assertion errors
        }

        executor.shutdown();
        assertTrue(cache.size() <= 50, "Cache size should not exceed max capacity");
    }
}
