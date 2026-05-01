package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class SQL4JsonEngineTest {

    private static final String JSON =
            "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]";

    @Test
    void query_returnsJsonString() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        String result = engine.query("SELECT * FROM $r");
        assertTrue(result.contains("Alice"));
        assertTrue(result.contains("Bob"));
    }

    @Test
    void queryAsJsonValue_returnsJsonValue() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        JsonValue result = engine.queryAsJsonValue("SELECT * FROM $r");
        assertTrue(result.isArray());
        assertEquals(2, result.asArray().get().size());
    }

    @Test
    void query_withWhere_filtersCorrectly() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        String result = engine.query("SELECT * FROM $r WHERE age > 25");
        assertTrue(result.contains("Alice"));
        assertFalse(result.contains("Bob"));
    }

    @Test
    void query_multipleQueriesSameData() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        String r1 = engine.query("SELECT name FROM $r");
        String r2 = engine.query("SELECT * FROM $r WHERE age > 25");
        assertTrue(r1.contains("Alice") && r1.contains("Bob"));
        assertTrue(r2.contains("Alice") && !r2.contains("Bob"));
    }

    @Test
    void data_fromJsonValue() {
        JsonValue data = SQL4Json.queryAsJsonValue("SELECT * FROM $r", JSON);
        SQL4JsonEngine engine = SQL4Json.engine().data(data).build();
        String result = engine.query("SELECT * FROM $r");
        assertTrue(result.contains("Alice"));
    }

    @Test
    void build_withoutData_throws() {
        var builder = SQL4Json.engine();
        assertThrows(SQL4JsonException.class, builder::build);
    }

    @Test
    void noCache_cacheSizeReturnsZero() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        assertEquals(0, engine.cacheSize());
    }

    @Test
    void noCache_clearCacheIsNoOp() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        engine.clearCache();
        assertEquals(0, engine.cacheSize());
    }

    @Test
    void withDefaultCache_cachesResults() {
        var settings = Sql4jsonSettings.builder().cache(c -> c.queryResultCacheEnabled(true)).build();
        SQL4JsonEngine engine = SQL4Json.engine().settings(settings).data(JSON).build();
        assertEquals(0, engine.cacheSize());
        engine.query("SELECT * FROM $r");
        assertEquals(1, engine.cacheSize());
        engine.query("SELECT name FROM $r");
        assertEquals(2, engine.cacheSize());
        engine.query("SELECT * FROM $r");
        assertEquals(2, engine.cacheSize()); // cache hit
    }

    @Test
    void withSizedCache_lruEviction() {
        var settings = Sql4jsonSettings.builder()
                .cache(c -> c.queryResultCacheEnabled(true).queryResultCacheSize(2)).build();
        SQL4JsonEngine engine = SQL4Json.engine().settings(settings).data(JSON).build();
        engine.query("SELECT * FROM $r");
        engine.query("SELECT name FROM $r");
        assertEquals(2, engine.cacheSize());
        engine.query("SELECT age FROM $r");
        assertEquals(2, engine.cacheSize()); // evicted oldest
    }

    @Test
    void clearCache_resetsSize() {
        var settings = Sql4jsonSettings.builder().cache(c -> c.queryResultCacheEnabled(true)).build();
        SQL4JsonEngine engine = SQL4Json.engine().settings(settings).data(JSON).build();
        engine.query("SELECT * FROM $r");
        engine.query("SELECT name FROM $r");
        assertEquals(2, engine.cacheSize());
        engine.clearCache();
        assertEquals(0, engine.cacheSize());
    }

    @Test
    void cachedResult_matchesUncachedResult() {
        var settings = Sql4jsonSettings.builder().cache(c -> c.queryResultCacheEnabled(true)).build();
        SQL4JsonEngine cached = SQL4Json.engine().settings(settings).data(JSON).build();
        SQL4JsonEngine uncached = SQL4Json.engine().data(JSON).build();
        String sql = "SELECT name FROM $r WHERE age > 25";
        String r1 = cached.query(sql);
        String r2 = cached.query(sql); // cache hit
        String r3 = uncached.query(sql);
        assertEquals(r1, r2);
        assertEquals(r1, r3);
    }

    @Test
    void withCustomCache_usesProvidedImplementation() {
        var customCache = new QueryResultCache() {
            int putCount = 0;

            @Override
            public JsonValue get(String sql) {
                return null;
            }

            @Override
            public void put(String sql, JsonValue result) {
                putCount++;
            }

            @Override
            public void clear() { /* no-op for test stub */ }

            @Override
            public int size() {
                return putCount;
            }
        };
        var settings = Sql4jsonSettings.builder().cache(c -> c.customCache(customCache)).build();
        SQL4JsonEngine engine = SQL4Json.engine()
                .settings(settings).data(JSON).build();
        engine.query("SELECT * FROM $r");
        assertEquals(1, customCache.putCount);
    }

    @Test
    void queryWithNow_bypassesCache() {
        String jsonWithDate = "[{\"created_at\":\"2099-12-31T23:59:59\"}]";
        var settings = Sql4jsonSettings.builder().cache(c -> c.queryResultCacheEnabled(true)).build();
        SQL4JsonEngine engine = SQL4Json.engine()
                .settings(settings).data(jsonWithDate).build();
        engine.query("SELECT * FROM $r");
        assertEquals(1, engine.cacheSize());
        engine.query("SELECT * FROM $r WHERE TO_DATE(created_at) > NOW()");
        assertEquals(1, engine.cacheSize());
    }

    @Test
    void data_nullString_throws() {
        var builder = SQL4Json.engine();
        assertThrows(SQL4JsonException.class,
                () -> builder.data((String) null));
    }

    @Test
    void data_nullJsonValue_throws() {
        var builder = SQL4Json.engine();
        assertThrows(SQL4JsonException.class,
                () -> builder.data((JsonValue) null));
    }

    @Test
    void queryAsJsonValue_nullJsonValue_throws() {
        assertThrows(SQL4JsonException.class,
                () -> SQL4Json.queryAsJsonValue("SELECT * FROM $r", (JsonValue) null));
    }

    @Test
    void concurrentQueries_noExceptions() throws Exception {
        var settings = Sql4jsonSettings.builder().cache(c -> c.queryResultCacheEnabled(true)).build();
        SQL4JsonEngine engine = SQL4Json.engine()
                .settings(settings).data(JSON).build();
        int threadCount = 8;
        int queriesPerThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            int threadId = t;
            futures.add(pool.submit(() -> {
                try {
                    for (int i = 0; i < queriesPerThread; i++) {
                        String sql = (threadId % 2 == 0)
                                ? "SELECT * FROM $r"
                                : "SELECT name FROM $r WHERE age > 25";
                        String result = engine.query(sql);
                        assertNotNull(result);
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            }));
        }
        pool.shutdown();
        for (Future<?> f : futures) f.get();
        assertTrue(errors.isEmpty(), "Concurrent engine queries failed: " + errors);
    }

    // ── Codec integration tests ────────────────────────────────────────────

    @Test
    void engine_builder_with_custom_codec() {
        JsonCodec codec = new DefaultJsonCodec();
        SQL4JsonEngine engine = SQL4Json.engine()
                .settings(Sql4jsonSettings.builder().codec(codec).build())
                .data("[{\"x\":1}]")
                .build();
        String result = engine.query("SELECT * FROM $r");
        assertNotNull(result);
    }

    @Test
    void query_with_custom_codec() {
        JsonCodec codec = new DefaultJsonCodec();
        var settings = Sql4jsonSettings.builder().codec(codec).build();
        String result = SQL4Json.query("SELECT * FROM $r", "[{\"a\":1}]", settings);
        assertNotNull(result);
    }

    @Test
    void queryAsJsonValue_with_custom_codec() {
        JsonCodec codec = new DefaultJsonCodec();
        var settings = Sql4jsonSettings.builder().codec(codec).build();
        JsonValue result = SQL4Json.queryAsJsonValue("SELECT * FROM $r", "[{\"a\":1}]", settings);
        assertTrue(result.isArray());
    }

    @Test
    void prepare_with_custom_codec() {
        JsonCodec codec = new DefaultJsonCodec();
        var settings = Sql4jsonSettings.builder().codec(codec).build();
        PreparedQuery q = SQL4Json.prepare("SELECT * FROM $r", settings);
        String result = q.execute("[{\"a\":1}]");
        assertNotNull(result);
    }

    // ── SQL4JsonEngineBuilder named source branches ──────────────────────

    @Test
    void builder_namedSource_string() {
        // SQL4JsonEngineBuilder: data(String, String) branch
        SQL4JsonEngine engine = SQL4Json.engine()
                .data("users", "[{\"id\":1,\"name\":\"Alice\"}]")
                .data("orders", "[{\"user_id\":1,\"product\":\"Widget\"}]")
                .build();
        String result = engine.query(
                "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id");
        assertTrue(result.contains("Alice"));
    }

    @Test
    void builder_namedSource_jsonValue() {
        // SQL4JsonEngineBuilder: data(String, JsonValue) branch
        JsonValue usersData = SQL4Json.queryAsJsonValue("SELECT * FROM $r",
                "[{\"id\":1,\"name\":\"Alice\"}]");
        JsonValue ordersData = SQL4Json.queryAsJsonValue("SELECT * FROM $r",
                "[{\"user_id\":1,\"product\":\"Widget\"}]");
        SQL4JsonEngine engine = SQL4Json.engine()
                .data("users", usersData)
                .data("orders", ordersData)
                .build();
        String result = engine.query(
                "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id");
        assertTrue(result.contains("Alice"));
    }

    @Test
    void builder_namedSource_nullName_throws() {
        assertThrows(SQL4JsonException.class,
                () -> SQL4Json.engine().data(null, "[{\"id\":1}]"));
    }

    @Test
    void builder_namedSource_blankName_throws() {
        assertThrows(SQL4JsonException.class,
                () -> SQL4Json.engine().data("  ", "[{\"id\":1}]"));
    }

    @Test
    void joinQuery_onSingleSourceEngine_throws() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        var ex = assertThrows(io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException.class,
                () -> engine.query(
                        "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id"));
        assertTrue(ex.getMessage().contains("JOIN queries require named data sources"));
    }

    @Test
    void parameterized_queryAsJsonValue_namedParam() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        JsonValue result = engine.queryAsJsonValue(
                "SELECT name FROM $r WHERE age > :min",
                BoundParameters.named().bind("min", 25));
        assertNotNull(result);
        assertEquals(1, result.asArray().orElseThrow().size());
    }

    @Test
    void parameterized_queryAsJsonValue_noPlaceholders_fallsThroughToCachedPath() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        JsonValue result = engine.queryAsJsonValue(
                "SELECT name FROM $r WHERE age > 25",
                BoundParameters.named());
        assertNotNull(result);
        assertEquals(1, result.asArray().orElseThrow().size());
    }

    @Test
    void parameterized_queryAsJsonValue_positionalParam() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        JsonValue result = engine.queryAsJsonValue(
                "SELECT name FROM $r WHERE age > ?", BoundParameters.of(25));
        assertEquals(1, result.asArray().orElseThrow().size());
    }

    @Test
    void parameterized_queryAsJsonValue_limitParam() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        JsonValue result = engine.queryAsJsonValue(
                "SELECT name FROM $r LIMIT ?", BoundParameters.of(1));
        assertEquals(1, result.asArray().orElseThrow().size());
    }

    @Test
    void parameterized_queryAsJsonValue_offsetParam() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        JsonValue result = engine.queryAsJsonValue(
                "SELECT name FROM $r LIMIT 10 OFFSET ?", BoundParameters.of(1));
        assertEquals(1, result.asArray().orElseThrow().size());
    }

    @Test
    void builder_namedSource_nullData_throws() {
        assertThrows(SQL4JsonException.class,
                () -> SQL4Json.engine().data("users", (String) null));
    }

    @Test
    void builder_namedSource_nullJsonValueData_throws() {
        assertThrows(SQL4JsonException.class,
                () -> SQL4Json.engine().data("users", (JsonValue) null));
    }

    @Test
    void builder_bothUnnamedAndNamed() {
        // SQL4JsonEngineBuilder: hasUnnamed=true, hasNamed=true
        SQL4JsonEngine engine = SQL4Json.engine()
                .data("[{\"x\":1}]")
                .data("users", "[{\"id\":1}]")
                .data("orders", "[{\"user_id\":1}]")
                .build();
        String result = engine.query("SELECT * FROM $r");
        assertTrue(result.contains("x"));
    }

    @Test
    void builder_directDataOverridesRawJson() {
        // SQL4JsonEngineBuilder: data(JsonValue) sets directData, clears rawJson
        JsonValue data = SQL4Json.queryAsJsonValue("SELECT * FROM $r",
                "[{\"name\":\"Direct\"}]");
        SQL4JsonEngine engine = SQL4Json.engine()
                .data("[{\"name\":\"Raw\"}]")
                .data(data)
                .build();
        String result = engine.query("SELECT name FROM $r");
        assertTrue(result.contains("Direct"));
    }

    @Test
    void builder_rawJsonOverridesDirectData() {
        // SQL4JsonEngineBuilder: data(String) sets rawJson, clears directData
        JsonValue data = SQL4Json.queryAsJsonValue("SELECT * FROM $r",
                "[{\"name\":\"Direct\"}]");
        SQL4JsonEngine engine = SQL4Json.engine()
                .data(data)
                .data("[{\"name\":\"Raw\"}]")
                .build();
        String result = engine.query("SELECT name FROM $r");
        assertTrue(result.contains("Raw"));
    }

    @Test
    void builder_mixedNamedSources_rawAndDirect() {
        // SQL4JsonEngineBuilder: resolveNamedSources with both raw + direct
        JsonValue ordersData = SQL4Json.queryAsJsonValue("SELECT * FROM $r",
                "[{\"user_id\":1,\"product\":\"Widget\"}]");
        SQL4JsonEngine engine = SQL4Json.engine()
                .data("users", "[{\"id\":1,\"name\":\"Alice\"}]")
                .data("orders", ordersData)
                .build();
        String result = engine.query(
                "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id");
        assertTrue(result.contains("Alice"));
        assertTrue(result.contains("Widget"));
    }

    @Test
    void engine_queryAsJsonValue_cache_hit() {
        // SQL4JsonEngine: cache hit returns cached JsonValue
        var settings = Sql4jsonSettings.builder()
                .cache(c -> c.queryResultCacheEnabled(true))
                .build();
        SQL4JsonEngine engine = SQL4Json.engine().settings(settings).data(JSON).build();
        JsonValue r1 = engine.queryAsJsonValue("SELECT * FROM $r");
        JsonValue r2 = engine.queryAsJsonValue("SELECT * FROM $r");
        assertEquals(r1.toString(), r2.toString());
        assertEquals(1, engine.cacheSize());
    }

    @Test
    void preparedQuery_executeJsonValue() {
        // PreparedQuery: execute(JsonValue) branch
        JsonValue data = SQL4Json.queryAsJsonValue("SELECT * FROM $r", JSON);
        PreparedQuery q = SQL4Json.prepare("SELECT name FROM $r");
        JsonValue result = q.execute(data);
        assertNotNull(result);
        assertTrue(result.isArray());
    }

    @Test
    void engine_nullSettings_throws() {
        assertThrows(NullPointerException.class,
                () -> SQL4Json.engine().settings(null));
    }

    @Test
    void engine_subquery_reparsesRawJson() {
        // SQL4JsonEngine: resolveData with subquery forces re-parse from rawJson
        SQL4JsonEngine engine = SQL4Json.engine()
                .data("[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]")
                .build();
        String result = engine.query("SELECT name FROM (SELECT * FROM $r WHERE age > 25)");
        assertTrue(result.contains("Alice"));
        assertFalse(result.contains("Bob"));
    }

    @Test
    void engine_nonRootPath_reparsesRawJson() {
        // SQL4JsonEngine: resolveData with non-$r root path forces re-parse
        String json = "{\"items\":[{\"x\":1}]}";
        SQL4JsonEngine engine = SQL4Json.engine().data(json).build();
        String result = engine.query("SELECT * FROM $r.items");
        assertTrue(result.contains("x"));
    }

    @Test
    void engine_joinWithoutNamedSources_throws() {
        // SQL4JsonEngine: executeQuery with JOIN but no namedSources
        SQL4JsonEngine engine = SQL4Json.engine().data("[{\"id\":1}]").build();
        assertThrows(io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException.class,
                () -> engine.query("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id"));
    }
}
