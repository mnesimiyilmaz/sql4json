package io.github.mnesimiyilmaz.sql4json.settings;

import io.github.mnesimiyilmaz.sql4json.QueryResultCache;
import io.github.mnesimiyilmaz.sql4json.SQL4Json;
import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Sql4jsonSettingsCacheTests {

    @Test
    void customCache_is_honored_by_engine_builder() {
        AtomicInteger puts = new AtomicInteger();
        AtomicInteger gets = new AtomicInteger();
        QueryResultCache spy = new QueryResultCache() {
            private final ConcurrentMap<String, JsonValue> m = new ConcurrentHashMap<>();

            @Override
            public JsonValue get(String sql) {
                gets.incrementAndGet();
                return m.get(sql);
            }

            @Override
            public void put(String sql, JsonValue result) {
                puts.incrementAndGet();
                m.put(sql, result);
            }

            @Override
            public void clear() {
                m.clear();
            }

            @Override
            public int size() {
                return m.size();
            }
        };
        var settings = Sql4jsonSettings.builder()
                .cache(c -> c.customCache(spy))
                .build();
        var engine = SQL4Json.engine().settings(settings).data("[{\"a\":1}]").build();
        engine.query("SELECT a FROM $r");
        engine.query("SELECT a FROM $r");
        assertTrue(gets.get() >= 2, "custom cache should have been consulted on each query");
        assertEquals(1, puts.get(), "custom cache should store exactly one entry for the same SQL");
    }

    @Test
    void likePatternCacheSize_evicts_old_patterns_through_full_stack() {
        // Capacity of 1 guarantees eviction when a second distinct LIKE pattern runs.
        var settings = Sql4jsonSettings.builder()
                .cache(c -> c.likePatternCacheSize(1))
                .build();
        String json = "[{\"name\":\"alice\"},{\"name\":\"bob\"},{\"name\":\"charlie\"}]";
        // Two distinct LIKE patterns against the same data → second compile evicts first.
        String r1 = SQL4Json.query("SELECT name FROM $r WHERE name LIKE '%a%'", json, settings);
        String r2 = SQL4Json.query("SELECT name FROM $r WHERE name LIKE '%b%'", json, settings);
        // Both queries should succeed, just with a smaller shared cache.
        assertTrue(r1.contains("alice") && r1.contains("charlie"));
        assertTrue(r2.contains("bob"));
    }

    @Test
    void queryResultCacheEnabled_false_bypasses_cache() {
        var settings = Sql4jsonSettings.builder()
                .cache(c -> c.queryResultCacheEnabled(false))
                .build();
        var engine = SQL4Json.engine().settings(settings).data("[{\"a\":1}]").build();
        // Without a cache, size() is 0 and querying twice still works.
        assertEquals(0, engine.cacheSize());
        engine.query("SELECT a FROM $r");
        engine.query("SELECT a FROM $r");
        assertEquals(0, engine.cacheSize());
    }

    @Test
    void queryResultCacheEnabled_true_uses_lru() {
        var settings = Sql4jsonSettings.builder()
                .cache(c -> c.queryResultCacheEnabled(true).queryResultCacheSize(2))
                .build();
        var engine = SQL4Json.engine().settings(settings).data("[{\"a\":1},{\"a\":2}]").build();
        engine.query("SELECT a FROM $r WHERE a = 1");
        engine.query("SELECT a FROM $r WHERE a = 2");
        assertEquals(2, engine.cacheSize());
        engine.query("SELECT a FROM $r"); // third distinct SQL evicts the oldest
        assertEquals(2, engine.cacheSize());
    }

    @Test
    void data_before_settings_honors_late_settings() {
        // Regression: .data(json) must defer parsing until build(), so a later
        // .settings(custom) call still sees its custom codec. Eager parsing in
        // .data() would silently ignore the custom codec.
        AtomicInteger parseCalls = new AtomicInteger();
        var codec = new JsonCodec() {
            private final DefaultJsonCodec delegate = new DefaultJsonCodec();

            @Override
            public JsonValue parse(String s) {
                parseCalls.incrementAndGet();
                return delegate.parse(s);
            }

            @Override
            public String serialize(JsonValue v) {
                return delegate.serialize(v);
            }
        };
        var settings = Sql4jsonSettings.builder().codec(codec).build();
        // Deliberate ordering: data first, then settings. The spy codec MUST
        // be the one that parses the input.
        var engine = SQL4Json.engine().data("[{\"a\":1}]").settings(settings).build();
        assertEquals(1, parseCalls.get(),
                "custom codec must parse the input, proving deferred-parse fix works");
        // Sanity: engine actually works.
        String result = engine.query("SELECT a FROM $r");
        assertTrue(result.contains("1"));
    }
}
