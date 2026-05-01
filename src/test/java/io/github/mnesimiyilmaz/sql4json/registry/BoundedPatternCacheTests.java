package io.github.mnesimiyilmaz.sql4json.registry;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class BoundedPatternCacheTests {

    @Test
    void computes_pattern_on_miss() {
        var cache = new BoundedPatternCache(4);
        AtomicInteger compiles = new AtomicInteger();
        Pattern p = cache.computeIfAbsent("%a%", k -> {
            compiles.incrementAndGet();
            return Pattern.compile("^.*a.*$");
        });
        assertNotNull(p);
        assertEquals(1, compiles.get());
    }

    @Test
    void returns_cached_pattern_on_hit() {
        var cache = new BoundedPatternCache(4);
        AtomicInteger compiles = new AtomicInteger();
        cache.computeIfAbsent("%a%", k -> {
            compiles.incrementAndGet();
            return Pattern.compile("^.*a.*$");
        });
        cache.computeIfAbsent("%a%", k -> {
            compiles.incrementAndGet();
            return Pattern.compile("^.*a.*$");
        });
        assertEquals(1, compiles.get());
    }

    @Test
    void constructor_zeroCapacity_throws() {
        assertThrows(IllegalArgumentException.class, () -> new BoundedPatternCache(0));
    }

    @Test
    void constructor_negativeCapacity_throws() {
        assertThrows(IllegalArgumentException.class, () -> new BoundedPatternCache(-1));
    }

    @Test
    void size_reflects_inserted_entries() {
        var cache = new BoundedPatternCache(4);
        assertEquals(0, cache.size());
        cache.computeIfAbsent("a", k -> Pattern.compile("a"));
        cache.computeIfAbsent("b", k -> Pattern.compile("b"));
        assertEquals(2, cache.size());
        cache.computeIfAbsent("a", k -> {
            fail("should not recompile");
            return null;
        });
        assertEquals(2, cache.size());
    }

    @Test
    void evicts_least_recently_used_when_capacity_exceeded() {
        var cache = new BoundedPatternCache(2);
        cache.computeIfAbsent("a", k -> Pattern.compile("a"));
        cache.computeIfAbsent("b", k -> Pattern.compile("b"));
        // touch 'a' to make it MRU
        cache.computeIfAbsent("a", k -> {
            fail("should not recompile");
            return null;
        });
        // insert 'c' — evicts 'b'
        cache.computeIfAbsent("c", k -> Pattern.compile("c"));
        AtomicInteger compiles = new AtomicInteger();
        cache.computeIfAbsent("b", k -> {
            compiles.incrementAndGet();
            return Pattern.compile("b");
        });
        assertEquals(1, compiles.get(), "'b' should have been evicted");
    }
}
