// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Bounded LRU cache for compiled LIKE {@link Pattern} instances.
 *
 * <p>Replaces the previous unbounded {@code ConcurrentHashMap} that lived inside {@link LikeConditionHandler}. One
 * instance is shared between both the LIKE and NOT LIKE handlers inside a single {@link ConditionHandlerRegistry}.
 *
 * <p>Uses an access-order {@code LinkedHashMap} wrapped in a synchronized map with a coarse lock held across
 * {@code computeIfAbsent}. This is a deliberate departure from {@code ConcurrentHashMap.computeIfAbsent} — access-order
 * LRU and {@code removeEldestEntry} both require holding a lock across the get-then-put, which CHM cannot provide
 * atomically.
 */
final class BoundedPatternCache {

    private final Map<String, Pattern> cache;

    BoundedPatternCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive, got: " + capacity);
        }
        this.cache = Collections.synchronizedMap(new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Pattern> e) {
                return size() > capacity;
            }
        });
    }

    Pattern computeIfAbsent(String key, Function<String, Pattern> compiler) {
        synchronized (cache) {
            return cache.computeIfAbsent(key, compiler);
        }
    }

    int size() {
        synchronized (cache) {
            return cache.size();
        }
    }
}
