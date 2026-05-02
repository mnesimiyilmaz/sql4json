// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

/**
 * Abstraction for query result caching. Allows plugging in custom cache implementations (Caffeine, Guava, etc.).
 *
 * <p>Implementations MUST be thread-safe if the Engine is used from multiple threads. {@code get()} returns
 * {@code null} for cache misses. Implementations MUST NOT store {@code null} as a value.
 */
public interface QueryResultCache {

    /**
     * Look up a cached result for the given SQL string.
     *
     * @param sql the exact SQL string used as the cache key
     * @return the cached {@link JsonValue} result, or {@code null} if not present
     */
    JsonValue get(String sql);

    /**
     * Store a result in the cache. Implementations MUST NOT accept {@code null} values.
     *
     * @param sql the exact SQL string used as the cache key
     * @param result the result to cache (never {@code null})
     */
    void put(String sql, JsonValue result);

    /** Remove all entries from the cache. */
    void clear();

    /**
     * Returns the current number of entries held by the cache.
     *
     * @return current entry count
     */
    int size();
}
