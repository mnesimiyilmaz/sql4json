package io.github.mnesimiyilmaz.sql4json.settings;

import io.github.mnesimiyilmaz.sql4json.QueryResultCache;

/**
 * Cache subsection of {@link Sql4jsonSettings}: configures the LIKE-pattern compiled-regex
 * LRU, and the optional query-result cache used by {@code SQL4JsonEngine}.
 *
 * <p>{@link #customCache()} is the SPI slot: when non-null, the engine uses it verbatim
 * instead of constructing an internal LRU cache. When {@code null}, and
 * {@link #queryResultCacheEnabled()} is {@code true}, an LRU of size
 * {@link #queryResultCacheSize()} is created automatically.
 *
 * <p>Usage example:
 * <pre>{@code
 * Sql4jsonSettings settings = Sql4jsonSettings.builder()
 *     .cache(c -> c.queryResultCacheEnabled(true).queryResultCacheSize(128))
 *     .build();
 * }</pre>
 *
 * <p>Thread-safe: immutable record; a single instance may be shared across threads.
 *
 * @param likePatternCacheSize    LRU capacity for compiled LIKE-pattern regexes (default {@code 1_024})
 * @param queryResultCacheEnabled whether the query-result cache is active (default {@code false})
 * @param queryResultCacheSize    LRU eviction boundary for the internal query-result cache (default {@code 64})
 * @param customCache             user-supplied {@link QueryResultCache} SPI override; {@code null} means use internal LRU (default {@code null})
 * @see Sql4jsonSettings
 * @see QueryResultCache
 */
public record CacheSettings(
        int likePatternCacheSize,
        boolean queryResultCacheEnabled,
        int queryResultCacheSize,
        QueryResultCache customCache) {

    private static final CacheSettings DEFAULTS = new CacheSettings(
            1_024,   // likePatternCacheSize
            false,   // queryResultCacheEnabled — opt-in
            64,      // queryResultCacheSize (only when enabled)
            null);   // customCache — SPI override, null means "use internal LRU"

    /**
     * Returns the shared default cache settings singleton.
     *
     * @return default cache settings
     */
    public static CacheSettings defaults() {
        return DEFAULTS;
    }

    /**
     * Creates a new builder pre-populated with {@link #defaults()}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder(DEFAULTS);
    }

    /**
     * Creates a builder pre-populated with this instance's values.
     *
     * @return a new builder seeded from this settings
     */
    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Mutable builder for {@link CacheSettings}.
     */
    public static final class Builder {
        private int              likePatternCacheSize;
        private boolean          queryResultCacheEnabled;
        private int              queryResultCacheSize;
        private QueryResultCache customCache;

        Builder(CacheSettings src) {
            this.likePatternCacheSize = src.likePatternCacheSize;
            this.queryResultCacheEnabled = src.queryResultCacheEnabled;
            this.queryResultCacheSize = src.queryResultCacheSize;
            this.customCache = src.customCache;
        }

        /**
         * Sets the capacity of the bounded LRU cache for compiled LIKE-pattern regexes.
         *
         * <p><b>Default:</b> {@code 1_024}.
         *
         * <p><b>Security rationale:</b> Bounded LRU for compiled LIKE patterns reclaims memory
         * cleanly under hot-pattern access. Sized conservatively to avoid DoS via cache
         * poisoning (an attacker supplying distinct patterns cannot exhaust heap).
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v LRU capacity for LIKE pattern cache, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder likePatternCacheSize(int v) {
            if (v <= 0) throw new IllegalArgumentException("likePatternCacheSize must be positive, got: " + v);
            this.likePatternCacheSize = v;
            return this;
        }

        /**
         * Enables or disables the query-result cache for {@code SQL4JsonEngine}.
         *
         * <p><b>Default:</b> {@code false} (opt-in).
         *
         * <p><b>Security rationale:</b> Opt-in by default to preserve existing behavior and
         * avoid unbounded memory growth for callers that do not need result caching. When
         * enabled, results are cached using an LRU bounded by {@link #queryResultCacheSize(int)};
         * non-deterministic queries (e.g. those using {@code NOW()}) are never cached
         * regardless of this setting.
         *
         * @param v {@code true} to enable; {@code false} to disable
         * @return this builder
         */
        public Builder queryResultCacheEnabled(boolean v) {
            this.queryResultCacheEnabled = v;
            return this;
        }

        /**
         * Sets the LRU eviction boundary for the internal query-result cache.
         *
         * <p><b>Default:</b> {@code 64}.
         *
         * <p><b>Security rationale:</b> LRU eviction boundary when
         * {@link #queryResultCacheEnabled(boolean)} is {@code true}. Matches the historical
         * {@code SQL4JsonEngineBuilder.withQueryCache()} default. Ignored when
         * {@link #customCache(QueryResultCache)} is set or when the cache is disabled.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v query-result LRU capacity, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder queryResultCacheSize(int v) {
            if (v <= 0) throw new IllegalArgumentException("queryResultCacheSize must be positive, got: " + v);
            this.queryResultCacheSize = v;
            return this;
        }

        /**
         * Supplies a custom {@link QueryResultCache} implementation.
         *
         * <p><b>Default:</b> {@code null}.
         *
         * <p><b>Security rationale:</b> SPI slot for user-supplied cache implementations.
         * When non-null, takes precedence over the internal LRU construction path
         * ({@link #queryResultCacheEnabled(boolean)} and {@link #queryResultCacheSize(int)} are ignored
         * when this is set).
         *
         * @param cache custom cache implementation, or {@code null} to use the internal LRU
         * @return this builder
         * @see QueryResultCache
         */
        public Builder customCache(QueryResultCache cache) {
            this.customCache = cache;
            return this;
        }

        /**
         * Builds an immutable {@link CacheSettings} from the current builder state.
         *
         * @return a new cache settings instance
         */
        public CacheSettings build() {
            return new CacheSettings(likePatternCacheSize, queryResultCacheEnabled,
                    queryResultCacheSize, customCache);
        }
    }
}
