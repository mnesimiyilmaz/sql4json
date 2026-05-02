// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.settings;

import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Top-level immutable configuration for SQL4Json: composes security policy, numeric limits, cache configuration,
 * object-mapping behavior, and the JSON codec instance.
 *
 * <p>Obtain via {@link #defaults()} for safe defaults, or {@link #builder()} to customize specific subsections.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * // Default safe limits
 * Sql4jsonSettings settings = Sql4jsonSettings.defaults();
 *
 * // Customize one subsection
 * Sql4jsonSettings relaxed = Sql4jsonSettings.builder()
 *     .limits(l -> l.maxRowsPerQuery(5_000_000))
 *     .security(s -> s.redactErrorDetails(true))
 *     .build();
 *
 * String result = SQL4Json.query(sql, json, relaxed);
 * }</pre>
 *
 * <p>Thread-safe: all fields are final and immutable. A single instance may be shared freely across threads and reused
 * for multiple queries.
 *
 * @param security security policy subsection (LIKE wildcard limits, error redaction)
 * @param limits SQL parser and pipeline limits subsection (SQL length, row caps, etc.)
 * @param cache cache configuration subsection (LIKE-pattern cache, query-result cache)
 * @param mapping object-mapping subsection (missing-field policy, etc.)
 * @param codec JSON codec used for parsing and serialization
 * @see SecuritySettings
 * @see LimitsSettings
 * @see CacheSettings
 * @see MappingSettings
 * @see DefaultJsonCodecSettings
 * @see DefaultJsonCodec
 */
public record Sql4jsonSettings(
        SecuritySettings security,
        LimitsSettings limits,
        CacheSettings cache,
        MappingSettings mapping,
        JsonCodec codec) {

    /**
     * Canonical constructor — validates that no component is {@code null}.
     *
     * @param security security policy subsection
     * @param limits SQL parser and pipeline limits subsection
     * @param cache cache configuration subsection
     * @param mapping object-mapping subsection
     * @param codec JSON codec for parsing and serialization
     */
    public Sql4jsonSettings {
        Objects.requireNonNull(security, "security");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(cache, "cache");
        Objects.requireNonNull(mapping, "mapping");
        Objects.requireNonNull(codec, "codec");
    }

    private static final Sql4jsonSettings DEFAULTS = new Sql4jsonSettings(
            SecuritySettings.defaults(),
            LimitsSettings.defaults(),
            CacheSettings.defaults(),
            MappingSettings.defaults(),
            new DefaultJsonCodec());

    /**
     * Returns the shared default settings singleton.
     *
     * @return default settings instance
     */
    public static Sql4jsonSettings defaults() {
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

    /** Mutable builder for {@link Sql4jsonSettings}. */
    public static final class Builder {
        private final SecuritySettings.Builder security;
        private final LimitsSettings.Builder limits;
        private final CacheSettings.Builder cache;
        private final MappingSettings.Builder mapping;
        private JsonCodec codec;

        Builder(Sql4jsonSettings src) {
            this.security = src.security.toBuilder();
            this.limits = src.limits.toBuilder();
            this.cache = src.cache.toBuilder();
            this.mapping = src.mapping.toBuilder();
            this.codec = src.codec;
        }

        /**
         * Customizes the {@link SecuritySettings} subsection.
         *
         * @param fn consumer that receives the mutable builder; call setters on it to override security defaults
         * @return this builder
         * @see SecuritySettings
         */
        public Builder security(Consumer<SecuritySettings.Builder> fn) {
            fn.accept(security);
            return this;
        }

        /**
         * Customizes the {@link LimitsSettings} subsection.
         *
         * @param fn consumer that receives the mutable builder; call setters on it to override SQL and pipeline limit
         *     defaults
         * @return this builder
         * @see LimitsSettings
         */
        public Builder limits(Consumer<LimitsSettings.Builder> fn) {
            fn.accept(limits);
            return this;
        }

        /**
         * Customizes the {@link CacheSettings} subsection.
         *
         * @param fn consumer that receives the mutable builder; call setters on it to override cache defaults
         * @return this builder
         * @see CacheSettings
         */
        public Builder cache(Consumer<CacheSettings.Builder> fn) {
            fn.accept(cache);
            return this;
        }

        /**
         * Customizes the {@link MappingSettings} subsection.
         *
         * @param fn consumer that receives the mutable builder; call setters on it to override mapping defaults
         * @return this builder
         * @see MappingSettings
         * @since 1.1.0
         */
        public Builder mapping(Consumer<MappingSettings.Builder> fn) {
            fn.accept(mapping);
            return this;
        }

        /**
         * Replaces the JSON codec used by this settings instance.
         *
         * <p>The default codec is {@link DefaultJsonCodec} with its own safe defaults. Supply a custom codec to use a
         * different JSON library or to apply non-default {@link DefaultJsonCodecSettings}.
         *
         * @param codec non-null replacement codec
         * @return this builder
         * @see DefaultJsonCodec
         * @see DefaultJsonCodecSettings
         */
        public Builder codec(JsonCodec codec) {
            this.codec = Objects.requireNonNull(codec, "codec");
            return this;
        }

        /**
         * Builds an immutable {@link Sql4jsonSettings} from the current builder state.
         *
         * @return a new settings instance
         */
        public Sql4jsonSettings build() {
            return new Sql4jsonSettings(security.build(), limits.build(), cache.build(), mapping.build(), codec);
        }
    }
}
