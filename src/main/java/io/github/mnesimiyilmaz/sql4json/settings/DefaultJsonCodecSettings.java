// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.settings;

import java.util.Objects;

/**
 * Security and resource limits for the built-in {@link io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec}.
 * Enforced by {@code JsonParser} and {@code StreamingJsonParser} during JSON parsing.
 *
 * <p>All limits are inclusive: a value <i>equal</i> to the configured limit is accepted; exceeding it throws
 * {@code SQL4JsonExecutionException}.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * DefaultJsonCodecSettings codecSettings = DefaultJsonCodecSettings.builder()
 *     .maxInputLength(50 * 1024 * 1024)
 *     .duplicateKeyPolicy(DuplicateKeyPolicy.LAST_WINS)
 *     .build();
 *
 * Sql4jsonSettings settings = Sql4jsonSettings.builder()
 *     .codec(new DefaultJsonCodec(codecSettings))
 *     .build();
 * }</pre>
 *
 * <p>Thread-safe: immutable record; a single instance may be shared across threads.
 *
 * @param maxInputLength maximum JSON input length in chars (default 10 MiB)
 * @param maxNestingDepth maximum object/array nesting depth (default {@code 64})
 * @param maxStringLength maximum length of a single JSON string value in chars (default 1 MiB)
 * @param maxNumberLength maximum length of a numeric literal in chars (default {@code 64})
 * @param maxPropertyNameLength maximum length of a JSON object key in chars (default {@code 1_024})
 * @param maxArrayElements maximum number of elements in a single JSON array (default {@code 1_000_000})
 * @param duplicateKeyPolicy policy for duplicate keys in a JSON object (default {@link DuplicateKeyPolicy#REJECT})
 * @see io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec
 * @see DuplicateKeyPolicy
 * @see Sql4jsonSettings
 */
public record DefaultJsonCodecSettings(
        int maxInputLength,
        int maxNestingDepth,
        int maxStringLength,
        int maxNumberLength,
        int maxPropertyNameLength,
        int maxArrayElements,
        DuplicateKeyPolicy duplicateKeyPolicy) {

    /**
     * Canonical constructor — validates that all limits are positive and the policy is non-null.
     *
     * @param maxInputLength maximum JSON input length in chars
     * @param maxNestingDepth maximum object/array nesting depth
     * @param maxStringLength maximum length of a single JSON string value in chars
     * @param maxNumberLength maximum length of a numeric literal in chars
     * @param maxPropertyNameLength maximum length of a JSON object key in chars
     * @param maxArrayElements maximum number of elements in a single JSON array
     * @param duplicateKeyPolicy policy for duplicate keys in a JSON object
     */
    public DefaultJsonCodecSettings {
        positive(maxInputLength, "maxInputLength");
        positive(maxNestingDepth, "maxNestingDepth");
        positive(maxStringLength, "maxStringLength");
        positive(maxNumberLength, "maxNumberLength");
        positive(maxPropertyNameLength, "maxPropertyNameLength");
        positive(maxArrayElements, "maxArrayElements");
        Objects.requireNonNull(duplicateKeyPolicy, "duplicateKeyPolicy");
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    private static final DefaultJsonCodecSettings DEFAULTS = new DefaultJsonCodecSettings(
            10 * 1024 * 1024, // maxInputLength          — 10 MiB total JSON input
            64, // maxNestingDepth         — object/array nesting
            1 * 1024 * 1024, // maxStringLength         — 1 MiB per string value
            64, // maxNumberLength         — chars in a numeric literal
            1_024, // maxPropertyNameLength   — chars in an object key
            1_000_000, // maxArrayElements        — elements per array (breadth)
            DuplicateKeyPolicy.REJECT); // duplicateKeyPolicy      — fail-safe

    /**
     * Returns the shared default codec settings singleton.
     *
     * @return default codec settings
     */
    public static DefaultJsonCodecSettings defaults() {
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

    private static int positive(int v, String name) {
        if (v <= 0) throw new IllegalArgumentException(name + " must be positive, got: " + v);
        return v;
    }

    /** Mutable builder for {@link DefaultJsonCodecSettings}. */
    public static final class Builder {
        private int maxInputLength;
        private int maxNestingDepth;
        private int maxStringLength;
        private int maxNumberLength;
        private int maxPropertyNameLength;
        private int maxArrayElements;
        private DuplicateKeyPolicy duplicateKeyPolicy;

        Builder(DefaultJsonCodecSettings src) {
            this.maxInputLength = src.maxInputLength;
            this.maxNestingDepth = src.maxNestingDepth;
            this.maxStringLength = src.maxStringLength;
            this.maxNumberLength = src.maxNumberLength;
            this.maxPropertyNameLength = src.maxPropertyNameLength;
            this.maxArrayElements = src.maxArrayElements;
            this.duplicateKeyPolicy = src.duplicateKeyPolicy;
        }

        /**
         * Sets the total JSON input length cap checked against the raw JSON string before parsing.
         *
         * <p><b>Default:</b> {@code 10 * 1024 * 1024} (10 MiB).
         *
         * <p><b>Security rationale:</b> Total-input cap prevents the parser from allocating resources proportional to
         * arbitrarily large input strings.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum raw JSON string length in characters, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder maxInputLength(int v) {
            this.maxInputLength = positive(v, "maxInputLength");
            return this;
        }

        /**
         * Sets the maximum object/array nesting depth.
         *
         * <p><b>Default:</b> {@code 64}.
         *
         * <p><b>Security rationale:</b> Tightened from the legacy value of 256. Realistic JSON rarely exceeds a nesting
         * depth of 10; deeply nested structures can cause stack-overflow or quadratic recursion in tree-walking code.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum nesting depth, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder maxNestingDepth(int v) {
            this.maxNestingDepth = positive(v, "maxNestingDepth");
            return this;
        }

        /**
         * Sets the maximum length of a single JSON string value.
         *
         * <p><b>Default:</b> {@code 1 * 1024 * 1024} (1 MiB).
         *
         * <p><b>Security rationale:</b> Per-string-value cap. Tightened from the legacy 5 MiB limit; a single JSON
         * string value exceeding 1 MiB is unusual in practice.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum string value length in characters, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder maxStringLength(int v) {
            this.maxStringLength = positive(v, "maxStringLength");
            return this;
        }

        /**
         * Sets the maximum number of characters in a numeric literal.
         *
         * <p><b>Default:</b> {@code 64}.
         *
         * <p><b>Security rationale:</b> Tightened from the legacy 1,000-character limit. No real number (including
         * scientific notation) exceeds approximately 30 characters; very long numeric literals are a sign of
         * adversarial input.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum numeric literal length in characters, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder maxNumberLength(int v) {
            this.maxNumberLength = positive(v, "maxNumberLength");
            return this;
        }

        /**
         * Sets the maximum number of characters in a JSON object property name (key).
         *
         * <p><b>Default:</b> {@code 1_024}.
         *
         * <p><b>Security rationale:</b> Tightened from the legacy 50,000-character limit. Object keys longer than 1,024
         * characters are virtually never legitimate and can inflate internal map structures disproportionately.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum property name length in characters, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder maxPropertyNameLength(int v) {
            this.maxPropertyNameLength = positive(v, "maxPropertyNameLength");
            return this;
        }

        /**
         * Sets the maximum number of elements permitted in a single JSON array.
         *
         * <p><b>Default:</b> {@code 1_000_000}.
         *
         * <p><b>Security rationale:</b> Per-array breadth cap. One million elements per array is already extreme;
         * larger arrays can cause disproportionate heap allocation when converted to internal list structures.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum elements per JSON array, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder maxArrayElements(int v) {
            this.maxArrayElements = positive(v, "maxArrayElements");
            return this;
        }

        /**
         * Sets the policy for handling duplicate keys in a JSON object.
         *
         * <p><b>Default:</b> {@link DuplicateKeyPolicy#REJECT}.
         *
         * <p><b>Security rationale:</b> Fail-safe on {@code {"a":1,"a":2}}. RFC 8259 permits multiple strategies;
         * disagreement between systems on which duplicate wins enables HTTP Parameter Pollution-style desync attacks.
         * {@code REJECT} is the safest choice for any environment where input is not fully trusted.
         *
         * @param p duplicate key policy, must not be {@code null}
         * @return this builder
         * @throws NullPointerException if {@code p} is {@code null}
         * @see DuplicateKeyPolicy
         */
        public Builder duplicateKeyPolicy(DuplicateKeyPolicy p) {
            this.duplicateKeyPolicy = Objects.requireNonNull(p, "duplicateKeyPolicy");
            return this;
        }

        /**
         * Builds an immutable {@link DefaultJsonCodecSettings} from the current builder state.
         *
         * @return a new codec settings instance
         */
        public DefaultJsonCodecSettings build() {
            return new DefaultJsonCodecSettings(
                    maxInputLength,
                    maxNestingDepth,
                    maxStringLength,
                    maxNumberLength,
                    maxPropertyNameLength,
                    maxArrayElements,
                    duplicateKeyPolicy);
        }
    }
}
