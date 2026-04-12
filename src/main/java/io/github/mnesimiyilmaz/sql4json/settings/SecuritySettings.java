package io.github.mnesimiyilmaz.sql4json.settings;

/**
 * Security policy subsection of {@link Sql4jsonSettings}: controls soft ReDoS protection
 * for LIKE patterns and error-message redaction for multi-tenant safety.
 *
 * <p>Usage example:
 * <pre>{@code
 * Sql4jsonSettings settings = Sql4jsonSettings.builder()
 *     .security(s -> s.maxLikeWildcards(8).redactErrorDetails(true))
 *     .build();
 * }</pre>
 *
 * <p>Thread-safe: immutable record; a single instance may be shared across threads.
 *
 * @param maxLikeWildcards   maximum {@code %} wildcard count allowed in a single LIKE pattern
 *                           (soft ReDoS guard); default {@code 16}; {@code 0} disables wildcards entirely
 * @param redactErrorDetails when {@code true}, exception messages omit data-source names and
 *                           user-controlled format strings to prevent information disclosure;
 *                           default {@code true}
 * @see Sql4jsonSettings
 * @see LimitsSettings
 */
public record SecuritySettings(
        int maxLikeWildcards,
        boolean redactErrorDetails) {

    private static final SecuritySettings DEFAULTS = new SecuritySettings(16, true);

    /**
     * Returns the shared default security settings singleton.
     *
     * @return default security settings
     */
    public static SecuritySettings defaults() {
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
     * Mutable builder for {@link SecuritySettings}.
     */
    public static final class Builder {
        private int     maxLikeWildcards;
        private boolean redactErrorDetails;

        Builder(SecuritySettings src) {
            this.maxLikeWildcards = src.maxLikeWildcards;
            this.redactErrorDetails = src.redactErrorDetails;
        }

        /**
         * Sets the maximum number of {@code %} wildcards permitted in a single LIKE pattern.
         *
         * <p><b>Default:</b> {@code 16}.
         *
         * <p><b>Security rationale:</b> Soft ReDoS guard. Real LIKE patterns rarely exceed
         * 3–4 wildcards; 16 is a generous cap that blocks pathological inputs without
         * affecting legitimate queries.
         *
         * <p><b>Acceptable range:</b> {@code >= 0}. Setting {@code 0} disables wildcards entirely
         * (any {@code %} in a LIKE operand will be rejected). Negative values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum wildcard count, must be {@code >= 0}
         * @return this builder
         * @throws IllegalArgumentException if {@code v < 0}
         */
        public Builder maxLikeWildcards(int v) {
            if (v < 0) throw new IllegalArgumentException("maxLikeWildcards must be >= 0, got: " + v);
            this.maxLikeWildcards = v;
            return this;
        }

        /**
         * Controls whether exception messages redact data-source names and user-controlled
         * format strings.
         *
         * <p><b>Default:</b> {@code true}.
         *
         * <p><b>Security rationale:</b> Multi-tenant safety. When {@code true}, exception messages
         * omit data-source names, format strings, and user-controlled error content, preventing
         * them from leaking into client-visible responses.
         *
         * @param v {@code true} to redact sensitive detail from error messages; {@code false} to
         *          include full detail (useful in development or single-tenant environments)
         * @return this builder
         */
        public Builder redactErrorDetails(boolean v) {
            this.redactErrorDetails = v;
            return this;
        }

        /**
         * Builds an immutable {@link SecuritySettings} from the current builder state.
         *
         * @return a new security settings instance
         */
        public SecuritySettings build() {
            return new SecuritySettings(maxLikeWildcards, redactErrorDetails);
        }
    }
}
