package io.github.mnesimiyilmaz.sql4json.settings;

import java.util.Objects;

/**
 * Object-mapping configuration subsection of {@link Sql4jsonSettings}. Controls
 * behavior of {@code queryAs} / {@code queryAsList} / {@code JsonValue.as} and their
 * {@code execute*} / engine counterparts.
 *
 * <p>Usage example:
 * <pre>{@code
 * Sql4jsonSettings strict = Sql4jsonSettings.builder()
 *     .mapping(m -> m.missingFieldPolicy(MissingFieldPolicy.FAIL))
 *     .build();
 * }</pre>
 *
 * <p>Thread-safe: immutable record. Share a single instance across threads.
 *
 * @param missingFieldPolicy how to treat missing JSON fields during mapping
 * @see Sql4jsonSettings
 * @see MissingFieldPolicy
 * @since 1.1.0
 */
public record MappingSettings(MissingFieldPolicy missingFieldPolicy) {

    /**
     * Canonical constructor — rejects {@code null} components.
     *
     * @param missingFieldPolicy must be non-null
     */
    public MappingSettings {
        Objects.requireNonNull(missingFieldPolicy, "missingFieldPolicy");
    }

    private static final MappingSettings DEFAULTS = new MappingSettings(MissingFieldPolicy.IGNORE);

    /**
     * Returns the shared default mapping settings.
     *
     * @return the shared defaults singleton ({@link MissingFieldPolicy#IGNORE})
     */
    public static MappingSettings defaults() {
        return DEFAULTS;
    }

    /**
     * Creates a new builder pre-populated with {@link #defaults()}.
     *
     * @return a new builder pre-populated with {@link #defaults()}
     */
    public static Builder builder() {
        return new Builder(DEFAULTS);
    }

    /**
     * Creates a builder seeded from this instance.
     *
     * @return a new builder pre-populated with this instance's values
     */
    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Mutable builder for {@link MappingSettings}.
     */
    public static final class Builder {
        private MissingFieldPolicy missingFieldPolicy;

        Builder(MappingSettings src) {
            this.missingFieldPolicy = src.missingFieldPolicy;
        }

        /**
         * Sets the missing-field policy.
         *
         * @param p non-null missing-field policy
         * @return this builder
         * @throws NullPointerException if {@code p} is {@code null}
         */
        public Builder missingFieldPolicy(MissingFieldPolicy p) {
            this.missingFieldPolicy = Objects.requireNonNull(p, "missingFieldPolicy");
            return this;
        }

        /**
         * Builds an immutable {@link MappingSettings} from the current builder state.
         *
         * @return a new immutable {@link MappingSettings}
         */
        public MappingSettings build() {
            return new MappingSettings(missingFieldPolicy);
        }
    }
}
