package io.github.mnesimiyilmaz.sql4json.settings;

/**
 * SQL parser and pipeline limits subsection of {@link Sql4jsonSettings}: caps SQL input
 * size, subquery nesting, IN-list breadth, and per-query row counts to guard against
 * denial-of-service and runaway memory usage.
 *
 * <p>Usage example:
 * <pre>{@code
 * Sql4jsonSettings settings = Sql4jsonSettings.builder()
 *     .limits(l -> l.maxSqlLength(128 * 1024).maxRowsPerQuery(500_000))
 *     .build();
 * }</pre>
 *
 * <p>Thread-safe: immutable record; a single instance may be shared across threads.
 *
 * <h2>Row-count enforcement</h2>
 * <p>
 * The {@code maxRowsPerQuery} limit is enforced at every materializing pipeline stage.
 * When a stage would exceed the limit it throws {@code SQL4JsonExecutionException} with
 * a message of the form:
 * <pre>"&lt;STAGE&gt; row count exceeds configured maximum (&lt;N&gt;)"</pre>
 * where {@code <STAGE>} is one of the canonical stage names:
 * {@code GROUP BY}, {@code ORDER BY}, {@code WINDOW}, {@code JOIN},
 * {@code DISTINCT}, {@code PIPELINE}, {@code STREAMING}.
 *
 * <p>
 * The boundary is strict: a query producing exactly {@code maxRowsPerQuery} rows passes;
 * one producing {@code maxRowsPerQuery + 1} throws.
 *
 * <h3>What "row count" means per stage</h3>
 * <ul>
 *   <li>{@code GROUP BY} / {@code ORDER BY} / {@code WINDOW}: counts input rows
 *       (before grouping / sorting / windowing). These stages must buffer the entire
 *       input before producing output, so the limit guards memory usage at that point.</li>
 *   <li>{@code DISTINCT}: counts output rows (distinct keys). The input stream may be
 *       arbitrarily larger than the number of distinct values accumulated.</li>
 *   <li>{@code JOIN}: counts output (merged) rows produced by the hash probe, not the
 *       sizes of the left or right input sources.</li>
 *   <li>{@code PIPELINE} / {@code STREAMING}: counts final result rows delivered at the
 *       end of the pipeline or streaming sink — i.e. the row count after all prior stages
 *       have filtered, grouped, and ordered the data.</li>
 * </ul>
 *
 * <p>
 * When tuning this limit in production, size it against the largest intermediate row
 * count that is possible in the pipeline, not just the final output size.
 *
 * @param maxSqlLength     maximum SQL query string length in chars; default {@code 65_536} (64 KiB)
 * @param maxSubqueryDepth maximum nesting depth of FROM subqueries; default {@code 16}
 * @param maxInListSize    maximum number of values in an IN / NOT IN list; default {@code 1_024}
 * @param maxRowsPerQuery  maximum rows allowed at any materializing pipeline stage;
 *                         default {@code 1_000_000}
 * @param maxParameters    maximum number of parameter placeholders ({@code ?} and {@code :name}
 *                         combined) permitted in a single query; default {@code 1_024}
 * @see Sql4jsonSettings
 * @see SecuritySettings
 * @see CacheSettings
 */
public record LimitsSettings(
        int maxSqlLength,
        int maxSubqueryDepth,
        int maxInListSize,
        int maxRowsPerQuery,
        int maxParameters) {

    private static final LimitsSettings DEFAULTS = new LimitsSettings(
            65_536,
            16,
            1_024,
            1_000_000,
            1_024);

    /**
     * Returns the shared default limits settings singleton.
     *
     * @return default limits settings
     */
    public static LimitsSettings defaults() {
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
     * Mutable builder for {@link LimitsSettings}.
     */
    public static final class Builder {
        private int maxSqlLength;
        private int maxSubqueryDepth;
        private int maxInListSize;
        private int maxRowsPerQuery;
        private int maxParameters;

        Builder(LimitsSettings src) {
            this.maxSqlLength = src.maxSqlLength;
            this.maxSubqueryDepth = src.maxSubqueryDepth;
            this.maxInListSize = src.maxInListSize;
            this.maxRowsPerQuery = src.maxRowsPerQuery;
            this.maxParameters = src.maxParameters;
        }

        /**
         * Sets the maximum allowed SQL query string length in characters.
         *
         * <p><b>Default:</b> {@code 65_536} (64 KiB).
         *
         * <p><b>Security rationale:</b> DoS guard against unbounded SQL input. Hand-written SQL
         * is rarely more than a few KiB; 64 KiB absorbs generated queries while preventing
         * resource exhaustion from arbitrarily large inputs.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum character count, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder maxSqlLength(int v) {
            this.maxSqlLength = positive(v, "maxSqlLength");
            return this;
        }

        /**
         * Sets the maximum nesting depth of FROM subqueries.
         *
         * <p><b>Default:</b> {@code 16}.
         *
         * <p><b>Security rationale:</b> Recursion bomb guard. 16 levels of subquery nesting
         * is generous for any realistic use case; deeper nesting almost certainly indicates
         * a malformed or adversarial input.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum subquery nesting depth, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder maxSubqueryDepth(int v) {
            this.maxSubqueryDepth = positive(v, "maxSubqueryDepth");
            return this;
        }

        /**
         * Sets the maximum number of literal values permitted in an {@code IN} / {@code NOT IN} list.
         *
         * <p><b>Default:</b> {@code 1_024}.
         *
         * <p><b>Security rationale:</b> Parse-time amplification guard. 1,024 literal values
         * in an IN list is already extreme for any real query; larger lists can cause
         * disproportionate parse-time or evaluation-time work.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum IN-list element count, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder maxInListSize(int v) {
            this.maxInListSize = positive(v, "maxInListSize");
            return this;
        }

        /**
         * Sets the maximum number of rows allowed at any single materializing pipeline stage.
         *
         * <p><b>Default:</b> {@code 1_000_000}.
         *
         * <p><b>Security rationale:</b> Memory guard at materializing pipeline stages
         * ({@code GROUP BY}, {@code ORDER BY}, {@code WINDOW}, {@code JOIN}, {@code DISTINCT},
         * pipeline sink, and streaming sink). One million rows equates to roughly 100 MB of
         * heap for a typical row shape. See the class-level Javadoc for the precise per-stage
         * semantics of "row count".
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum row count per materializing stage, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         */
        public Builder maxRowsPerQuery(int v) {
            this.maxRowsPerQuery = positive(v, "maxRowsPerQuery");
            return this;
        }

        /**
         * Sets the maximum number of parameter placeholders permitted in a single query
         * (positional and named combined, globally across any nested subqueries).
         *
         * <p><b>Default:</b> {@code 1_024}.
         *
         * <p><b>Security rationale:</b> DoS guard against adversarial placeholder flooding.
         * Hand-written queries virtually never exceed ~20 placeholders; 1,024 absorbs
         * generated queries (e.g. IN-list expansions pre-expansion) while preventing
         * resource exhaustion from pathological input.
         *
         * <p><b>Acceptable range:</b> Must be positive ({@code > 0}). Non-positive values throw
         * {@link IllegalArgumentException}.
         *
         * @param v maximum placeholder count, must be positive
         * @return this builder
         * @throws IllegalArgumentException if {@code v <= 0}
         * @since 1.1.0
         */
        public Builder maxParameters(int v) {
            this.maxParameters = positive(v, "maxParameters");
            return this;
        }

        /**
         * Builds an immutable {@link LimitsSettings} from the current builder state.
         *
         * @return a new limits settings instance
         */
        public LimitsSettings build() {
            return new LimitsSettings(maxSqlLength, maxSubqueryDepth, maxInListSize, maxRowsPerQuery,
                    maxParameters);
        }

        private static int positive(int v, String name) {
            if (v <= 0) throw new IllegalArgumentException(name + " must be positive, got: " + v);
            return v;
        }
    }
}
