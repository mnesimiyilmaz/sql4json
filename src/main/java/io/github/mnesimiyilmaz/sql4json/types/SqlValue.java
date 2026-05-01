package io.github.mnesimiyilmaz.sql4json.types;

/**
 * A sealed interface representing a typed SQL value used internally during query processing.
 *
 * <p>JSON values are converted to {@code SqlValue} instances for type-safe comparison,
 * aggregation, and function evaluation. The type hierarchy maps to SQL-like types:
 *
 * <ul>
 *   <li>{@link SqlString} &mdash; text values</li>
 *   <li>{@link SqlNumber} &mdash; integer and floating-point numbers</li>
 *   <li>{@link SqlBoolean} &mdash; {@code true} / {@code false}</li>
 *   <li>{@link SqlDate} &mdash; date values ({@link java.time.LocalDate})</li>
 *   <li>{@link SqlDateTime} &mdash; date-time values ({@link java.time.LocalDateTime})</li>
 *   <li>{@link SqlNull} &mdash; the absence of a value</li>
 * </ul>
 *
 * @see JsonValue
 */
public sealed interface SqlValue permits SqlNumber, SqlString, SqlBoolean, SqlDate, SqlDateTime, SqlNull {

    /**
     * Returns {@code true} if this value represents SQL {@code NULL}.
     *
     * @return {@code true} for {@link SqlNull}, {@code false} for all other types
     */
    default boolean isNull() {
        return this instanceof SqlNull;
    }

    /**
     * Returns the underlying Java object for this SQL value.
     *
     * <p>Return types by implementation:
     * <ul>
     *   <li>{@link SqlString} &rarr; {@link String}</li>
     *   <li>{@link SqlNumber} (variant {@link SqlLong} / {@link SqlDouble} / {@link SqlDecimal}) &rarr; the typed primitive or {@link java.math.BigDecimal}</li>
     *   <li>{@link SqlBoolean} &rarr; {@link Boolean}</li>
     *   <li>{@link SqlDate} &rarr; {@link java.time.LocalDate}</li>
     *   <li>{@link SqlDateTime} &rarr; {@link java.time.LocalDateTime}</li>
     *   <li>{@link SqlNull} &rarr; {@code null}</li>
     * </ul>
     *
     * @return the raw value, or {@code null} for {@link SqlNull}
     */
    Object rawValue();
}
