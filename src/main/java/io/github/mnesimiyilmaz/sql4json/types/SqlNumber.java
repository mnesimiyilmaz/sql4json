package io.github.mnesimiyilmaz.sql4json.types;

import java.math.BigDecimal;

/**
 * Sealed numeric SQL value &mdash; root of the numeric family.
 *
 * <p>Three variants permit unboxed primitive storage where possible:
 * <ul>
 *   <li>{@link SqlLong} &mdash; integer in {@code long} range</li>
 *   <li>{@link SqlDouble} &mdash; fraction / exponent</li>
 *   <li>{@link SqlDecimal} &mdash; arbitrary precision</li>
 * </ul>
 *
 * @since 1.2.0
 */
public sealed interface SqlNumber extends SqlValue
        permits SqlLong, SqlDouble, SqlDecimal {

    /**
     * Returns this value as a primitive {@code long} (truncating fractions).
     *
     * @return the long value
     */
    long longValue();

    /**
     * Returns this value as a primitive {@code double}.
     *
     * @return the double value
     */
    double doubleValue();

    /**
     * Returns this value as a {@link BigDecimal}.
     *
     * @return the big-decimal value
     */
    BigDecimal bigDecimalValue();

    /**
     * Returns this value as a boxed {@link Number}. {@link SqlLong} returns a
     * {@link Long}, {@link SqlDouble} returns a {@link Double}, {@link SqlDecimal}
     * returns the underlying {@link BigDecimal}.
     *
     * @return a boxed {@link Number} view of this value
     */
    Number numberValue();

    /**
     * Factory: returns a cached {@link SqlLong} for 0..255, otherwise a fresh one.
     *
     * @param value the long value
     * @return an {@link SqlLong} wrapping {@code value}
     */
    static SqlNumber of(long value) {
        return SqlLong.of(value);
    }

    /**
     * Factory: returns a fresh {@link SqlDouble}.
     *
     * @param value the double value
     * @return an {@link SqlDouble} wrapping {@code value}
     */
    static SqlNumber of(double value) {
        return new SqlDouble(value);
    }

    /**
     * Factory: returns a fresh {@link SqlDecimal}.
     *
     * @param value the {@link BigDecimal} value (never {@code null})
     * @return an {@link SqlDecimal} wrapping {@code value}
     */
    static SqlNumber of(BigDecimal value) {
        return new SqlDecimal(value);
    }

    /**
     * Factory: dispatches a {@link Number} to the appropriate variant.
     *
     * @param value the number (never {@code null})
     * @return a typed {@link SqlNumber} variant
     */
    static SqlNumber of(Number value) {
        return switch (value) {
            case Long l        -> of(l.longValue());
            case Integer i     -> of(i.longValue());
            case Short s       -> of(s.longValue());
            case Byte b        -> of(b.longValue());
            case Double d      -> of(d.doubleValue());
            case Float f       -> of(f.doubleValue());
            case BigDecimal bd -> of(bd);
            default            -> of(value.doubleValue());
        };
    }
}
