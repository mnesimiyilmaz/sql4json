package io.github.mnesimiyilmaz.sql4json.types;

import java.math.BigDecimal;

/**
 * SQL floating-point value backed by an unboxed {@code double}.
 *
 * @param value the double value
 * @since 1.2.0
 */
public record SqlDouble(double value) implements SqlNumber {

    @Override
    public long longValue() {
        return (long) value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public BigDecimal bigDecimalValue() {
        return BigDecimal.valueOf(value);
    }

    @Override
    public Number numberValue() {
        return value;
    }

    @Override
    public Object rawValue() {
        return value;
    }
}
