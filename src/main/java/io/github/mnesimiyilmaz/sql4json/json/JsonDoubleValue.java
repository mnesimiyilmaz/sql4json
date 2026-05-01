package io.github.mnesimiyilmaz.sql4json.json;

/**
 * JSON floating-point value backed by an unboxed {@code double}. Used for any
 * JSON number with a fractional or exponent part.
 *
 * @param value the double value
 * @since 1.2.0
 */
public record JsonDoubleValue(double value) implements JsonNumberValue {

    @Override
    public Number numberValue() {
        return value;
    }
}
