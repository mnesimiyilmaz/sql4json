package io.github.mnesimiyilmaz.sql4json.types;

/**
 * An SQL numeric value. Wraps a {@link Number} that may be an {@link Integer},
 * {@link Long}, or {@link Double}. Small integers (0&ndash;255) are cached to
 * reduce allocation pressure during aggregation and comparison.
 *
 * @param value the numeric value (never {@code null})
 */
public record SqlNumber(Number value) implements SqlValue {

    private static final SqlNumber[] SMALL_CACHE = new SqlNumber[256];

    static {
        for (int i = 0; i < 256; i++) SMALL_CACHE[i] = new SqlNumber(i);
    }

    /**
     * Creates an {@code SqlNumber} from the given {@link Number}, using the
     * internal cache for small integers (0-255).
     *
     * @param value the numeric value
     * @return an {@code SqlNumber} wrapping the value
     */
    public static SqlNumber of(Number value) {
        if (value instanceof Integer i && i >= 0 && i < 256) return SMALL_CACHE[i];
        if (value instanceof Long l && l >= 0 && l < 256) return SMALL_CACHE[l.intValue()];
        return new SqlNumber(value);
    }

    /**
     * Creates an {@code SqlNumber} from an {@code int}, using the cache for values 0-255.
     *
     * @param value the int value
     * @return an {@code SqlNumber} wrapping the value
     */
    public static SqlNumber of(int value) {
        return (value >= 0 && value < 256) ? SMALL_CACHE[value] : new SqlNumber(value);
    }

    /**
     * Creates an {@code SqlNumber} from a {@code double}.
     *
     * @param value the double value
     * @return an {@code SqlNumber} wrapping the value
     */
    public static SqlNumber of(double value) {
        return new SqlNumber(value);
    }

    /**
     * Creates an {@code SqlNumber} from a {@code long}.
     *
     * @param value the long value
     * @return an {@code SqlNumber} wrapping the value
     */
    public static SqlNumber of(long value) {
        return new SqlNumber(value);
    }

    /**
     * Returns this number as a {@code double}.
     *
     * @return the double value
     */
    public double doubleValue() {
        return value.doubleValue();
    }

    /**
     * Returns this number as a {@code long} (truncating any fractional part).
     *
     * @return the long value
     */
    public long longValue() {
        return value.longValue();
    }

    @Override
    public Object rawValue() {
        return value;
    }
}
