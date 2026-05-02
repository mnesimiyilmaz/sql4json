// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.types;

import java.math.BigDecimal;

/**
 * SQL integer value backed by an unboxed {@code long}.
 *
 * @param value the long value
 * @since 1.2.0
 */
public record SqlLong(long value) implements SqlNumber {

    // Package-private 256-entry small-int cache. Owned by SqlLong rather than the
    // SqlNumber sealed interface (where it would be implicitly public-static-final
    // and would leak as part of the exported API surface, per Sonar S2386).
    private static final SqlLong[] SMALL_LONGS = buildSmallCache();

    private static SqlLong[] buildSmallCache() {
        SqlLong[] out = new SqlLong[256];
        for (int i = 0; i < 256; i++) out[i] = new SqlLong(i);
        return out;
    }

    /**
     * Factory: returns a cached instance for {@code 0..255}, otherwise a fresh one. Hot-path JSON / aggregation code
     * paths route through this to avoid per-row {@link SqlLong} allocations for small integers.
     *
     * @param value the long value
     * @return an {@link SqlLong} wrapping {@code value}
     * @since 1.2.0
     */
    public static SqlLong of(long value) {
        return (value >= 0 && value < 256) ? SMALL_LONGS[(int) value] : new SqlLong(value);
    }

    @Override
    public long longValue() {
        return value;
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
