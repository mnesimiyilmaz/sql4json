// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.types;

import java.math.BigDecimal;

/**
 * SQL arbitrary-precision number, backed by {@link BigDecimal}.
 *
 * @param value the {@link BigDecimal} value (never {@code null})
 * @since 1.2.0
 */
public record SqlDecimal(BigDecimal value) implements SqlNumber {

    @Override
    public long longValue() {
        return value.longValue();
    }

    @Override
    public double doubleValue() {
        return value.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        return value;
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
