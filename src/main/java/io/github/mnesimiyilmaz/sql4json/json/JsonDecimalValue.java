// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import java.math.BigDecimal;

/**
 * JSON arbitrary-precision number. Used for integer literals that overflow {@link Long} and any fraction the parser
 * cannot lossly hold in a double.
 *
 * @param value the {@link BigDecimal} value (never {@code null})
 * @since 1.2.0
 */
public record JsonDecimalValue(BigDecimal value) implements JsonNumberValue {

    @Override
    public Number numberValue() {
        return value;
    }
}
