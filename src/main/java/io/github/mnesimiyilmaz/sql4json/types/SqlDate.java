// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.types;

import java.time.LocalDate;

/**
 * An SQL date value (date without time).
 *
 * <p>Produced by the {@code TO_DATE()} function or {@code CAST(x AS DATE)}.
 *
 * @param value the date (never {@code null})
 */
public record SqlDate(LocalDate value) implements SqlValue {
    @Override
    public Object rawValue() {
        return value;
    }
}
