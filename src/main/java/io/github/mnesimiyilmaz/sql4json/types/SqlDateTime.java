// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.types;

import java.time.LocalDateTime;

/**
 * An SQL date-time value (date with time of day).
 *
 * <p>Produced by {@code NOW()}, {@code TO_DATE()} with a datetime format, or {@code CAST(x AS DATETIME)}.
 *
 * @param value the date-time (never {@code null})
 */
public record SqlDateTime(LocalDateTime value) implements SqlValue {
    @Override
    public Object rawValue() {
        return value;
    }
}
