// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.types;

/**
 * An SQL string value.
 *
 * @param value the string content (never {@code null})
 */
public record SqlString(String value) implements SqlValue {
    @Override
    public Object rawValue() {
        return value;
    }
}
