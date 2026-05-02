// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.types;

/**
 * An SQL boolean value. Uses singleton constants {@link #TRUE} and {@link #FALSE} to avoid unnecessary object creation.
 *
 * @param value the boolean value
 */
public record SqlBoolean(boolean value) implements SqlValue {
    /** The singleton {@code true} value. */
    public static final SqlBoolean TRUE = new SqlBoolean(true);
    /** The singleton {@code false} value. */
    public static final SqlBoolean FALSE = new SqlBoolean(false);

    /**
     * Returns the singleton instance for the given boolean value.
     *
     * @param value the boolean
     * @return {@link #TRUE} or {@link #FALSE}
     */
    public static SqlBoolean of(boolean value) {
        return value ? TRUE : FALSE;
    }

    @Override
    public Object rawValue() {
        return value;
    }
}
