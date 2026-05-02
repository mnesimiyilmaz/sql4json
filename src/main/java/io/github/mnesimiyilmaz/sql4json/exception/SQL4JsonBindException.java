// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.exception;

/**
 * Thrown when parameter binding fails at substitute time. Categories include:
 *
 * <ul>
 *   <li>Missing positional or named parameter
 *   <li>Extra positional parameter (count mismatch)
 *   <li>Mixing named and positional on the same {@code BoundParameters} instance
 *   <li>Unsupported Java type for a parameter value
 *   <li>IN-list expansion exceeding {@link io.github.mnesimiyilmaz.sql4json.settings.LimitsSettings#maxInListSize()}
 *   <li>Collection bound to a scalar placeholder position
 *   <li>{@code LIMIT}/{@code OFFSET} bound to {@code null}, negative, non-integer, or out-of-range value
 * </ul>
 *
 * <p>Parse-time errors (syntactic) remain {@link SQL4JsonParseException}; bind errors are surfaced only when
 * {@code execute(..., BoundParameters)} is called.
 *
 * @see SQL4JsonException
 * @since 1.1.0
 */
public final class SQL4JsonBindException extends SQL4JsonException {

    /**
     * Creates a bind exception with the given message.
     *
     * @param message detail message
     */
    public SQL4JsonBindException(String message) {
        super(message);
    }

    /**
     * Creates a bind exception with the given message and cause.
     *
     * @param message detail message
     * @param cause underlying cause
     */
    public SQL4JsonBindException(String message, Throwable cause) {
        super(message, cause);
    }
}
