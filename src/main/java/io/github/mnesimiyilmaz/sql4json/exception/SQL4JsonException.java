// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.exception;

/**
 * Base exception for all SQL4Json errors. Subtypes distinguish parse-time vs execution-time failures.
 *
 * @see SQL4JsonParseException
 * @see SQL4JsonExecutionException
 */
public sealed class SQL4JsonException extends RuntimeException
        permits SQL4JsonParseException, SQL4JsonExecutionException, SQL4JsonMappingException, SQL4JsonBindException {

    /**
     * Creates an exception with the given message.
     *
     * @param message detail message
     */
    public SQL4JsonException(String message) {
        super(message);
    }

    /**
     * Creates an exception with the given message and cause.
     *
     * @param message detail message
     * @param cause underlying cause
     */
    public SQL4JsonException(String message, Throwable cause) {
        super(message, cause);
    }
}
