// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.exception;

/** Thrown when an error occurs during query execution (e.g., unsupported operation, type mismatch). */
public final class SQL4JsonExecutionException extends SQL4JsonException {

    /**
     * Creates an execution exception with the given message.
     *
     * @param message detail message
     */
    public SQL4JsonExecutionException(String message) {
        super(message);
    }

    /**
     * Creates an execution exception with the given message and cause.
     *
     * @param message detail message
     * @param cause underlying cause
     */
    public SQL4JsonExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
