// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.cli;

/**
 * Thrown by {@link ArgParser}, {@link ParamValueParser}, and {@link CliRunner} when the user supplies invalid
 * command-line arguments.
 *
 * <p>The CLI catches this exception and exits with status {@code 2}, printing only the message (no stack trace) to
 * {@code stderr}.
 *
 * @since 1.2.0
 */
final class UsageException extends RuntimeException {

    UsageException(String message) {
        super(message);
    }
}
