// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.settings;

/**
 * Policy for handling duplicate keys in a JSON object during parsing by {@link DefaultJsonCodecSettings}.
 *
 * <p>RFC 8259 permits multiple strategies; the choice matters for security because two systems that disagree on which
 * duplicate wins can desynchronize — analogous to HTTP Parameter Pollution attacks. Defaults to {@link #REJECT} for
 * fail-safe behavior.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * DefaultJsonCodecSettings codecSettings = DefaultJsonCodecSettings.builder()
 *     .duplicateKeyPolicy(DuplicateKeyPolicy.LAST_WINS)
 *     .build();
 * }</pre>
 *
 * <p>Thread-safe: enum constants are intrinsically thread-safe.
 *
 * @see DefaultJsonCodecSettings
 * @see io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec
 */
public enum DuplicateKeyPolicy {
    /** Throw on any duplicate key. Safest default. */
    REJECT,
    /** Keep the last occurrence (current legacy behavior). */
    LAST_WINS,
    /** Keep the first occurrence (matches Go's encoding/json). */
    FIRST_WINS
}
