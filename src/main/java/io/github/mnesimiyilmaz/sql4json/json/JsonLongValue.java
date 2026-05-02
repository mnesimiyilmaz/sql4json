// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

/**
 * JSON integer value backed by an unboxed {@code long}.
 *
 * <p>Avoids the boxed-{@link Number} field that the legacy {@code record JsonNumberValue(Number)} carried — the storage
 * footprint is roughly half of the legacy shape on 64-bit JVMs.
 *
 * @param value the long value
 * @since 1.2.0
 */
public record JsonLongValue(long value) implements JsonNumberValue {

    @Override
    public Number numberValue() {
        return value;
    }
}
