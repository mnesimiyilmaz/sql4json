// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.settings;

/**
 * Controls how {@link io.github.mnesimiyilmaz.sql4json.mapper.JsonValueMapper} handles JSON objects that are missing
 * fields required by the target type.
 *
 * <p>Applies to record components, POJO setters, and any target that would otherwise be populated from a JSON key that
 * isn't present.
 *
 * @see MappingSettings
 * @since 1.1.0
 */
public enum MissingFieldPolicy {

    /**
     * Production default. Forward-compatible with schema evolution. Missing fields produce:
     *
     * <ul>
     *   <li>Reference target → {@code null}
     *   <li>{@code Optional<?>} → {@code Optional.empty()}
     *   <li>Primitive target → Java default ({@code 0}, {@code false}, {@code '\0'})
     *   <li>Collection target → empty collection
     * </ul>
     */
    IGNORE,

    /**
     * Development / CI default. Surfaces schema drift early. Missing fields throw
     * {@link io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException} with the JSON path of the missing
     * field.
     */
    FAIL
}
