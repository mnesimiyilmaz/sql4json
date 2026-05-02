// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonToSqlConverter;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Locates a {@link JsonArrayValue} for a column-ref path on a row, preferring original-value navigation and falling
 * back to flat-key family reassembly for rows that have lost their original (post-JOIN merged rows).
 *
 * <p>Used by {@code ArrayPredicateConditionHandler} to read the LHS array of {@code CONTAINS}, {@code @>}, {@code <@},
 * {@code &&}, and array equality without materialising a {@code SqlArray} type.
 */
final class ArrayPathNavigator {

    private ArrayPathNavigator() {
        // utility class — not instantiable
    }

    /**
     * Locates a {@link JsonArrayValue} at the given column path on a row.
     *
     * @param row the row whose array field to locate
     * @param path dot-separated column path (e.g. {@code "tags"} or {@code "u.tags"})
     * @return the array at {@code path} on {@code row}, or {@code null} if the field is missing, JSON null, or not an
     *     array. Indexed paths (e.g. {@code "tags[0]"}) always return null — they refer to a scalar element, not an
     *     array.
     */
    static JsonArrayValue navigateToArray(RowAccessor row, String path) {
        if (path.indexOf('[') >= 0) {
            // Indexed path resolves to a scalar element, not an array.
            return null;
        }
        Optional<JsonValue> original = row.originalValue();
        if (original.isPresent()) {
            return navigateOriginal(original.get(), path);
        }
        return reassembleFromFamily(row, path);
    }

    private static JsonArrayValue navigateOriginal(JsonValue root, String path) {
        JsonValue cursor = root;
        for (String segment : path.split("\\.")) {
            if (!(cursor instanceof JsonObjectValue(Map<String, JsonValue> fields))) {
                return null;
            }
            JsonValue next = fields.get(segment);
            if (next == null) {
                return null;
            }
            cursor = next;
        }
        if (cursor instanceof JsonArrayValue arr) {
            return arr;
        }
        return null;
    }

    private static JsonArrayValue reassembleFromFamily(RowAccessor row, String path) {
        List<SqlValue> values = row.valuesByFamily(path);
        if (values.isEmpty()) {
            return null;
        }
        List<JsonValue> elements = new ArrayList<>(values.size());
        for (SqlValue v : values) {
            elements.add(JsonToSqlConverter.toJsonValue(v));
        }
        return new JsonArrayValue(List.copyOf(elements));
    }
}
