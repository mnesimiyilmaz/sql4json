// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Flattens nested {@link JsonValue} trees into flat key-value rows for SQL processing, and provides path navigation
 * within JSON structures.
 */
public final class JsonFlattener {

    private JsonFlattener() {}

    /**
     * Primary: Stream of lazy rows — each row holds its original JsonValue reference. No upfront flatten. Fields are
     * resolved on demand via Row.get().
     *
     * @param root the root JSON value to stream from
     * @param rootPath the dot-separated path within the root (e.g. {@code "$r.data.items"})
     * @param interner the field key interner for deduplicating keys
     * @return a stream of lazily-flattened rows
     */
    public static Stream<Row> streamLazy(JsonValue root, String rootPath, FieldKey.Interner interner) {
        JsonValue target = navigateToPath(root, rootPath);
        return switch (target) {
            case JsonArrayValue(var elements) -> elements.stream().map(element -> Row.lazy(element, interner));
            case JsonObjectValue ignored -> Stream.of(Row.lazy(target, interner));
            default -> Stream.empty();
        };
    }

    /**
     * Full flatten into an existing cache map — called by Row.ensureFullyFlattened().
     *
     * @param element the JSON element to flatten
     * @param target the map to populate with flattened key-value pairs
     * @param interner the field key interner for deduplicating keys
     */
    public static void flattenInto(JsonValue element, Map<FieldKey, SqlValue> target, FieldKey.Interner interner) {
        flattenInto(element, null, target, interner);
    }

    /**
     * Full flatten with optional alias prefix. When prefix is non-null, all field keys are prefixed: "alias.fieldName".
     * Used by JoinExecutor to produce alias-qualified rows.
     *
     * @param element the JSON element to flatten
     * @param prefix optional alias prefix for all field keys, or {@code null}
     * @param target the map to populate with flattened key-value pairs
     * @param interner the field key interner for deduplicating keys
     */
    public static void flattenInto(
            JsonValue element, String prefix, Map<FieldKey, SqlValue> target, FieldKey.Interner interner) {
        StringBuilder path = new StringBuilder(64);
        if (prefix != null) path.append(prefix);
        flattenRecursive(element, path, target, interner);
    }

    /**
     * Navigate from root to the path specified in the FROM clause. "$r" or null returns root. "$r.data.items" navigates
     * to root.data.items.
     *
     * @param root the root JSON value
     * @param rootPath the dot-separated path (e.g. {@code "$r.data.items"}), or {@code null}
     * @return the JSON value at the specified path
     */
    public static JsonValue navigateToPath(JsonValue root, String rootPath) {
        if (rootPath == null || rootPath.equals("$r")) return root;
        String path = rootPath.startsWith("$r.") ? rootPath.substring(3) : rootPath;
        JsonValue current = root;
        for (String segment : path.split("\\.")) {
            current = current.asObject()
                    .map(m -> m.getOrDefault(segment, JsonNullValue.INSTANCE))
                    .orElse(JsonNullValue.INSTANCE);
        }
        return current;
    }

    private static void flattenRecursive(
            JsonValue value, StringBuilder path, Map<FieldKey, SqlValue> result, FieldKey.Interner interner) {
        switch (value) {
            case JsonObjectValue(var fields) -> {
                int baseLen = path.length();
                for (var entry : fields.entrySet()) {
                    path.setLength(baseLen);
                    if (baseLen > 0) path.append('.');
                    path.append(entry.getKey());
                    flattenRecursive(entry.getValue(), path, result, interner);
                }
                path.setLength(baseLen);
            }
            case JsonArrayValue(var elements) -> {
                int baseLen = path.length();
                for (int i = 0; i < elements.size(); i++) {
                    path.setLength(baseLen);
                    path.append('[').append(i).append(']');
                    flattenRecursive(elements.get(i), path, result, interner);
                }
                path.setLength(baseLen);
            }
            default -> {
                String key = path.toString();
                result.put(
                        interner != null ? FieldKey.of(key, interner) : FieldKey.of(key),
                        JsonToSqlConverter.toSqlValue(value));
            }
        }
    }
}
