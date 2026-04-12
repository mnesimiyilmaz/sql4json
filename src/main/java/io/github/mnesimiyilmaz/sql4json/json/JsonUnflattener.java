package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reconstructs nested {@link JsonValue} trees from flat rows produced by the query pipeline.
 */
public final class JsonUnflattener {

    private static final int MAX_UNFLATTEN_ARRAY_INDEX = 10_000_000;

    private JsonUnflattener() {
    }

    /**
     * Convenience overload for callers that don't need scalar function application.
     *
     * @param rows    the flat rows to unflatten
     * @param columns the selected column definitions
     * @return the reconstructed JSON value
     */
    public static JsonValue unflatten(List<Row> rows, List<SelectColumnDef> columns) {
        return unflatten(rows, columns, null);
    }

    /**
     * Unflattens the given rows into a JSON array value, applying scalar functions as needed.
     *
     * @param rows             the flat rows to unflatten
     * @param columns          the selected column definitions
     * @param functionRegistry the function registry for expression evaluation, or {@code null}
     * @return the reconstructed JSON value
     */
    public static JsonValue unflatten(List<Row> rows, List<SelectColumnDef> columns,
                                      FunctionRegistry functionRegistry) {
        List<JsonValue> results = new ArrayList<>(rows.size());
        for (Row row : rows) {
            results.add(unflattenRow(row, columns, functionRegistry));
        }
        return new JsonArrayValue(results);
    }

    static JsonValue unflattenRow(Row row, List<SelectColumnDef> columns,
                                  FunctionRegistry functionRegistry) {
        // PATH 1: Row has original and was not modified → cherry-pick from original
        if (row.originalValue().isPresent() && !row.isModified()) {
            return cherryPick(row.originalValue().orElseThrow(), row, columns, functionRegistry);
        }
        // PATH 2: Aggregated / modified row → reconstruct from flat map
        return reconstructFromFlatMap(row, columns);
    }

    /**
     * Cherry-pick: navigate original JsonValue for selected fields only.
     * Applies expression evaluation via ExpressionEvaluator for non-column-ref expressions.
     * Column paths with dots (without an explicit alias) are kept as flat output keys.
     */
    private static JsonValue cherryPick(JsonValue original, Row row,
                                        List<SelectColumnDef> columns,
                                        FunctionRegistry functionRegistry) {
        // SELECT * → return the entire original document as-is
        if (columns.size() == 1 && columns.getFirst().isAsterisk()) {
            return original;
        }
        // Create a temporary row from the original for expression evaluation
        // (NOT the pipeline row, which may have been projected)
        Row tempRow = Row.lazy(original, new FieldKey.Interner());
        var root = new LinkedHashMap<String, Object>();
        for (SelectColumnDef col : columns) {
            JsonValue value;
            if (col.containsWindow()) {
                // Window results are pre-computed by WindowStage, stored in row's windowResults/cache
                SqlValue result = row.get(FieldKey.of(col.aliasOrName()));
                value = JsonToSqlConverter.toJsonValue(result);
            } else if (col.expression() instanceof Expression.ColumnRef(var path)) {
                // Plain column — navigate original JSON (preserves structure)
                value = navigateToField(original, path);
            } else if (functionRegistry != null) {
                // Expression with functions — evaluate via ExpressionEvaluator
                SqlValue result = ExpressionEvaluator.evaluate(col.expression(), tempRow, functionRegistry);
                value = JsonToSqlConverter.toJsonValue(result);
            } else {
                // No registry and not a plain column — fall back to innermost column if available
                String path = col.columnName();
                value = path != null ? navigateToField(original, path) : JsonNullValue.INSTANCE;
            }
            // Only unflatten when an explicit dotted alias is provided
            if (col.alias() != null && col.alias().contains(".")) {
                setNestedField(root, col.alias(), value);
            } else {
                root.put(col.aliasOrName(), value);
            }
        }
        return buildJsonObject(root);
    }

    /**
     * Reconstruct: build structured JSON from flat SqlValue map.
     * Used for GROUP BY / aggregation results where there is no original JsonValue.
     * Explicit dotted aliases are unflattened; other keys are kept flat.
     */
    private static JsonValue reconstructFromFlatMap(Row row, List<SelectColumnDef> columns) {
        var root = new LinkedHashMap<String, Object>();
        for (SelectColumnDef col : columns) {
            if (col.isAsterisk()) {
                // SELECT * → reconstruct nested structure from flat field paths
                row.entries().forEach(e -> insertAtPath(root, e.getKey().getKey(),
                        JsonToSqlConverter.toJsonValue(e.getValue())));
            } else {
                // Aggregate columns are stored under aliasOrName() by GroupAggregator.
                // Non-aggregate columns are stored under columnName() (falls back to aliasOrName).
                String keyName = col.containsAggregate() || col.columnName() == null
                        ? col.aliasOrName()
                        : col.columnName();
                FieldKey key = FieldKey.of(keyName);
                SqlValue val = row.get(key);
                JsonValue jv = JsonToSqlConverter.toJsonValue(val);
                if (col.alias() != null && col.alias().contains(".")) {
                    setNestedField(root, col.alias(), jv);
                } else {
                    root.put(col.aliasOrName(), jv);
                }
            }
        }
        return buildJsonObject(root);
    }

    /**
     * Set a value at a dot-separated path within a nested map structure.
     * E.g. setNestedField(root, "user.name", value) sets root["user"]["name"] = value.
     */
    @SuppressWarnings("unchecked")
    private static void setNestedField(Map<String, Object> root, String dottedKey,
                                       JsonValue value) {
        String[] parts = dottedKey.split("\\.");
        Map<String, Object> current = root;
        for (int i = 0; i < parts.length - 1; i++) {
            Object existing = current.get(parts[i]);
            if (!(existing instanceof Map)) {
                existing = new LinkedHashMap<String, Object>();
                current.put(parts[i], existing);
            }
            current = (Map<String, Object>) existing;
        }
        current.put(parts[parts.length - 1], value);
    }

    /**
     * Insert a value at a flat field path, reconstructing nested objects and arrays.
     * Handles dot-separated paths ("address.city") and array indices ("tags[0]").
     * E.g. "address.geo.lat" → {"address":{"geo":{"lat":value}}}
     * "tags[0]"        → {"tags":["value"]}
     */
    @SuppressWarnings("unchecked")
    private static void insertAtPath(Map<String, Object> root, String path, JsonValue value) {
        String[] segments = path.split("\\.");
        Object current = root;

        for (int i = 0; i < segments.length; i++) {
            String seg = segments[i];
            boolean isLast = (i == segments.length - 1);
            int bracket = seg.indexOf('[');

            if (bracket >= 0) {
                current = insertArraySegment((Map<String, Object>) current, seg, bracket,
                        isLast, value);
                if (current == null) break;
            } else {
                Map<String, Object> map = (Map<String, Object>) current;
                if (isLast) {
                    map.put(seg, value);
                } else {
                    current = map.computeIfAbsent(seg, k -> new LinkedHashMap<String, Object>());
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Object insertArraySegment(Map<String, Object> map, String seg, int bracket,
                                             boolean isLast, JsonValue value) {
        String field = seg.substring(0, bracket);
        int idx = Integer.parseInt(seg.substring(bracket + 1, seg.length() - 1));
        if (idx < 0 || idx > MAX_UNFLATTEN_ARRAY_INDEX) {
            throw new SQL4JsonExecutionException(
                    "Unflatten array index out of range: " + idx);
        }
        List<Object> list = (List<Object>) map.computeIfAbsent(field, k -> new ArrayList<>());
        while (list.size() <= idx) list.add(null);
        if (isLast) {
            list.set(idx, value);
            return null;
        }
        if (!(list.get(idx) instanceof Map)) {
            list.set(idx, new LinkedHashMap<String, Object>());
        }
        return list.get(idx);
    }

    /**
     * Build a JsonObjectValue from a nested map.
     * Values may be JsonValue, Map (nested object), or List (array).
     */
    private static JsonObjectValue buildJsonObject(Map<String, Object> map) {
        var fields = new LinkedHashMap<String, JsonValue>();
        for (var entry : map.entrySet()) {
            fields.put(entry.getKey(), buildJsonValue(entry.getValue()));
        }
        return new JsonObjectValue(fields);
    }

    @SuppressWarnings("unchecked")
    private static JsonValue buildJsonValue(Object obj) {
        if (obj instanceof JsonValue jv) return jv;
        if (obj instanceof Map<?, ?> map) return buildJsonObject((Map<String, Object>) map);
        if (obj instanceof List<?> list) {
            List<JsonValue> elements = new ArrayList<>(list.size());
            for (Object item : list) {
                elements.add(item == null ? JsonNullValue.INSTANCE : buildJsonValue(item));
            }
            return new JsonArrayValue(elements);
        }
        return JsonNullValue.INSTANCE;
    }

    private static JsonValue navigateToField(JsonValue root, String path) {
        JsonValue current = root;
        for (String segment : path.split("\\.")) {
            current = current.asObject()
                    .map(m -> m.getOrDefault(segment, JsonNullValue.INSTANCE))
                    .orElse(JsonNullValue.INSTANCE);
        }
        return current;
    }
}
