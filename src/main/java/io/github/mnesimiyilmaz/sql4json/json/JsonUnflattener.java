package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reconstructs nested {@link JsonValue} trees from flat rows produced by the query pipeline.
 *
 * <p>Since 1.2.0 the entry point accepts {@code List<RowAccessor>} — both lazy
 * {@link Row} and materialised {@link io.github.mnesimiyilmaz.sql4json.engine.FlatRow}
 * are supported. Aggregated rows from GROUP BY are read flat (their values are
 * pre-evaluated against the SELECT list); other rows go through expression
 * evaluation, with optional cherry-pick navigation when the row retains its
 * original {@link JsonValue}.</p>
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
    public static JsonValue unflatten(List<RowAccessor> rows, List<SelectColumnDef> columns) {
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
    public static JsonValue unflatten(List<RowAccessor> rows, List<SelectColumnDef> columns,
                                      FunctionRegistry functionRegistry) {
        List<JsonValue> results = new ArrayList<>(rows.size());
        for (RowAccessor row : rows) {
            results.add(unflattenRow(row, columns, functionRegistry));
        }
        return new JsonArrayValue(results);
    }

    /**
     * Unflattens a single row through the SELECT column list. GROUP BY output is
     * read as-is from the row (values are pre-evaluated by
     * {@link io.github.mnesimiyilmaz.sql4json.grouping.GroupAggregator}); other
     * rows are projected via {@link ExpressionEvaluator}.
     *
     * @param row              the row to unflatten
     * @param columns          the selected column definitions
     * @param functionRegistry the function registry, or {@code null}
     * @return the reconstructed JSON value for {@code row}
     */
    public static JsonValue unflattenRow(RowAccessor row, List<SelectColumnDef> columns,
                                         FunctionRegistry functionRegistry) {
        if (row.isAggregated()) {
            return reconstructFromAggregatedRow(row, columns, functionRegistry);
        }
        return projectFromRow(row, columns, functionRegistry);
    }

    /**
     * Project a non-aggregated row through the SELECT column list. Evaluates
     * literal / scalar-function / case expressions via {@link ExpressionEvaluator};
     * for plain column refs, prefers navigating the original JsonValue when
     * available (so nested object/array structure is preserved).
     */
    private static JsonValue projectFromRow(RowAccessor row, List<SelectColumnDef> columns,
                                            FunctionRegistry functionRegistry) {
        JsonValue original = row.originalValue().orElse(null);

        // SELECT * → return original as-is when available; otherwise reconstruct nested
        // structure from the flat field map (pre-flattened / JOIN rows).
        if (columns.size() == 1 && columns.getFirst().isAsterisk()) {
            return projectAsterisk(row, original);
        }

        // When the row retains an original JsonValue (lazy {@link Row}, or a
        // {@link io.github.mnesimiyilmaz.sql4json.engine.FlatRow} produced by
        // SelectStage materialisation), evaluate against a fresh lazy row over
        // that original — the pipeline row may have been projected to a subset
        // of fields, and computed expressions (e.g. {@code CONCAT(name, ' - ', dept)})
        // need the full source.
        // When the pipeline row carries WindowStage-precomputed window results, keep the
        // pipeline row in scope so ExpressionEvaluator can resolve nested WindowFnCall
        // references — including those buried inside CASE WHEN conditions, where
        // SelectColumnDef.containsWindow() can't see them (the CriteriaNode closure is
        // opaque to expression-tree walking).
        RowAccessor evalRow = (original != null && !row.hasWindowResults())
                ? Row.lazy(original, new FieldKey.Interner())
                : row;

        var root = new LinkedHashMap<String, Object>();
        for (SelectColumnDef col : columns) {
            JsonValue value = resolveColumnValue(col, row, evalRow, original, functionRegistry);
            if (col.alias() != null && col.alias().contains(".")) {
                setNestedField(root, col.alias(), value);
            } else {
                root.put(col.aliasOrName(), value);
            }
        }
        return buildJsonObject(root);
    }

    private static JsonValue projectAsterisk(RowAccessor row, JsonValue original) {
        if (original != null) return original;
        var root = new LinkedHashMap<String, Object>();
        row.entries().forEach(e -> insertAtPath(root, e.getKey().getKey(),
                JsonToSqlConverter.toJsonValue(e.getValue())));
        return buildJsonObject(root);
    }

    private static JsonValue resolveColumnValue(SelectColumnDef col, RowAccessor row, RowAccessor evalRow,
                                                JsonValue original, FunctionRegistry functionRegistry) {
        // Plain column with original — navigate to preserve nested structure.
        // ColumnRef can never contain a window, so no extra guard needed.
        if (col.expression() instanceof Expression.ColumnRef(var path) && original != null) {
            return navigateToField(original, path);
        }
        // Literal / scalar fn / case / column-ref-without-original / window — evaluate.
        // WindowFnCall nodes inside the tree resolve via the row's RowSchema-indexed window slot.
        if (functionRegistry != null) {
            SqlValue result = ExpressionEvaluator.evaluate(col.expression(), evalRow, functionRegistry);
            return JsonToSqlConverter.toJsonValue(result);
        }
        // No registry, no original — fall back to raw flat-map lookup.
        String path = col.columnName();
        FieldKey key = FieldKey.of(path != null ? path : col.aliasOrName());
        return JsonToSqlConverter.toJsonValue(row.get(key));
    }

    /**
     * Reconstruct: build structured JSON from an aggregated row's flat values.
     * Used for GROUP BY / aggregation results. Explicit dotted aliases are
     * unflattened; other keys are kept flat.
     */
    private static JsonValue reconstructFromAggregatedRow(RowAccessor row, List<SelectColumnDef> columns,
                                                          FunctionRegistry functionRegistry) {
        var root = new LinkedHashMap<String, Object>();
        for (SelectColumnDef col : columns) {
            if (col.isAsterisk()) {
                // SELECT * → reconstruct nested structure from flat field paths
                row.entries().forEach(e -> insertAtPath(root, e.getKey().getKey(),
                        JsonToSqlConverter.toJsonValue(e.getValue())));
                continue;
            }
            JsonValue jv;
            if (col.containsWindow() && functionRegistry != null) {
                // Window functions on GROUP BY output: WindowStage stored each WindowFnCall
                // result in the row's RowSchema-indexed window slot; evaluate the column
                // expression so any outer scalar/CASE wrapper is applied correctly.
                SqlValue val = ExpressionEvaluator.evaluate(col.expression(), row, functionRegistry);
                jv = JsonToSqlConverter.toJsonValue(val);
            } else {
                // Aggregate columns are stored under aliasOrName() by GroupAggregator.
                // Non-aggregate columns are stored under columnName() (falls back to aliasOrName).
                String keyName = col.containsAggregate() || col.columnName() == null
                        ? col.aliasOrName()
                        : col.columnName();
                FieldKey key = FieldKey.of(keyName);
                SqlValue val = row.get(key);
                jv = JsonToSqlConverter.toJsonValue(val);
            }
            if (col.alias() != null && col.alias().contains(".")) {
                setNestedField(root, col.alias(), jv);
            } else {
                root.put(col.aliasOrName(), jv);
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
     *
     * <p>Builds the {@link CompactStringMap}'s arrays directly to skip the
     * intermediate {@link LinkedHashMap} that the legacy shape allocated per
     * output row.</p>
     */
    private static JsonObjectValue buildJsonObject(Map<String, Object> map) {
        int size = map.size();
        if (size == 0) return new JsonObjectValue(Collections.emptyMap());
        String[] keys = new String[size];
        JsonValue[] values = new JsonValue[size];
        int i = 0;
        for (var entry : map.entrySet()) {
            keys[i] = entry.getKey();
            values[i] = buildJsonValue(entry.getValue());
            i++;
        }
        return new JsonObjectValue(new CompactStringMap<>(keys, values));
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
