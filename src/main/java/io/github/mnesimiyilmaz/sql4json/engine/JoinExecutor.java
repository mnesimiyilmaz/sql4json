package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.internal.SkipCoverageGenerated;
import io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonFlattener;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.parser.JoinDef;
import io.github.mnesimiyilmaz.sql4json.parser.JoinEquality;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hash JOIN executor for INNER, LEFT, and RIGHT joins.
 * Builds a hash map from one side, probes from the other.
 *
 * <p>As of 1.2.0 the executor operates on {@link FlatRow} end-to-end. Inputs and
 * outputs are {@code List<FlatRow>} sharing a {@link RowSchema} computed once
 * per join step by the caller via {@link RowSchema#concat(RowSchema)}. The
 * merged schema is always {@code leftSchema.concat(rightSchema)} regardless of
 * join direction; for RIGHT joins the inputs are swapped internally but the
 * resulting columns retain the original left-then-right ordering.</p>
 */
final class JoinExecutor {

    private JoinExecutor() {
        // Utility class — no instances.
    }

    /**
     * Execute a single JOIN step.
     *
     * @param left         rows from the left (accumulated) side
     * @param right        rows from the right (joined) side
     * @param mergedSchema the combined output schema, computed by the caller as
     *                     {@code leftSchema.concat(rightSchema)} so column
     *                     ordering matches the user's expectation across all
     *                     join types
     * @param joinDef      the join definition (type, ON conditions)
     * @param maxRows      the row-count cap from the active settings
     * @return merged rows
     */
    static List<FlatRow> execute(List<FlatRow> left, List<FlatRow> right,
                                 RowSchema mergedSchema, JoinDef joinDef, int maxRows) {
        return switch (joinDef.joinType()) {
            case INNER -> innerJoin(left, right, joinDef.onConditions(), mergedSchema, maxRows);
            case LEFT -> leftJoin(left, right, joinDef.onConditions(), mergedSchema, maxRows);
            case RIGHT -> {
                var swapped = swapConditions(joinDef.onConditions());
                yield leftJoin(right, left, swapped, mergedSchema, maxRows);
            }
        };
    }

    private static List<FlatRow> innerJoin(List<FlatRow> left, List<FlatRow> right,
                                           List<JoinEquality> conditions,
                                           RowSchema mergedSchema, int maxRows) {
        boolean buildFromRight = right.size() <= left.size();
        List<FlatRow> buildSide = buildFromRight ? right : left;
        List<FlatRow> probeSide = buildFromRight ? left : right;

        Map<JoinKey, List<FlatRow>> hashMap = buildHashMap(buildSide, conditions, buildFromRight);

        var result = new ArrayList<FlatRow>();
        for (FlatRow probeRow : probeSide) {
            JoinKey key = extractKey(probeRow, conditions, !buildFromRight);
            List<FlatRow> matches = hashMap.getOrDefault(key, List.of());
            for (FlatRow match : matches) {
                checkRowLimit(result.size(), maxRows);
                FlatRow leftRow = buildFromRight ? probeRow : match;
                FlatRow rightRow = buildFromRight ? match : probeRow;
                result.add(mergeRows(leftRow, rightRow, mergedSchema));
            }
        }
        return result;
    }

    private static List<FlatRow> leftJoin(List<FlatRow> left, List<FlatRow> right,
                                          List<JoinEquality> conditions,
                                          RowSchema mergedSchema, int maxRows) {
        Map<JoinKey, List<FlatRow>> rightMap = buildHashMap(right, conditions, true);

        var result = new ArrayList<FlatRow>();
        for (FlatRow leftRow : left) {
            JoinKey key = extractKey(leftRow, conditions, false);
            List<FlatRow> matches = rightMap.get(key);
            if (matches != null && !matches.isEmpty()) {
                for (FlatRow match : matches) {
                    checkRowLimit(result.size(), maxRows);
                    result.add(mergeRows(leftRow, match, mergedSchema));
                }
            } else {
                checkRowLimit(result.size(), maxRows);
                result.add(mergeWithNulls(leftRow, mergedSchema));
            }
        }
        return result;
    }

    private static void checkRowLimit(int currentSize, int maxRows) {
        if (currentSize >= maxRows) {
            throw new SQL4JsonExecutionException(
                    "JOIN row count exceeds configured maximum (" + maxRows + ")");
        }
    }

    private static Map<JoinKey, List<FlatRow>> buildHashMap(List<FlatRow> rows,
                                                            List<JoinEquality> conditions,
                                                            boolean useRightPaths) {
        var map = new HashMap<JoinKey, List<FlatRow>>();
        for (FlatRow row : rows) {
            JoinKey key = extractKey(row, conditions, useRightPaths);
            map.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
        return map;
    }

    /**
     * Extract key values from a row for the given conditions.
     *
     * @param row            the row to extract values from
     * @param conditions     the equality conditions
     * @param useRightPaths  {@code true} → use rightPath from each equality; {@code false} → use leftPath
     * @return the composite key
     */
    private static JoinKey extractKey(FlatRow row, List<JoinEquality> conditions,
                                      boolean useRightPaths) {
        var values = new ArrayList<SqlValue>(conditions.size());
        for (JoinEquality eq : conditions) {
            String path = useRightPaths ? eq.rightPath() : eq.leftPath();
            values.add(row.get(FieldKey.of(path)));
        }
        return JoinKey.of(values);
    }

    private static FlatRow mergeRows(FlatRow left, FlatRow right, RowSchema mergedSchema) {
        Object[] vals = new Object[mergedSchema.size()];
        copyInto(vals, mergedSchema, left);
        copyInto(vals, mergedSchema, right);
        return FlatRow.of(mergedSchema, vals);
    }

    private static FlatRow mergeWithNulls(FlatRow left, RowSchema mergedSchema) {
        Object[] vals = new Object[mergedSchema.size()];
        copyInto(vals, mergedSchema, left);
        // Right-side ordinals stay null (decoded as SqlNull on read).
        return FlatRow.of(mergedSchema, vals);
    }

    private static void copyInto(Object[] dst, RowSchema dstSchema, FlatRow src) {
        RowSchema srcSchema = src.schema();
        for (int i = 0; i < srcSchema.size(); i++) {
            int dstOrd = dstSchema.indexOf(srcSchema.columnAt(i));
            if (dstOrd >= 0) {
                SqlValue v = src.get(i);
                if (!(v instanceof SqlNull)) {
                    dst[dstOrd] = v;
                }
            }
        }
    }

    private static List<JoinEquality> swapConditions(List<JoinEquality> conditions) {
        return conditions.stream()
                .map(eq -> new JoinEquality(eq.rightPath(), eq.leftPath()))
                .toList();
    }

    /**
     * Flatten a JSON array source into alias-prefixed {@link FlatRow}s sharing a
     * single {@link RowSchema} that captures every field key seen across the
     * batch.
     *
     * @param data     the JSON value to flatten (array or single object)
     * @param alias    the table alias used as a column prefix
     * @param interner the field-key interner for deduplication
     * @return the flattened rows together with the unified schema
     */
    static FlattenedSource flattenSource(JsonValue data, String alias, FieldKey.Interner interner) {
        if (!(data instanceof JsonArrayValue(var elements))) {
            // Single object — wrap in single-element list.
            var fields = HashMap.<FieldKey, SqlValue>newHashMap(estimateFlatSize(data));
            JsonFlattener.flattenInto(data, alias, fields, interner);
            return wrapAsFlat(List.of(fields));
        }
        var rowMaps = new ArrayList<Map<FieldKey, SqlValue>>(elements.size());
        for (JsonValue element : elements) {
            var fields = HashMap.<FieldKey, SqlValue>newHashMap(estimateFlatSize(element));
            JsonFlattener.flattenInto(element, alias, fields, interner);
            rowMaps.add(fields);
        }
        return wrapAsFlat(rowMaps);
    }

    /**
     * Hint for {@link HashMap#newHashMap(int)} — keeps the per-row hash table
     * from resizing 3-4 times during full flatten on typical 13-field rows.
     * Falls back to a small default for non-object inputs (rare — primitive
     * bound as a table source).
     */
    @SkipCoverageGenerated
    private static int estimateFlatSize(JsonValue v) {
        if (v instanceof JsonObjectValue(var fields)) {
            return Math.max(8, fields.size() + (fields.size() >>> 1));
        }
        return 8;
    }

    private static FlattenedSource wrapAsFlat(List<Map<FieldKey, SqlValue>> rowMaps) {
        var schemaBuilder = new RowSchema.Builder();
        for (var m : rowMaps) m.keySet().forEach(schemaBuilder::add);
        RowSchema schema = schemaBuilder.build();
        var rows = new ArrayList<FlatRow>(rowMaps.size());
        for (var m : rowMaps) {
            Object[] vals = new Object[schema.size()];
            for (int i = 0; i < schema.size(); i++) {
                FieldKey k = schema.columnAt(i);
                SqlValue v = m.get(k);
                if (v != null && !(v instanceof SqlNull)) vals[i] = v;
            }
            rows.add(FlatRow.of(schema, vals));
        }
        return new FlattenedSource(Collections.unmodifiableList(rows), schema);
    }

    /**
     * Container for the output of {@link #flattenSource(JsonValue, String, FieldKey.Interner)}.
     *
     * @param rows   the flattened rows in source order
     * @param schema the unified schema covering every field key in {@code rows}
     */
    record FlattenedSource(List<FlatRow> rows, RowSchema schema) {
    }
}
