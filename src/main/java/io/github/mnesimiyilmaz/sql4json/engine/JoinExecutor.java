package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonFlattener;
import io.github.mnesimiyilmaz.sql4json.parser.JoinDef;
import io.github.mnesimiyilmaz.sql4json.parser.JoinEquality;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.*;

/**
 * Hash JOIN executor for INNER, LEFT, and RIGHT joins.
 * Builds a hash map from one side, probes from the other.
 */
final class JoinExecutor {

    private JoinExecutor() {
    }

    /**
     * Execute a single JOIN step.
     *
     * @param left           rows from the left (accumulated) side
     * @param right          rows from the right (joined) side
     * @param oppositeSchema schema of the "other" side for NULL padding —
     *                       for LEFT join: right-side schema; for RIGHT join: left-side schema
     * @param joinDef        the join definition (type, ON conditions)
     * @return merged rows
     */
    static List<Row> execute(List<Row> left, List<Row> right,
                             Set<FieldKey> oppositeSchema, JoinDef joinDef, int maxRows) {
        return switch (joinDef.joinType()) {
            case INNER -> innerJoin(left, right, joinDef.onConditions(), maxRows);
            case LEFT -> leftJoin(left, right, joinDef.onConditions(), oppositeSchema, maxRows);
            case RIGHT -> {
                var swapped = swapConditions(joinDef.onConditions());
                yield leftJoin(right, left, swapped, oppositeSchema, maxRows);
            }
        };
    }

    private static List<Row> innerJoin(List<Row> left, List<Row> right,
                                       List<JoinEquality> conditions, int maxRows) {
        boolean buildFromRight = right.size() <= left.size();
        List<Row> buildSide = buildFromRight ? right : left;
        List<Row> probeSide = buildFromRight ? left : right;

        Map<JoinKey, List<Row>> hashMap = buildHashMap(buildSide, conditions, buildFromRight);

        var result = new ArrayList<Row>();
        for (Row probeRow : probeSide) {
            JoinKey key = extractKey(probeRow, conditions, !buildFromRight);
            List<Row> matches = hashMap.getOrDefault(key, List.of());
            for (Row match : matches) {
                checkRowLimit(result.size(), maxRows);
                Row leftRow = buildFromRight ? probeRow : match;
                Row rightRow = buildFromRight ? match : probeRow;
                result.add(mergeRows(leftRow, rightRow));
            }
        }
        return result;
    }

    private static List<Row> leftJoin(List<Row> left, List<Row> right,
                                      List<JoinEquality> conditions,
                                      Set<FieldKey> rightSchema, int maxRows) {
        Map<JoinKey, List<Row>> rightMap = buildHashMap(right, conditions, true);

        var result = new ArrayList<Row>();
        for (Row leftRow : left) {
            JoinKey key = extractKey(leftRow, conditions, false);
            List<Row> matches = rightMap.get(key);
            if (matches != null && !matches.isEmpty()) {
                for (Row match : matches) {
                    checkRowLimit(result.size(), maxRows);
                    result.add(mergeRows(leftRow, match));
                }
            } else {
                checkRowLimit(result.size(), maxRows);
                result.add(mergeWithNulls(leftRow, rightSchema));
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

    private static Map<JoinKey, List<Row>> buildHashMap(List<Row> rows,
                                                        List<JoinEquality> conditions,
                                                        boolean useRightPaths) {
        var map = new HashMap<JoinKey, List<Row>>();
        for (Row row : rows) {
            JoinKey key = extractKey(row, conditions, useRightPaths);
            map.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
        return map;
    }

    /**
     * Extract key values from a row for the given conditions.
     *
     * @param useRightPaths true → use rightPath from each equality; false → use leftPath
     */
    private static JoinKey extractKey(Row row, List<JoinEquality> conditions,
                                      boolean useRightPaths) {
        var values = new ArrayList<SqlValue>(conditions.size());
        for (JoinEquality eq : conditions) {
            String path = useRightPaths ? eq.rightPath() : eq.leftPath();
            values.add(row.get(FieldKey.of(path)));
        }
        return JoinKey.of(values);
    }

    private static Row mergeRows(Row left, Row right) {
        var merged = new HashMap<FieldKey, SqlValue>();
        left.entries().forEach(e -> merged.put(e.getKey(), e.getValue()));
        right.entries().forEach(e -> merged.put(e.getKey(), e.getValue()));
        return Row.eager(merged);
    }

    private static Row mergeWithNulls(Row left, Set<FieldKey> rightSchema) {
        var merged = new HashMap<FieldKey, SqlValue>();
        left.entries().forEach(e -> merged.put(e.getKey(), e.getValue()));
        rightSchema.forEach(k -> merged.put(k, SqlNull.INSTANCE));
        return Row.eager(merged);
    }

    private static List<JoinEquality> swapConditions(List<JoinEquality> conditions) {
        return conditions.stream()
                .map(eq -> new JoinEquality(eq.rightPath(), eq.leftPath()))
                .toList();
    }

    /**
     * Flatten a JSON array source into alias-prefixed rows.
     * Returns both the rows and the collected schema (all unique field keys).
     */
    static FlattenedSource flattenSource(JsonValue data, String alias, FieldKey.Interner interner) {
        if (!(data instanceof JsonArrayValue(var elements))) {
            // Single object — wrap in single-element list
            var fields = new HashMap<FieldKey, SqlValue>();
            JsonFlattener.flattenInto(data, alias, fields, interner);
            var row = Row.eager(Collections.unmodifiableMap(fields));
            return new FlattenedSource(List.of(row), row.keys());
        }
        var rows = new ArrayList<Row>(elements.size());
        var schema = new LinkedHashSet<FieldKey>();
        for (JsonValue element : elements) {
            var fields = new HashMap<FieldKey, SqlValue>();
            JsonFlattener.flattenInto(element, alias, fields, interner);
            var row = Row.eager(Collections.unmodifiableMap(fields));
            rows.add(row);
            schema.addAll(row.keys());
        }
        return new FlattenedSource(Collections.unmodifiableList(rows), Collections.unmodifiableSet(schema));
    }

    record FlattenedSource(List<Row> rows, Set<FieldKey> schema) {
    }
}
