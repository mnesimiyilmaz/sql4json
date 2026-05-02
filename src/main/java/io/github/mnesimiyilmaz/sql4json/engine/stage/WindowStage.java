// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.*;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.parser.OrderByColumnDef;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.sorting.SqlValueComparator;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Materializing pipeline stage that evaluates window functions. Position in pipeline: after HAVING, before ORDER BY.
 *
 * <p>Consumes the parser-collected list of {@link Expression.WindowFnCall} nodes and an alias map derived from SELECT
 * columns whose top-level expression IS a {@code WindowFnCall}. Builds a window-aware {@link RowSchema} via
 * {@link RowSchema#withWindowSlots(List, Map)} — the alias becomes the canonical column key for the slot, so
 * {@code ORDER BY alias} / any {@code ColumnRef} lookup against the alias resolves naturally through the schema index.
 *
 * <p>For each row, the stage allocates an {@code Object[]} working buffer sized to {@code schema.size()}, prefills with
 * input values, and writes window results into the appropriate slot ordinals. Each buffer is wrapped in a
 * {@link FlatRow} for the downstream {@code Stream<RowAccessor>} pipeline.
 *
 * <p>Wrapped windows like {@code ROUND(... OVER (...))} get a synthetic column key and resolve through
 * {@link RowAccessor#getWindowResult(Expression.WindowFnCall)} → {@link RowSchema#windowSlot(Expression.WindowFnCall)}.
 *
 * @since 1.2.0
 */
public final class WindowStage implements MaterializingPipelineStage {

    private final List<Expression.WindowFnCall> windowCalls;
    private final Map<Expression.WindowFnCall, FieldKey> aliasKeysByCall;
    private final Set<FieldKey> referencedColumns;
    private final boolean selectsAsterisk;
    private final FunctionRegistry functionRegistry;
    private final int maxRows;

    /**
     * Creates a WindowStage for the given window function call list.
     *
     * @param windowCalls every {@link Expression.WindowFnCall} the parser saw, in source order (typically
     *     {@code QueryDefinition.windowFunctionCalls()})
     * @param selectedColumns SELECT column definitions; used to derive an alias-{@link FieldKey} mirror for plain
     *     windowed columns so that ORDER BY / HAVING / any {@code ColumnRef} lookup against the alias resolves to the
     *     precomputed window value, and to detect {@code SELECT *}
     * @param referencedColumns the union of all field paths referenced by the query, derived from
     *     {@code QueryDefinition.referencedColumns()}; used as the input-side schema for non-asterisk queries to avoid
     *     an O(N×K) scan over input rows
     * @param functionRegistry function registry for expression evaluation
     * @param maxRows maximum rows to materialise before throwing
     * @since 1.2.0
     */
    public WindowStage(
            List<Expression.WindowFnCall> windowCalls,
            List<SelectColumnDef> selectedColumns,
            Set<FieldKey> referencedColumns,
            FunctionRegistry functionRegistry,
            int maxRows) {
        this.windowCalls = List.copyOf(windowCalls);
        this.aliasKeysByCall = buildAliasMap(selectedColumns);
        this.referencedColumns = Set.copyOf(referencedColumns);
        this.selectsAsterisk = selectedColumns.stream().anyMatch(SelectColumnDef::isAsterisk);
        this.functionRegistry = functionRegistry;
        this.maxRows = maxRows;
    }

    private static Map<Expression.WindowFnCall, FieldKey> buildAliasMap(List<SelectColumnDef> columns) {
        // Only plain windowed columns (top-level expression IS a WindowFnCall) get an alias mirror.
        // Wrapped variants like ROUND(window OVER (...), 2) have no meaningful alias to mirror —
        // the outer scalar is evaluated lazily by JsonUnflattener via ExpressionEvaluator, which
        // resolves the inner WindowFnCall through the schema slot directly.
        Map<Expression.WindowFnCall, FieldKey> out = new HashMap<>();
        for (SelectColumnDef col : columns) {
            if (col.expression() instanceof Expression.WindowFnCall wfc) {
                out.putIfAbsent(wfc, FieldKey.of(col.aliasOrName()));
            }
        }
        return out;
    }

    @Override
    public Stream<RowAccessor> apply(Stream<RowAccessor> input) {
        List<RowAccessor> allRows = StreamMaterializer.toList(input, maxRows, "WINDOW");
        if (allRows.isEmpty()) return Stream.empty();

        RowSchema baseSchema = collectBaseSchema(allRows);

        // Append window-result slots, mapping alias FieldKeys to slots so plain-
        // windowed SELECT columns resolve via the schema index naturally.
        Set<Expression.WindowFnCall> distinctCalls = new LinkedHashSet<>(windowCalls);
        RowSchema schema = baseSchema.withWindowSlots(new ArrayList<>(distinctCalls), aliasKeysByCall);

        Object[][] working = prefillWorkingBuffers(allRows, schema, baseSchema);

        // Group by WindowSpec — same OVER clause partitions+sorts identically.
        Map<WindowSpec, List<Expression.WindowFnCall>> bySpec = distinctCalls.stream()
                .collect(Collectors.groupingBy(Expression.WindowFnCall::spec, LinkedHashMap::new, Collectors.toList()));
        for (var entry : bySpec.entrySet()) {
            processWindowSpec(entry.getKey(), entry.getValue(), allRows, working, schema);
        }

        return wrapWorkingBuffers(allRows, schema, working).stream();
    }

    // Per-row Object[] working buffers, pre-filled with input column values at the
    // ordinals that {@code baseSchema} assigns. Slots for window results (sized into
    // {@code schema.size()} but absent from {@code baseSchema}) stay {@code null}.
    private Object[][] prefillWorkingBuffers(List<RowAccessor> allRows, RowSchema schema, RowSchema baseSchema) {
        Object[][] working = new Object[allRows.size()][];
        for (int i = 0; i < allRows.size(); i++) {
            Object[] vals = new Object[schema.size()];
            RowAccessor row = allRows.get(i);
            for (int o = 0; o < baseSchema.size(); o++) {
                FieldKey k = baseSchema.columnAt(o);
                SqlValue v = row.get(k);
                if (!(v instanceof SqlNull)) vals[o] = v;
            }
            working[i] = vals;
        }
        return working;
    }

    // Partitions by working[] indices (not Row instances) so we can write window
    // results back into the right working buffer slot.
    private void processWindowSpec(
            WindowSpec spec,
            List<Expression.WindowFnCall> calls,
            List<RowAccessor> allRows,
            Object[][] working,
            RowSchema schema) {
        List<List<Integer>> partitions = partitionIndices(allRows, spec);
        for (List<Integer> partition : partitions) {
            if (!spec.orderBy().isEmpty()) {
                sortIndices(partition, spec.orderBy(), allRows);
            }
            for (Expression.WindowFnCall wfc : calls) {
                // Slot presence is an invariant: every call in `calls` comes from
                // `distinctCalls`, which `withWindowSlots` indexed.
                int slot = schema.windowSlot(wfc).orElseThrow();
                evaluateWindowFunction(wfc, partition, allRows, working, slot);
            }
        }
    }

    private List<RowAccessor> wrapWorkingBuffers(List<RowAccessor> allRows, RowSchema schema, Object[][] working) {
        List<RowAccessor> result = new ArrayList<>(working.length);
        for (int i = 0; i < working.length; i++) {
            result.add(wrapBuffer(allRows.get(i), schema, working[i]));
        }
        return result;
    }

    // Preserve the input row's aggregated flag and source group: when WindowStage runs
    // after GROUP BY, the input rows are aggregated and the unflattener routes them
    // through the aggregated path (reading aggregate values as-is). Wrapping them as
    // plain FlatRow.of would lose that flag and force re-evaluation of AggregateFnCall
    // in ExpressionEvaluator (which is illegal per-row).
    private static FlatRow wrapBuffer(RowAccessor src, RowSchema schema, Object[] vals) {
        if (src.isAggregated()) {
            return FlatRow.aggregated(schema, vals, src.sourceGroup().orElseGet(Collections::emptyList));
        }
        return FlatRow.of(schema, vals);
    }

    /**
     * Collects the input-side schema for the materialization buffer. Three paths in descending order of cost-savings:
     *
     * <ol>
     *   <li><b>FlatRow input (post-GROUP BY / post-JOIN / post-engine pre-flatten):</b> Schema is uniform across the
     *       stream — return the first row's schema in O(1).
     *   <li><b>Non-{@code SELECT *} {@link Row} input:</b> use the parser-collected {@code referencedColumns} set,
     *       which already covers every {@link Expression.ColumnRef} seen in WHERE / GROUP BY / HAVING / ORDER BY /
     *       SELECT / WINDOW partition+order. O(K_ref). Avoids the per-row full-flatten that {@link Row#keys()} triggers
     *       on lazy rows.
     *   <li><b>{@code SELECT *} {@link Row} input:</b> referenced fields can't be statically enumerated, so we scan
     *       every row's keys. O(N×K) — only on the explicit asterisk path.
     * </ol>
     */
    private RowSchema collectBaseSchema(List<RowAccessor> allRows) {
        if (allRows.getFirst() instanceof FlatRow flat) {
            return flat.schema();
        }
        if (!selectsAsterisk) {
            var schemaBuilder = new RowSchema.Builder();
            referencedColumns.forEach(schemaBuilder::add);
            return schemaBuilder.build();
        }
        var schemaBuilder = new RowSchema.Builder();
        for (RowAccessor r : allRows) {
            if (r instanceof Row lazy) {
                lazy.keys().forEach(schemaBuilder::add);
            }
        }
        return schemaBuilder.build();
    }

    private List<List<Integer>> partitionIndices(List<RowAccessor> rows, WindowSpec spec) {
        if (spec.partitionBy().isEmpty()) {
            List<Integer> all = new ArrayList<>(rows.size());
            for (int i = 0; i < rows.size(); i++) all.add(i);
            return List.of(all);
        }
        Map<List<SqlValue>, List<Integer>> partitions = new LinkedHashMap<>();
        for (int i = 0; i < rows.size(); i++) {
            RowAccessor row = rows.get(i);
            List<SqlValue> key = spec.partitionBy().stream()
                    .map(expr -> ExpressionEvaluator.evaluate(expr, row, functionRegistry))
                    .toList();
            partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
        }
        return new ArrayList<>(partitions.values());
    }

    private void sortIndices(List<Integer> partition, List<OrderByColumnDef> orderBy, List<RowAccessor> rows) {
        Comparator<Integer> cmp = orderBy.stream()
                .map(col -> {
                    Comparator<Integer> c = Comparator.comparing(
                            (Integer idx) ->
                                    ExpressionEvaluator.evaluate(col.expression(), rows.get(idx), functionRegistry),
                            SqlValueComparator::compare);
                    return "DESC".equalsIgnoreCase(col.direction()) ? c.reversed() : c;
                })
                .reduce(Comparator::thenComparing)
                .orElse((a, b) -> 0);
        partition.sort(cmp);
    }

    private void evaluateWindowFunction(
            Expression.WindowFnCall wfc,
            List<Integer> partition,
            List<RowAccessor> rows,
            Object[][] working,
            int slot) {
        String fnName = wfc.name().toUpperCase();
        switch (fnName) {
            case "ROW_NUMBER" -> computeRowNumber(partition, working, slot);
            case "RANK" -> computeRank(wfc, partition, rows, working, slot);
            case "DENSE_RANK" -> computeDenseRank(wfc, partition, rows, working, slot);
            case "NTILE" -> computeNtile(wfc, partition, rows, working, slot);
            case "LAG" -> computeLag(wfc, partition, rows, working, slot);
            case "LEAD" -> computeLead(wfc, partition, rows, working, slot);
            case "SUM", "AVG", "COUNT", "MIN", "MAX" ->
                computeAggregateWindow(fnName, wfc, partition, rows, working, slot);
            default -> throw new SQL4JsonExecutionException("Unknown window function: " + fnName);
        }
    }

    private void computeRowNumber(List<Integer> partition, Object[][] working, int slot) {
        for (int i = 0; i < partition.size(); i++) {
            working[partition.get(i)][slot] = SqlNumber.of(i + 1L);
        }
    }

    private void computeRank(
            Expression.WindowFnCall wfc,
            List<Integer> partition,
            List<RowAccessor> rows,
            Object[][] working,
            int slot) {
        int prevRank = 0;
        for (int i = 0; i < partition.size(); i++) {
            int rank;
            if (i == 0) {
                rank = 1;
            } else if (sameOrderValues(partition.get(i), partition.get(i - 1), rows, wfc.spec())) {
                rank = prevRank;
            } else {
                rank = i + 1;
            }
            working[partition.get(i)][slot] = SqlNumber.of(rank);
            prevRank = rank;
        }
    }

    private void computeDenseRank(
            Expression.WindowFnCall wfc,
            List<Integer> partition,
            List<RowAccessor> rows,
            Object[][] working,
            int slot) {
        int currentRank = 0;
        for (int i = 0; i < partition.size(); i++) {
            if (i == 0 || !sameOrderValues(partition.get(i), partition.get(i - 1), rows, wfc.spec())) {
                currentRank++;
            }
            working[partition.get(i)][slot] = SqlNumber.of(currentRank);
        }
    }

    private void computeNtile(
            Expression.WindowFnCall wfc,
            List<Integer> partition,
            List<RowAccessor> rows,
            Object[][] working,
            int slot) {
        if (wfc.args().isEmpty()) {
            throw new SQL4JsonExecutionException("NTILE requires a bucket count argument");
        }
        SqlValue arg =
                ExpressionEvaluator.evaluate(wfc.args().getFirst(), rows.get(partition.getFirst()), functionRegistry);
        int buckets = (int) ((SqlNumber) arg).longValue();

        for (int i = 0; i < partition.size(); i++) {
            working[partition.get(i)][slot] = SqlNumber.of(ntileOf(i, partition.size(), buckets));
        }
    }

    static int ntileOf(int index, int partitionSize, int buckets) {
        int rowsPerBucket = partitionSize / buckets;
        int remainder = partitionSize % buckets;
        if (index < remainder * (rowsPerBucket + 1)) {
            return index / (rowsPerBucket + 1) + 1;
        }
        return (index - remainder * (rowsPerBucket + 1)) / rowsPerBucket + remainder + 1;
    }

    private void computeLag(
            Expression.WindowFnCall wfc,
            List<Integer> partition,
            List<RowAccessor> rows,
            Object[][] working,
            int slot) {
        int offset = resolveOffset(wfc);
        Expression argExpr = wfc.args().getFirst();
        for (int i = 0; i < partition.size(); i++) {
            SqlValue value = i - offset >= 0
                    ? ExpressionEvaluator.evaluate(argExpr, rows.get(partition.get(i - offset)), functionRegistry)
                    : SqlNull.INSTANCE;
            working[partition.get(i)][slot] = value instanceof SqlNull ? null : value;
        }
    }

    private void computeLead(
            Expression.WindowFnCall wfc,
            List<Integer> partition,
            List<RowAccessor> rows,
            Object[][] working,
            int slot) {
        int offset = resolveOffset(wfc);
        Expression argExpr = wfc.args().getFirst();
        for (int i = 0; i < partition.size(); i++) {
            SqlValue value = i + offset < partition.size()
                    ? ExpressionEvaluator.evaluate(argExpr, rows.get(partition.get(i + offset)), functionRegistry)
                    : SqlNull.INSTANCE;
            working[partition.get(i)][slot] = value instanceof SqlNull ? null : value;
        }
    }

    private int resolveOffset(Expression.WindowFnCall wfc) {
        if (wfc.args().size() > 1) {
            SqlValue offsetVal = ExpressionEvaluator.evaluate(wfc.args().get(1), Row.eager(Map.of()), functionRegistry);
            return (int) ((SqlNumber) offsetVal).longValue();
        }
        return 1;
    }

    private void computeAggregateWindow(
            String aggName,
            Expression.WindowFnCall wfc,
            List<Integer> partition,
            List<RowAccessor> rows,
            Object[][] working,
            int slot) {
        List<SqlValue> values;
        if (wfc.args().isEmpty()) {
            values = partition.stream().<SqlValue>map(idx -> SqlNull.INSTANCE).toList();
        } else {
            Expression argExpr = wfc.args().getFirst();
            values = partition.stream()
                    .map(idx -> ExpressionEvaluator.evaluate(argExpr, rows.get(idx), functionRegistry))
                    .toList();
        }
        SqlValue aggregated = functionRegistry
                .getAggregate(aggName)
                .orElseThrow(() -> new SQL4JsonExecutionException("Unknown aggregate: " + aggName))
                .apply()
                .apply(values);
        for (Integer idx : partition) {
            working[idx][slot] = aggregated instanceof SqlNull ? null : aggregated;
        }
    }

    private boolean sameOrderValues(int idxA, int idxB, List<RowAccessor> rows, WindowSpec spec) {
        RowAccessor a = rows.get(idxA);
        RowAccessor b = rows.get(idxB);
        for (OrderByColumnDef col : spec.orderBy()) {
            SqlValue va = ExpressionEvaluator.evaluate(col.expression(), a, functionRegistry);
            SqlValue vb = ExpressionEvaluator.evaluate(col.expression(), b, functionRegistry);
            if (SqlValueComparator.compare(va, vb) != 0) return false;
        }
        return true;
    }
}
