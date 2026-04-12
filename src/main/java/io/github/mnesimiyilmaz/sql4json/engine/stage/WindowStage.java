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
 * Materializing pipeline stage that evaluates window functions.
 * Position in pipeline: after HAVING, before ORDER BY.
 * Materializes all rows, partitions them, sorts within partitions,
 * evaluates window functions, and re-streams in original order.
 */
public final class WindowStage implements MaterializingPipelineStage {

    private final List<WindowColumn> windowColumns;
    private final FunctionRegistry   functionRegistry;
    private final int                maxRows;

    /**
     * A window function expression paired with its output field key.
     */
    record WindowColumn(Expression.WindowFnCall expression, FieldKey resultKey) {
    }

    /**
     * Creates a WindowStage for the given SELECT columns.
     *
     * @param selectedColumns  SELECT column definitions (only window columns are processed)
     * @param functionRegistry function registry for expression evaluation
     * @param maxRows          maximum rows to materialize before throwing
     */
    public WindowStage(List<SelectColumnDef> selectedColumns, FunctionRegistry functionRegistry, int maxRows) {
        this.windowColumns = selectedColumns.stream()
                .filter(SelectColumnDef::containsWindow)
                .map(c -> new WindowColumn(
                        (Expression.WindowFnCall) c.expression(),
                        FieldKey.of(c.aliasOrName())))
                .toList();
        this.functionRegistry = functionRegistry;
        this.maxRows = maxRows;
    }

    @Override
    public Stream<Row> apply(Stream<Row> input) {
        List<Row> allRows = StreamMaterializer.toList(input, maxRows, "WINDOW");
        if (allRows.isEmpty()) return Stream.empty();

        // Group window columns by WindowSpec — same OVER clause = same pass
        Map<WindowSpec, List<WindowColumn>> bySpec = windowColumns.stream()
                .collect(Collectors.groupingBy(wc -> wc.expression().spec()));

        for (var entry : bySpec.entrySet()) {
            WindowSpec spec = entry.getKey();
            List<WindowColumn> columns = entry.getValue();

            Map<List<SqlValue>, List<Row>> partitions = partitionRows(allRows, spec);

            for (List<Row> partition : partitions.values()) {
                if (!spec.orderBy().isEmpty()) {
                    sortPartition(partition, spec.orderBy());
                }
                for (WindowColumn wc : columns) {
                    evaluateWindowFunction(wc, partition);
                }
            }
        }

        return allRows.stream();
    }

    private Map<List<SqlValue>, List<Row>> partitionRows(List<Row> rows, WindowSpec spec) {
        if (spec.partitionBy().isEmpty()) {
            return Map.of(List.of(), new ArrayList<>(rows));
        }
        Map<List<SqlValue>, List<Row>> partitions = new LinkedHashMap<>();
        for (Row row : rows) {
            List<SqlValue> key = spec.partitionBy().stream()
                    .map(expr -> ExpressionEvaluator.evaluate(expr, row, functionRegistry))
                    .toList();
            partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
        return partitions;
    }

    private void sortPartition(List<Row> partition, List<OrderByColumnDef> orderBy) {
        Comparator<Row> cmp = orderBy.stream()
                .map(col -> {
                    Comparator<Row> c = Comparator.comparing(
                            (Row r) -> ExpressionEvaluator.evaluate(col.expression(), r, functionRegistry),
                            SqlValueComparator::compare);
                    return "DESC".equalsIgnoreCase(col.direction()) ? c.reversed() : c;
                })
                .reduce(Comparator::thenComparing)
                .orElse((a, b) -> 0);
        partition.sort(cmp);
    }

    private void evaluateWindowFunction(WindowColumn wc, List<Row> partition) {
        String fnName = wc.expression().name().toUpperCase();
        switch (fnName) {
            case "ROW_NUMBER" -> computeRowNumber(wc, partition);
            case "RANK" -> computeRank(wc, partition);
            case "DENSE_RANK" -> computeDenseRank(wc, partition);
            case "NTILE" -> computeNtile(wc, partition);
            case "LAG" -> computeLag(wc, partition);
            case "LEAD" -> computeLead(wc, partition);
            case "SUM", "AVG", "COUNT", "MIN", "MAX" -> computeAggregateWindow(fnName, wc, partition);
            default -> throw new SQL4JsonExecutionException("Unknown window function: " + fnName);
        }
    }

    private void computeRowNumber(WindowColumn wc, List<Row> partition) {
        for (int i = 0; i < partition.size(); i++) {
            partition.get(i).putWindowResult(wc.resultKey(), SqlNumber.of(i + 1));
        }
    }

    private void computeRank(WindowColumn wc, List<Row> partition) {
        for (int i = 0; i < partition.size(); i++) {
            int rank;
            if (i == 0) {
                rank = 1;
            } else if (sameOrderValues(partition.get(i), partition.get(i - 1), wc.expression().spec())) {
                rank = ((SqlNumber) partition.get(i - 1).get(wc.resultKey())).value().intValue();
            } else {
                rank = i + 1;
            }
            partition.get(i).putWindowResult(wc.resultKey(), SqlNumber.of(rank));
        }
    }

    private void computeDenseRank(WindowColumn wc, List<Row> partition) {
        int currentRank = 0;
        for (int i = 0; i < partition.size(); i++) {
            if (i == 0 || !sameOrderValues(partition.get(i), partition.get(i - 1), wc.expression().spec())) {
                currentRank++;
            }
            partition.get(i).putWindowResult(wc.resultKey(), SqlNumber.of(currentRank));
        }
    }

    private void computeNtile(WindowColumn wc, List<Row> partition) {
        if (wc.expression().args().isEmpty()) {
            throw new SQL4JsonExecutionException("NTILE requires a bucket count argument");
        }
        SqlValue arg = ExpressionEvaluator.evaluate(
                wc.expression().args().getFirst(), partition.getFirst(), functionRegistry);
        int buckets = ((SqlNumber) arg).value().intValue();

        for (int i = 0; i < partition.size(); i++) {
            partition.get(i).putWindowResult(wc.resultKey(), SqlNumber.of(ntileOf(i, partition.size(), buckets)));
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

    private void computeLag(WindowColumn wc, List<Row> partition) {
        int offset = resolveOffset(wc);
        Expression argExpr = wc.expression().args().getFirst();
        for (int i = 0; i < partition.size(); i++) {
            SqlValue value = i - offset >= 0
                    ? ExpressionEvaluator.evaluate(argExpr, partition.get(i - offset), functionRegistry)
                    : SqlNull.INSTANCE;
            partition.get(i).putWindowResult(wc.resultKey(), value);
        }
    }

    private void computeLead(WindowColumn wc, List<Row> partition) {
        int offset = resolveOffset(wc);
        Expression argExpr = wc.expression().args().getFirst();
        for (int i = 0; i < partition.size(); i++) {
            SqlValue value = i + offset < partition.size()
                    ? ExpressionEvaluator.evaluate(argExpr, partition.get(i + offset), functionRegistry)
                    : SqlNull.INSTANCE;
            partition.get(i).putWindowResult(wc.resultKey(), value);
        }
    }

    private int resolveOffset(WindowColumn wc) {
        if (wc.expression().args().size() > 1) {
            SqlValue offsetVal = ExpressionEvaluator.evaluate(
                    wc.expression().args().get(1), Row.eager(Map.of()), functionRegistry);
            return ((SqlNumber) offsetVal).value().intValue();
        }
        return 1;
    }

    private void computeAggregateWindow(String aggName, WindowColumn wc, List<Row> partition) {
        List<SqlValue> values;
        if (wc.expression().args().isEmpty()) {
            values = partition.stream().<SqlValue>map(r -> SqlNull.INSTANCE).toList();
        } else {
            Expression argExpr = wc.expression().args().getFirst();
            values = partition.stream()
                    .map(r -> ExpressionEvaluator.evaluate(argExpr, r, functionRegistry))
                    .toList();
        }
        SqlValue aggregated = functionRegistry.getAggregate(aggName)
                .orElseThrow(() -> new SQL4JsonExecutionException("Unknown aggregate: " + aggName))
                .apply().apply(values);
        for (Row row : partition) {
            row.putWindowResult(wc.resultKey(), aggregated);
        }
    }

    private boolean sameOrderValues(Row a, Row b, WindowSpec spec) {
        for (OrderByColumnDef col : spec.orderBy()) {
            SqlValue va = ExpressionEvaluator.evaluate(col.expression(), a, functionRegistry);
            SqlValue vb = ExpressionEvaluator.evaluate(col.expression(), b, functionRegistry);
            if (SqlValueComparator.compare(va, vb) != 0) return false;
        }
        return true;
    }
}
