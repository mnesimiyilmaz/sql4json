package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.engine.MaterializingPipelineStage;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.engine.StreamMaterializer;
import io.github.mnesimiyilmaz.sql4json.parser.OrderByColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.sorting.SqlValueComparator;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Pipeline stage that sorts rows according to ORDER BY column definitions.
 * This is a materializing stage — all rows are collected before sorting.
 */
public final class OrderByStage implements MaterializingPipelineStage {

    private final Comparator<RowAccessor> comparator;
    private final int                     maxRows;

    /**
     * Creates a new OrderByStage from the given column definitions.
     *
     * @param columns          the ORDER BY column definitions specifying sort expressions and directions
     * @param functionRegistry the function registry for evaluating expressions
     * @param maxRows          the maximum number of rows allowed before throwing
     */
    public OrderByStage(List<OrderByColumnDef> columns, FunctionRegistry functionRegistry, int maxRows) {
        this.comparator = columns.stream()
                .map(col -> columnComparator(col, functionRegistry))
                .reduce(Comparator::thenComparing)
                .orElse((a, b) -> 0);
        this.maxRows = maxRows;
    }

    @Override
    public Stream<RowAccessor> apply(Stream<RowAccessor> input) {
        List<RowAccessor> materialized = StreamMaterializer.toList(input, maxRows, "ORDER BY");
        materialized.sort(comparator);
        return materialized.stream();
    }

    static Comparator<RowAccessor> columnComparator(OrderByColumnDef col,
                                                    FunctionRegistry functionRegistry) {
        var expr = col.expression();
        Comparator<RowAccessor> cmp = Comparator.comparing(
                (RowAccessor row) -> expr.containsAggregate()
                        ? row.sourceGroup()
                          .map(group -> ExpressionEvaluator.evaluateAggregate(expr, group, functionRegistry))
                          .orElseGet(() -> ExpressionEvaluator.evaluate(expr, row, functionRegistry))
                        : ExpressionEvaluator.evaluate(expr, row, functionRegistry),
                SqlValueComparator::compare);
        return "DESC".equalsIgnoreCase(col.direction()) ? cmp.reversed() : cmp;
    }
}
