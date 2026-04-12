package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.*;
import io.github.mnesimiyilmaz.sql4json.grouping.GroupAggregator;
import io.github.mnesimiyilmaz.sql4json.grouping.GroupKey;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Materializing pipeline stage that groups rows by expression values (SQL GROUP BY)
 * and produces one aggregated row per group via {@link GroupAggregator}.
 */
public final class GroupByStage implements MaterializingPipelineStage {

    private final List<Expression>      groupByExpressions;
    private final List<SelectColumnDef> selectedColumns;
    private final FunctionRegistry      functionRegistry;
    private final int                   maxRows;

    /**
     * Creates a new GroupByStage with the specified grouping configuration.
     *
     * @param groupByExpressions expressions to group by
     * @param selectedColumns    SELECT column definitions (used for aggregation)
     * @param functionRegistry   function registry for expression evaluation
     * @param maxRows            maximum number of rows to materialize before throwing
     */
    public GroupByStage(List<Expression> groupByExpressions, List<SelectColumnDef> selectedColumns,
                        FunctionRegistry functionRegistry, int maxRows) {
        this.groupByExpressions = groupByExpressions;
        this.selectedColumns = selectedColumns;
        this.functionRegistry = functionRegistry;
        this.maxRows = maxRows;
    }

    @Override
    public Stream<Row> apply(Stream<Row> input) {
        List<Row> materialized = StreamMaterializer.toList(
                input.map(Row::ensureFullyFlattened), maxRows, "GROUP BY");
        Map<GroupKey, List<Row>> groups = materialized.stream()
                .collect(Collectors.groupingBy(this::extractGroupKey));
        return groups.values().stream()
                .map(rows -> GroupAggregator.aggregate(rows, selectedColumns, functionRegistry));
    }

    private GroupKey extractGroupKey(Row row) {
        var keyValues = new ArrayList<SqlValue>(groupByExpressions.size());
        for (Expression expr : groupByExpressions) {
            keyValues.add(ExpressionEvaluator.evaluate(expr, row, functionRegistry));
        }
        return new GroupKey(keyValues);
    }
}
