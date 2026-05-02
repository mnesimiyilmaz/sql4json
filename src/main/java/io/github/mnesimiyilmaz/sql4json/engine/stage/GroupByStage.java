// SPDX-License-Identifier: Apache-2.0
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
 * Materializing pipeline stage that groups rows by expression values (SQL GROUP BY) and produces one aggregated row per
 * group via {@link GroupAggregator}.
 *
 * <p>Inputs may be lazy {@link Row} (from streaming flatten + WHERE) or {@link FlatRow} (from JOIN). Lazy rows are
 * fully flattened on entry so {@code SELECT *} reconstruction has every key available; flat rows already expose every
 * column via their schema.
 */
public final class GroupByStage implements MaterializingPipelineStage {

    private final List<Expression> groupByExpressions;
    private final List<SelectColumnDef> selectedColumns;
    private final FunctionRegistry functionRegistry;
    private final int maxRows;

    /**
     * Creates a new GroupByStage with the specified grouping configuration.
     *
     * @param groupByExpressions expressions to group by
     * @param selectedColumns SELECT column definitions (used for aggregation)
     * @param functionRegistry function registry for expression evaluation
     * @param maxRows maximum number of rows to materialize before throwing
     */
    public GroupByStage(
            List<Expression> groupByExpressions,
            List<SelectColumnDef> selectedColumns,
            FunctionRegistry functionRegistry,
            int maxRows) {
        this.groupByExpressions = groupByExpressions;
        this.selectedColumns = selectedColumns;
        this.functionRegistry = functionRegistry;
        this.maxRows = maxRows;
    }

    @Override
    public Stream<RowAccessor> apply(Stream<RowAccessor> input) {
        List<RowAccessor> materialized =
                StreamMaterializer.toList(input.map(GroupByStage::ensureFlattenedIfLazy), maxRows, "GROUP BY");
        Map<GroupKey, List<RowAccessor>> groups =
                materialized.stream().collect(Collectors.groupingBy(this::extractGroupKey));
        return groups.values().stream().map(rows -> GroupAggregator.aggregate(rows, selectedColumns, functionRegistry));
    }

    /**
     * Force lazy rows fully flattened so SELECT * has every key available. FlatRow inputs are already fully populated.
     */
    private static RowAccessor ensureFlattenedIfLazy(RowAccessor row) {
        return row instanceof Row r ? r.ensureFullyFlattened() : row;
    }

    private GroupKey extractGroupKey(RowAccessor row) {
        var keyValues = new ArrayList<SqlValue>(groupByExpressions.size());
        for (Expression expr : groupByExpressions) {
            keyValues.add(ExpressionEvaluator.evaluate(expr, row, functionRegistry));
        }
        return new GroupKey(keyValues);
    }
}
