package io.github.mnesimiyilmaz.sql4json.grouping;

import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.HashMap;
import java.util.List;

/**
 * Applies SELECT column definitions to a group of Rows, returning one aggregated Row.
 *
 * <p>Key convention (must match JsonUnflattener.reconstructFromFlatMap):
 * <ul>
 *   <li>Aggregate results are stored under {@code FieldKey.of(col.aliasOrName())} — so HAVING
 *       can reference them by alias.</li>
 *   <li>Non-aggregate results are stored under {@code FieldKey.of(col.columnName())} — consistent
 *       with the cherry-pick path in JsonUnflattener.</li>
 * </ul>
 */
public final class GroupAggregator {

    private GroupAggregator() {
    }

    /**
     * Aggregates a group of rows according to the given SELECT column definitions.
     *
     * @param group            the rows in one GROUP BY group
     * @param selectedColumns  SELECT column definitions (aggregates and group-by keys)
     * @param functionRegistry function registry for expression evaluation
     * @return a single aggregated row
     */
    public static Row aggregate(List<Row> group, List<SelectColumnDef> selectedColumns,
                                FunctionRegistry functionRegistry) {
        var result = new HashMap<FieldKey, SqlValue>();

        for (SelectColumnDef col : selectedColumns) {
            if (col.containsWindow()) {
                // Window functions are computed later by WindowStage — skip here
                continue;
            }
            if (col.isAsterisk()) {
                // SELECT * in GROUP BY context — dump all fields from representative row
                group.getFirst().entries().forEach(e -> result.put(e.getKey(), e.getValue()));
            } else if (col.containsAggregate()) {
                // Aggregate column: evaluate full expression tree via ExpressionEvaluator
                SqlValue aggregated = ExpressionEvaluator.evaluateAggregate(
                        col.expression(), group, functionRegistry);
                // Store under ALIAS key — so HAVING can find it by alias name
                result.put(FieldKey.of(col.aliasOrName()), aggregated);
            } else {
                // Non-aggregate column (GROUP BY key): evaluate against first row
                SqlValue val = ExpressionEvaluator.evaluate(
                        col.expression(), group.getFirst(), functionRegistry);
                // Store under COLUMN NAME key — consistent with JsonUnflattener cherry-pick path.
                // For computed expressions (e.g. CASE WHEN), columnName() is null; fall back to aliasOrName().
                String colPath = col.columnName();
                FieldKey colKey = FieldKey.of(colPath != null ? colPath : col.aliasOrName());
                result.put(colKey, val);
            }
        }

        return Row.eager(result, group);
    }
}
