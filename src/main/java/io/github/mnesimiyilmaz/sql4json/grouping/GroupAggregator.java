package io.github.mnesimiyilmaz.sql4json.grouping;

import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.FlatRow;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.engine.RowSchema;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.List;

/**
 * Applies SELECT column definitions to a group of rows, returning one aggregated {@link FlatRow}.
 *
 * <p>Key convention (must match JsonUnflattener.reconstructFromAggregatedRow):
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
     * @param group            the rows in one GROUP BY group (lazy or flat)
     * @param selectedColumns  SELECT column definitions (aggregates and group-by keys)
     * @param functionRegistry function registry for expression evaluation
     * @return a single aggregated {@link FlatRow}
     */
    public static FlatRow aggregate(List<? extends RowAccessor> group,
                                    List<SelectColumnDef> selectedColumns,
                                    FunctionRegistry functionRegistry) {
        RowSchema schema = buildSchema(selectedColumns, group);
        Object[] values = new Object[schema.size()];
        populateValues(schema, values, selectedColumns, group, functionRegistry);
        return FlatRow.aggregated(schema, values, group);
    }

    // Schema: one column per non-window SELECT entry; SELECT * expands to the
    // representative row's keys (same convention as the legacy Row.eager path).
    private static RowSchema buildSchema(List<SelectColumnDef> selectedColumns,
                                         List<? extends RowAccessor> group) {
        var schemaBuilder = new RowSchema.Builder();
        for (SelectColumnDef col : selectedColumns) {
            if (col.containsWindow()) continue;
            if (col.isAsterisk()) {
                group.getFirst().keys().forEach(schemaBuilder::add);
            } else {
                schemaBuilder.add(columnKey(col));
            }
        }
        return schemaBuilder.build();
    }

    private static void populateValues(RowSchema schema, Object[] values,
                                       List<SelectColumnDef> selectedColumns,
                                       List<? extends RowAccessor> group,
                                       FunctionRegistry functionRegistry) {
        for (SelectColumnDef col : selectedColumns) {
            if (col.containsWindow()) continue; // computed later by WindowStage
            if (col.isAsterisk()) {
                populateAsterisk(schema, values, group);
            } else if (col.containsAggregate()) {
                populateAggregate(schema, values, col, group, functionRegistry);
            } else {
                populateGroupKey(schema, values, col, group, functionRegistry);
            }
        }
    }

    // Aggregate columns key by alias; group-by-key columns key by columnName, falling
    // back to aliasOrName for computed expressions (e.g. CASE WHEN) where columnName is null.
    private static FieldKey columnKey(SelectColumnDef col) {
        if (col.containsAggregate()) {
            return FieldKey.of(col.aliasOrName());
        }
        String colPath = col.columnName();
        return FieldKey.of(colPath != null ? colPath : col.aliasOrName());
    }

    private static void populateAsterisk(RowSchema schema, Object[] values,
                                         List<? extends RowAccessor> group) {
        group.getFirst().entries().forEach(e -> {
            int ord = schema.indexOf(e.getKey());
            if (ord >= 0 && !(e.getValue() instanceof SqlNull)) {
                values[ord] = e.getValue();
            }
        });
    }

    private static void populateAggregate(RowSchema schema, Object[] values, SelectColumnDef col,
                                          List<? extends RowAccessor> group,
                                          FunctionRegistry functionRegistry) {
        SqlValue agg = ExpressionEvaluator.evaluateAggregate(
                col.expression(), group, functionRegistry);
        int ord = schema.indexOf(FieldKey.of(col.aliasOrName()));
        if (!(agg instanceof SqlNull)) values[ord] = agg;
    }

    private static void populateGroupKey(RowSchema schema, Object[] values, SelectColumnDef col,
                                         List<? extends RowAccessor> group,
                                         FunctionRegistry functionRegistry) {
        SqlValue v = ExpressionEvaluator.evaluate(
                col.expression(), group.getFirst(), functionRegistry);
        int ord = schema.indexOf(columnKey(col));
        if (!(v instanceof SqlNull)) values[ord] = v;
    }
}
