package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.registry.CriteriaNode;

import java.util.List;
import java.util.Set;

/**
 * Immutable parsed query representation produced by QueryParser.
 * Consumed by QueryPipeline.
 *
 * @param selectedColumns          SELECT columns (may include aggregate and alias)
 * @param rootPath                 FROM path, e.g. "$r", "$r.data.items"
 * @param fromSubQuery             raw SQL of inner FROM (SELECT ...) subquery, or null
 * @param whereClause              CriteriaNode (registry package), or null
 * @param groupBy                  GROUP BY expressions, or null
 * @param havingClause             CriteriaNode for HAVING, or null
 * @param orderBy                  ORDER BY column defs, or null
 * @param referencedFields         all field paths referenced anywhere in the query
 * @param distinct                 true iff SELECT DISTINCT was specified
 * @param limit                    LIMIT value, or null if not specified
 * @param offset                   OFFSET value, or null if not specified
 * @param containsNonDeterministic true iff the query references non-deterministic functions (e.g. NOW())
 * @param rootAlias                alias for the root FROM source, or null for plain $r queries
 * @param joins                    JOIN definitions, or null when no JOINs are present
 * @param limitParam               non-null when LIMIT uses a placeholder; substitution resolves it
 * @param offsetParam              non-null when OFFSET uses a placeholder; substitution resolves it
 * @param positionalCount          total {@code ?} placeholders across this def and captured subqueries
 * @param namedParameters          all {@code :name} placeholders seen across this def and subqueries
 * @param subqueryPositionalOffset the global offset to pass when re-parsing the inner subquery
 *                                 (0 for top-level parse without subqueries)
 * @param windowFunctionCalls      every {@link Expression.WindowFnCall} built by the parser, in
 *                                 source order. Includes calls buried inside {@link CriteriaNode}
 *                                 closures (CASE WHEN conditions). Consumed by WindowStage to
 *                                 precompute results stored on each row.
 */
public record QueryDefinition(
        List<SelectColumnDef> selectedColumns,
        String rootPath,
        String fromSubQuery,
        CriteriaNode whereClause,
        List<Expression> groupBy,
        CriteriaNode havingClause,
        List<OrderByColumnDef> orderBy,
        Set<String> referencedFields,
        boolean distinct,
        Integer limit,
        Integer offset,
        boolean containsNonDeterministic,
        String rootAlias,
        List<JoinDef> joins,
        Expression.ParameterRef limitParam,
        Expression.ParameterRef offsetParam,
        int positionalCount,
        Set<String> namedParameters,
        int subqueryPositionalOffset,
        List<Expression.WindowFnCall> windowFunctionCalls
) {
    /**
     * True iff this is a bare SELECT * (no projection).
     *
     * @return {@code true} if the query selects all columns
     */
    public boolean isSelectAll() {
        return selectedColumns.size() == 1 && selectedColumns.getFirst().isAsterisk();
    }

    /**
     * True iff GROUP BY is present — triggers a full flatten before grouping.
     *
     * @return {@code true} if a GROUP BY clause is present
     */
    public boolean requiresFullFlatten() {
        return groupBy != null;
    }

    /**
     * True iff any window function appears anywhere in the query — triggers WindowStage in
     * the pipeline. Reflects the parser-collected list, which catches windows buried inside
     * CASE WHEN conditions (where the {@link CriteriaNode} closure is opaque to tree-walks).
     *
     * @return {@code true} if any window function call was parsed
     */
    public boolean containsWindowFunctions() {
        return !windowFunctionCalls.isEmpty();
    }

    /**
     * Returns the union of all field paths referenced anywhere in the query as
     * {@link io.github.mnesimiyilmaz.sql4json.engine.FieldKey}s. Used by
     * materialization stages to derive a
     * {@link io.github.mnesimiyilmaz.sql4json.engine.RowSchema} without an
     * extra pass over input rows.
     *
     * @return the referenced columns as field keys
     * @since 1.2.0
     */
    public java.util.Set<io.github.mnesimiyilmaz.sql4json.engine.FieldKey> referencedColumns() {
        return referencedFields.stream()
                .map(io.github.mnesimiyilmaz.sql4json.engine.FieldKey::of)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
}
