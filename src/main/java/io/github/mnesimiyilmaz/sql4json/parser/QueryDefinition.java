package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.registry.CriteriaNode;

import java.util.List;
import java.util.Set;

/**
 * Immutable parsed query representation produced by QueryParser.
 * Consumed by QueryPipeline (Phase 5).
 *
 * @param selectedColumns          SELECT columns (may include aggregate and alias)
 * @param rootPath                 FROM path, e.g. "$r", "$r.data.items"
 * @param fromSubQuery             raw SQL of inner FROM (SELECT ...) subquery, or null
 * @param whereClause              v2 CriteriaNode (registry package), or null
 * @param groupBy                  GROUP BY expressions, or null
 * @param havingClause             v2 CriteriaNode for HAVING, or null
 * @param orderBy                  ORDER BY column defs, or null
 * @param referencedFields         all field paths referenced anywhere in the query
 * @param distinct                 true iff SELECT DISTINCT was specified
 * @param limit                    LIMIT value, or null if not specified
 * @param offset                   OFFSET value, or null if not specified
 * @param containsNonDeterministic true iff the query references non-deterministic functions (e.g. NOW())
 * @param rootAlias                alias for the root FROM source, or null for plain $r queries
 * @param joins                    JOIN definitions, or null when no JOINs are present
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
        List<JoinDef> joins
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
     * True iff GROUP BY is present — triggers full flatten in Phase 5.
     *
     * @return {@code true} if a GROUP BY clause is present
     */
    public boolean requiresFullFlatten() {
        return groupBy != null;
    }

    /**
     * True iff any SELECT column contains a window function — triggers WindowStage in the pipeline.
     *
     * @return {@code true} if any selected column uses a window function
     */
    public boolean containsWindowFunctions() {
        return selectedColumns.stream().anyMatch(SelectColumnDef::containsWindow);
    }
}
