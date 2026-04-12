package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.engine.stage.*;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Assembles and executes a pipeline of stages derived from a QueryDefinition.
 * Stage order: WHERE → GROUP BY → HAVING → WINDOW → ORDER BY → LIMIT → SELECT → DISTINCT.
 */
final class QueryPipeline {

    private final List<PipelineStage> stages;
    private final int                 maxRows;

    private QueryPipeline(List<PipelineStage> stages, int maxRows) {
        this.stages = stages;
        this.maxRows = maxRows;
    }

    static QueryPipeline build(QueryDefinition query, FunctionRegistry functionRegistry,
                               Sql4jsonSettings settings) {
        var stages = new ArrayList<PipelineStage>();
        int maxRows = settings.limits().maxRowsPerQuery();
        if (query.whereClause() != null) {
            stages.add(new WhereStage(query.whereClause()));
        }
        if (query.groupBy() != null) {
            stages.add(new GroupByStage(query.groupBy(), query.selectedColumns(), functionRegistry, maxRows));
        }
        if (query.havingClause() != null) {
            stages.add(new HavingStage(query.havingClause()));
        }
        // Window functions: after HAVING, before ORDER BY (per SQL standard)
        if (query.containsWindowFunctions()) {
            stages.add(new WindowStage(query.selectedColumns(), functionRegistry, maxRows));
        }
        // ORDER BY + LIMIT fast path: fold into a bounded max-heap (top-N) so we
        // don't sort the entire input just to take k rows. Plain ORDER BY (no LIMIT)
        // and plain LIMIT (no ORDER BY) keep their original standalone stages.
        if (query.orderBy() != null && query.limit() != null) {
            stages.add(new TopNOrderByStage(
                    query.orderBy(), resolveOffset(query), query.limit(), functionRegistry, maxRows));
        } else {
            if (query.orderBy() != null) {
                stages.add(new OrderByStage(query.orderBy(), functionRegistry, maxRows));
            }
            if (query.limit() != null) {
                stages.add(new LimitStage(query.limit(), resolveOffset(query)));
            }
        }
        stages.add(new SelectStage(query.selectedColumns()));
        if (query.distinct()) {
            stages.add(new DistinctStage(maxRows));
        }
        return new QueryPipeline(stages, maxRows);
    }

    private static int resolveOffset(QueryDefinition query) {
        return query.offset() != null ? query.offset() : 0;
    }

    List<Row> execute(Stream<Row> input) {
        Stream<Row> current = input;
        for (PipelineStage stage : stages) {
            current = stage.apply(current);
        }
        return StreamMaterializer.toList(current, maxRows, "PIPELINE");
    }

    Stream<Row> executeAsStream(Stream<Row> input) {
        Stream<Row> current = input;
        for (PipelineStage stage : stages) {
            current = stage.apply(current);
        }
        return current;
    }
}
