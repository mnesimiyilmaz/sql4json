package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.LazyPipelineStage;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.registry.CriteriaNode;

import java.util.stream.Stream;

/**
 * Lazy pipeline stage that filters rows by a WHERE condition.
 */
public final class WhereStage implements LazyPipelineStage {

    private final CriteriaNode criteria;

    /**
     * Creates a WhereStage with the specified condition.
     *
     * @param criteria the WHERE condition to apply
     */
    public WhereStage(CriteriaNode criteria) {
        this.criteria = criteria;
    }

    @Override
    public Stream<Row> apply(Stream<Row> input) {
        return input.filter(criteria::test);
    }
}
