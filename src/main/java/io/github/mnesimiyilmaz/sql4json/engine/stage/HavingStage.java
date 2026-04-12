package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.LazyPipelineStage;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.registry.CriteriaNode;

import java.util.stream.Stream;

/**
 * Lazy pipeline stage that filters grouped rows by a HAVING condition.
 */
public final class HavingStage implements LazyPipelineStage {

    private final CriteriaNode criteria;

    /**
     * Creates a new HavingStage with the specified condition.
     *
     * @param criteria the HAVING condition to apply
     */
    public HavingStage(CriteriaNode criteria) {
        this.criteria = criteria;
    }

    @Override
    public Stream<Row> apply(Stream<Row> input) {
        return input.filter(criteria::test);
    }
}
