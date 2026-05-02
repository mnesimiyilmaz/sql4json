// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.LazyPipelineStage;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.registry.CriteriaNode;
import java.util.stream.Stream;

/** Lazy pipeline stage that filters rows by a WHERE condition. */
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
    public Stream<RowAccessor> apply(Stream<RowAccessor> input) {
        return input.filter(criteria::test);
    }
}
