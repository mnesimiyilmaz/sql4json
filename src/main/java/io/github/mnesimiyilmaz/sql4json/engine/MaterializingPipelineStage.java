// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

/**
 * A pipeline stage that must consume the entire stream (sort, group). Implementations: OrderByStage, GroupByStage,
 * WindowStage, DistinctStage.
 */
public non-sealed interface MaterializingPipelineStage extends PipelineStage {
    @Override
    default boolean isLazy() {
        return false;
    }
}
