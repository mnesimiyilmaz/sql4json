package io.github.mnesimiyilmaz.sql4json.engine;

/**
 * A pipeline stage that does not buffer (filter, map).
 * Implementations: WhereStage, HavingStage, SelectStage.
 */
public non-sealed interface LazyPipelineStage extends PipelineStage {
    @Override
    default boolean isLazy() {
        return true;
    }
}
