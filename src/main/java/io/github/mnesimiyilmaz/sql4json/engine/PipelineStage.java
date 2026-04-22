package io.github.mnesimiyilmaz.sql4json.engine;

import java.util.stream.Stream;

/**
 * A stage in the query execution pipeline.
 * Lazy stages do not buffer rows; materializing stages must collect the full stream.
 * <p>
 * Sealed: only LazyPipelineStage and MaterializingPipelineStage are permitted.
 * Concrete stage classes (WhereStage, etc.) implement the non-sealed sub-interfaces.
 */
public sealed interface PipelineStage permits LazyPipelineStage, MaterializingPipelineStage {
    /**
     * Applies this pipeline stage to the input row stream.
     *
     * @param input the incoming row stream
     * @return the transformed row stream
     */
    Stream<Row> apply(Stream<Row> input);

    /**
     * Returns {@code true} if this stage processes rows lazily without buffering.
     *
     * @return {@code true} if lazy, {@code false} if materializing
     */
    boolean isLazy();
}
