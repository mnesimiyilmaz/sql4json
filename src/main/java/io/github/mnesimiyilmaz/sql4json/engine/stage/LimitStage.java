package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.LazyPipelineStage;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;

import java.util.stream.Stream;

/**
 * Pipeline stage that applies SQL LIMIT and OFFSET to the row stream.
 * This is a lazy stage — rows are skipped and limited without materializing the full stream.
 */
public final class LimitStage implements LazyPipelineStage {

    private final int limit;
    private final int offset;

    /**
     * Creates a new LimitStage with the given limit and offset.
     *
     * @param limit  the maximum number of rows to emit
     * @param offset the number of rows to skip before emitting
     */
    public LimitStage(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public Stream<RowAccessor> apply(Stream<RowAccessor> input) {
        Stream<RowAccessor> result = input;
        if (offset > 0) {
            result = result.skip(offset);
        }
        return result.limit(limit);
    }
}
