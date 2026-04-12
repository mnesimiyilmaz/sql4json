package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Materializes a {@link Stream} into a {@link List} with an explicit max-rows guard.
 *
 * <p>Throws {@link SQL4JsonExecutionException} with a stage-name-prefixed message
 * when the stream would produce more than {@code maxRows} items. Designed for
 * pipeline stages that unavoidably buffer their entire input before producing output.</p>
 *
 * <p>This class implements the
 * {@code "<STAGE> row count exceeds configured maximum (<N>)"} error-message contract
 * defined by {@link io.github.mnesimiyilmaz.sql4json.settings.LimitsSettings#maxRowsPerQuery()}.
 * The three current users that delegate to this helper are
 * {@code GroupByStage}, {@code OrderByStage}, and {@code WindowStage}.
 * Other enforcement points ({@code DistinctStage}, the {@code QueryPipeline} terminal,
 * and {@code StreamingSerializer}) implement equivalent inline guards using the same
 * message format rather than calling this helper.</p>
 */
public final class StreamMaterializer {

    private StreamMaterializer() {
    }

    /**
     * Drains {@code stream} into a newly allocated list, throwing before the list
     * would exceed {@code maxRows} entries.
     *
     * @param stream    the stream to drain (must not be null)
     * @param maxRows   the inclusive upper bound on collected rows (must be positive)
     * @param stageName prefix used in the overflow exception message (e.g. {@code "GROUP BY"})
     * @param <T>       element type of the stream
     * @return the materialized list of elements
     * @throws SQL4JsonExecutionException if the stream would produce more than {@code maxRows} items
     */
    public static <T> List<T> toList(Stream<T> stream, int maxRows, String stageName) {
        List<T> result = new ArrayList<>();
        Iterator<T> it = stream.iterator();
        while (it.hasNext()) {
            if (result.size() >= maxRows) {
                throw new SQL4JsonExecutionException(
                        stageName + " row count exceeds configured maximum (" + maxRows + ")");
            }
            result.add(it.next());
        }
        return result;
    }
}
