package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.MaterializingPipelineStage;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.parser.OrderByColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;

import java.util.*;
import java.util.stream.Stream;

/**
 * Top-N fast path for {@code ORDER BY ... LIMIT k [OFFSET m]}.
 *
 * <p>Replaces the full-sort {@link OrderByStage} + {@link LimitStage} combo with a
 * bounded max-heap of capacity {@code k + m}, turning O(n log n) / O(n) space into
 * O(n log (k+m)) / O(k+m) space. The streaming input is scanned once; only the
 * smallest (k+m) rows under the comparator are retained, then the heap is drained
 * into a sorted list and the offset/limit slice is emitted.
 *
 * <p>Row-count enforcement counts <em>input</em> rows, not retained rows, so that
 * {@code maxRowsPerQuery} still bounds adversarial inputs.
 */
public final class TopNOrderByStage implements MaterializingPipelineStage {

    private final Comparator<Row> comparator;
    private final int             offset;
    private final int             limit;
    private final int             maxRows;

    /**
     * Creates a TopNOrderByStage.
     *
     * @param columns          ORDER BY column definitions
     * @param offset           the OFFSET value
     * @param limit            the LIMIT value
     * @param functionRegistry function registry for expression evaluation
     * @param maxRows          maximum input rows before throwing
     */
    public TopNOrderByStage(List<OrderByColumnDef> columns,
                            int offset,
                            int limit,
                            FunctionRegistry functionRegistry,
                            int maxRows) {
        this.comparator = columns.stream()
                .map(col -> OrderByStage.columnComparator(col, functionRegistry))
                .reduce(Comparator::thenComparing)
                .orElse((a, b) -> 0);
        this.offset = offset;
        this.limit = limit;
        this.maxRows = maxRows;
    }

    @Override
    public Stream<Row> apply(Stream<Row> input) {
        if (limit <= 0) {
            input.forEach(r -> { /* drain */ });
            return Stream.empty();
        }

        // Heap capacity = offset + limit, capped at maxRows to avoid oversizing.
        long capacityLong = (long) offset + (long) limit;
        int capacity = (int) Math.min(capacityLong, maxRows);

        // Max-heap under the ORDER BY comparator: largest-so-far at the head,
        // so we can evict it when a smaller candidate arrives.
        PriorityQueue<Row> heap = new PriorityQueue<>(
                Math.min(capacity, 1024), comparator.reversed());

        long seen = 0;
        Iterator<Row> it = input.iterator();
        while (it.hasNext()) {
            if (seen >= maxRows) {
                throw new SQL4JsonExecutionException(
                        "ORDER BY row count exceeds configured maximum (" + maxRows + ")");
            }
            Row row = it.next();
            seen++;
            if (heap.size() < capacity) {
                heap.offer(row);
            } else if (comparator.compare(row, heap.peek()) < 0) {
                heap.poll();
                heap.offer(row);
            }
        }

        // Drain heap in ascending comparator order (poll returns largest first
        // because of the reversed comparator, so reverse at the end).
        List<Row> sorted = new ArrayList<>(heap.size());
        while (!heap.isEmpty()) {
            sorted.add(heap.poll());
        }
        Collections.reverse(sorted);

        int from = Math.min(offset, sorted.size());
        int to = (int) Math.min((long) from + (long) limit, sorted.size());
        return sorted.subList(from, to).stream();
    }

}
