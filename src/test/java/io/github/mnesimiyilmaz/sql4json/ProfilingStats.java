package io.github.mnesimiyilmaz.sql4json;

import java.util.Arrays;

/**
 * Pure statistical helpers used by {@link ProfilingTest} when collapsing
 * multi-run measurements into a single number for the report.
 *
 * <p>Package-private — internal to the profiling test suite, not part of
 * the public API.
 */
final class ProfilingStats {

    private ProfilingStats() {
        // no instances — static helpers only
    }

    /**
     * Returns the median of {@code values}. For odd-length input, returns the
     * middle element after sorting. For even-length input, returns the integer
     * mean of the two middle elements (truncating toward zero per Java's
     * {@code /} on longs).
     *
     * <p>Does not mutate the input — sorts a defensive copy.
     *
     * @param values measurements (e.g. wall-time in ms across N runs)
     * @return median value
     * @throws IllegalArgumentException if {@code values.length == 0}
     */
    static long median(long[] values) {
        if (values.length == 0) {
            throw new IllegalArgumentException("median requires at least one value");
        }
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        int n = sorted.length;
        if (n % 2 == 1) {
            return sorted[n / 2];
        }
        return (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
    }
}
