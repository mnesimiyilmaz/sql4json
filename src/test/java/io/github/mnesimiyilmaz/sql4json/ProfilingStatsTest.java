package io.github.mnesimiyilmaz.sql4json;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProfilingStatsTest {

    @Test
    void median_singleValue() {
        assertEquals(42L, ProfilingStats.median(new long[]{42}));
    }

    @Test
    void median_oddCount_returnsMiddle() {
        assertEquals(5L, ProfilingStats.median(new long[]{1, 5, 9}));
    }

    @Test
    void median_evenCount_returnsMeanOfTwoMiddles() {
        // {2, 4, 6, 8} sorted → middles are 4 and 6 → mean = 5
        assertEquals(5L, ProfilingStats.median(new long[]{2, 4, 6, 8}));
    }

    @Test
    void median_unsortedInput_isSortedFirst() {
        assertEquals(7L, ProfilingStats.median(new long[]{12, 3, 7, 1, 9}));
    }

    @Test
    void median_doesNotMutateInput() {
        long[] in = {3, 1, 2};
        ProfilingStats.median(in);
        assertEquals(3L, in[0]);
        assertEquals(1L, in[1]);
        assertEquals(2L, in[2]);
    }

    @Test
    void median_emptyArray_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ProfilingStats.median(new long[0]));
    }
}
