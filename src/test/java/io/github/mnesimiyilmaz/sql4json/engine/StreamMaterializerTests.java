package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamMaterializerTests {

    @Test
    void drains_stream_below_limit() {
        List<String> out = StreamMaterializer.toList(Stream.of("a", "b", "c"), 10, "TEST");
        assertEquals(List.of("a", "b", "c"), out);
    }

    @Test
    void drains_stream_at_exact_limit() {
        List<String> out = StreamMaterializer.toList(Stream.of("a", "b", "c"), 3, "TEST");
        assertEquals(List.of("a", "b", "c"), out);
    }

    @Test
    void throws_with_stage_name_on_overflow() {
        var ex = assertThrows(SQL4JsonExecutionException.class,
                () -> StreamMaterializer.toList(Stream.of("a", "b", "c", "d"), 3, "ORDER BY"));
        assertEquals("ORDER BY row count exceeds configured maximum (3)", ex.getMessage());
    }
}
