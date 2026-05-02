// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BoundParametersTest {

    @Test
    void when_named_bind_then_value_present_by_name() {
        BoundParameters p = BoundParameters.named().bind("a", 1).bind("b", "x");
        assertTrue(p.isNamed());
        assertEquals(1, p.getByName("a"));
        assertEquals("x", p.getByName("b"));
    }

    @Test
    void when_positional_bind_then_value_present_by_index() {
        BoundParameters p = BoundParameters.positional().bind(0, 42).bind(1, "y");
        assertFalse(p.isNamed());
        assertEquals(42, p.getByIndex(0));
        assertEquals("y", p.getByIndex(1));
    }

    @Test
    void when_bind_returns_new_instance_original_unchanged() {
        BoundParameters p = BoundParameters.named();
        BoundParameters p2 = p.bind("a", 1);
        assertNotSame(p, p2);
        assertThrows(SQL4JsonBindException.class, () -> p.getByName("a"));
    }

    @Test
    void when_mixing_named_with_positional_bind_then_exception() {
        BoundParameters p = BoundParameters.named().bind("a", 1);
        assertThrows(SQL4JsonBindException.class, () -> p.bind(0, 42));
    }

    @Test
    void when_mixing_positional_with_named_bind_then_exception() {
        BoundParameters p = BoundParameters.positional().bind(0, 1);
        assertThrows(SQL4JsonBindException.class, () -> p.bind("a", 2));
    }

    @Test
    void when_of_varargs_then_positional_mode() {
        BoundParameters p = BoundParameters.of(10, "x", true);
        assertFalse(p.isNamed());
        assertEquals(10, p.getByIndex(0));
        assertEquals("x", p.getByIndex(1));
        assertEquals(true, p.getByIndex(2));
        assertEquals(3, p.positionalCount());
    }

    @Test
    void when_of_map_then_named_mode() {
        BoundParameters p = BoundParameters.of(new LinkedHashMap<>(Map.of("k", 1)));
        assertTrue(p.isNamed());
        assertEquals(1, p.getByName("k"));
    }

    @Test
    void when_bindAll_then_appends_positionally() {
        BoundParameters p = BoundParameters.positional().bindAll(1, 2, 3);
        assertEquals(3, p.positionalCount());
        assertEquals(2, p.getByIndex(1));
    }

    @Test
    void when_get_missing_name_then_exception() {
        assertThrows(
                SQL4JsonBindException.class,
                () -> BoundParameters.named().bind("a", 1).getByName("missing"));
    }

    @Test
    void when_get_out_of_range_index_then_exception() {
        assertThrows(
                SQL4JsonBindException.class,
                () -> BoundParameters.positional().bind(0, 1).getByIndex(5));
    }

    @Test
    void when_EMPTY_used_and_queried_as_named_then_exception() {
        assertThrows(SQL4JsonBindException.class, () -> BoundParameters.EMPTY.getByName("a"));
    }

    @Test
    void when_EMPTY_used_and_queried_as_positional_then_exception() {
        assertThrows(SQL4JsonBindException.class, () -> BoundParameters.EMPTY.getByIndex(0));
    }

    @Test
    void when_is_empty_then_true_for_EMPTY_and_freshly_created() {
        assertTrue(BoundParameters.EMPTY.isEmpty());
        assertTrue(BoundParameters.named().isEmpty());
        assertTrue(BoundParameters.positional().isEmpty());
        assertFalse(BoundParameters.named().bind("a", 1).isEmpty());
    }
}
