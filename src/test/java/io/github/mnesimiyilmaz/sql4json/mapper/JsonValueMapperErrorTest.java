// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.mapper;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException;
import io.github.mnesimiyilmaz.sql4json.json.JsonLongValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.settings.MappingSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonValueMapperErrorTest {

    // POJO without public no-arg constructor.
    public static class NoDefaultCtor {
        private final String name;

        public NoDefaultCtor(String n) {
            this.name = n;
        }

        public String getName() {
            return name;
        }
    }

    public static class ThrowingSetter {
        public void setAge(int a) {
            if (a < 0) throw new IllegalArgumentException("negative");
        }
    }

    public static class Overloaded {
        private Object value;

        public void setValue(Object o) {
            this.value = o;
        }

        public void setValue(Integer i) {
            this.value = i;
        } // more specific

        public Object getValue() {
            return value;
        }
    }

    public static class Node {
        private Node next;

        public void setNext(Node n) {
            this.next = n;
        }

        public Node getNext() {
            return next;
        }
    }

    private static final MappingSettings S = MappingSettings.defaults();

    private static JsonValue obj(Map<String, JsonValue> m) {
        return new JsonObjectValue(new LinkedHashMap<>(m));
    }

    @Test
    void when_pojo_has_no_public_noarg_ctor_then_exception() {
        JsonValue v = obj(Map.of("name", new JsonStringValue("x")));
        SQL4JsonMappingException e = assertThrows(
                SQL4JsonMappingException.class, () -> JsonValueMapper.INSTANCE.map(v, NoDefaultCtor.class, S));
        assertTrue(e.getMessage().contains("No public no-arg constructor"));
        assertTrue(e.getMessage().contains(NoDefaultCtor.class.getName()));
    }

    @Test
    void when_setter_throws_then_wrapped_with_cause() {
        JsonValue v = obj(Map.of("age", new JsonLongValue(-1L)));
        SQL4JsonMappingException e = assertThrows(
                SQL4JsonMappingException.class, () -> JsonValueMapper.INSTANCE.map(v, ThrowingSetter.class, S));
        assertInstanceOf(IllegalArgumentException.class, e.getCause());
        assertEquals("negative", e.getCause().getMessage());
    }

    @Test
    void when_overloaded_setter_then_more_specific_picked() {
        JsonValue v = obj(Map.of("value", new JsonLongValue(42L)));
        Overloaded o = JsonValueMapper.INSTANCE.map(v, Overloaded.class, S);
        // Integer setter chosen — value stored as Integer not Double
        assertEquals(Integer.valueOf(42), o.getValue());
    }

    @Test
    void when_pojo_self_references_with_cycle_then_exception() {
        // Build cyclic JSON structure: {"next": <the same map>} — must share identity.
        var outer = new LinkedHashMap<String, JsonValue>();
        var inner = new JsonObjectValue(outer);
        outer.put("next", inner);
        assertThrows(SQL4JsonMappingException.class, () -> JsonValueMapper.INSTANCE.map(inner, Node.class, S));
    }
}
