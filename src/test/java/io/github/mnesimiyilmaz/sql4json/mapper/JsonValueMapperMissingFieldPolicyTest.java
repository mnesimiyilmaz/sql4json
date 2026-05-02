// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.mapper;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException;
import io.github.mnesimiyilmaz.sql4json.json.JsonDoubleValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonLongValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.settings.MappingSettings;
import io.github.mnesimiyilmaz.sql4json.settings.MissingFieldPolicy;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.*;
import org.junit.jupiter.api.Test;

class JsonValueMapperMissingFieldPolicyTest {

    record WithOptional(String name, Optional<Integer> age) {}

    record WithPrimitive(String name, int age) {}

    record Nested(String id, WithPrimitive inner) {}

    record WithCollections(String id, List<String> tags, Set<Integer> ids, Map<String, Integer> counts, int[] nums) {}

    private static final MappingSettings IGNORE = MappingSettings.defaults();
    private static final MappingSettings FAIL = MappingSettings.builder()
            .missingFieldPolicy(MissingFieldPolicy.FAIL)
            .build();

    private static JsonValue obj(Map<String, JsonValue> m) {
        return new JsonObjectValue(new LinkedHashMap<>(m));
    }

    @Test
    void when_ignore_and_missing_reference_field_then_null() {
        JsonValue v = obj(Map.of("age", new JsonLongValue(30L)));
        WithOptional w = JsonValueMapper.INSTANCE.map(v, WithOptional.class, IGNORE);
        assertNull(w.name());
    }

    @Test
    void when_ignore_and_missing_optional_field_then_empty() {
        JsonValue v = obj(Map.of("name", new JsonStringValue("Alice")));
        WithOptional w = JsonValueMapper.INSTANCE.map(v, WithOptional.class, IGNORE);
        assertEquals(Optional.empty(), w.age());
    }

    @Test
    void when_ignore_and_missing_primitive_field_then_default_zero() {
        JsonValue v = obj(Map.of("name", new JsonStringValue("Bob")));
        WithPrimitive w = JsonValueMapper.INSTANCE.map(v, WithPrimitive.class, IGNORE);
        assertEquals(0, w.age());
    }

    @Test
    void when_fail_and_missing_field_then_exception_with_path() {
        JsonValue v = obj(Map.of("name", new JsonStringValue("Bob")));
        SQL4JsonMappingException e = assertThrows(
                SQL4JsonMappingException.class, () -> JsonValueMapper.INSTANCE.map(v, WithPrimitive.class, FAIL));
        assertTrue(e.getMessage().contains("$.age"));
    }

    @Test
    void when_fail_and_missing_nested_field_then_path_includes_chain() {
        JsonValue inner = obj(Map.of("name", new JsonStringValue("x"))); // missing `age`
        JsonValue outer = obj(Map.of("id", new JsonStringValue("o1"), "inner", inner));
        SQL4JsonMappingException e = assertThrows(
                SQL4JsonMappingException.class, () -> JsonValueMapper.INSTANCE.map(outer, Nested.class, FAIL));
        assertTrue(e.getMessage().contains("$.inner.age"), e.getMessage());
    }

    @Test
    void when_optional_present_and_non_null_then_wrapped() {
        JsonValue v = obj(Map.of(
                "name", new JsonStringValue("Alice"),
                "age", new JsonLongValue(30L)));
        WithOptional w = JsonValueMapper.INSTANCE.map(v, WithOptional.class, IGNORE);
        assertEquals(Optional.of(30), w.age());
    }

    @Test
    void when_ignore_and_missing_collection_fields_then_empty_containers() {
        JsonValue v = obj(Map.of("id", new JsonStringValue("x")));
        WithCollections w = JsonValueMapper.INSTANCE.map(v, WithCollections.class, IGNORE);
        assertNotNull(w.tags());
        assertTrue(w.tags().isEmpty());
        assertNotNull(w.ids());
        assertTrue(w.ids().isEmpty());
        assertNotNull(w.counts());
        assertTrue(w.counts().isEmpty());
        assertNotNull(w.nums());
        assertEquals(0, w.nums().length);
    }

    record AllPrimitives(boolean b, char c, byte by, short s, int i, long l, float f, double d) {}

    record WithIterable(String id, Iterable<String> tags) {}

    @Test
    void when_ignore_and_missing_iterable_field_then_empty_arrayList() {
        JsonValue v = obj(Map.of("id", new JsonStringValue("x")));
        WithIterable w = JsonValueMapper.INSTANCE.map(v, WithIterable.class, IGNORE);
        assertNotNull(w.tags());
        assertFalse(w.tags().iterator().hasNext());
    }

    @Test
    void when_ignore_and_missing_all_primitive_fields_then_zero_defaults() {
        JsonValue v = obj(Map.of()); // empty — all fields missing
        AllPrimitives w = JsonValueMapper.INSTANCE.map(v, AllPrimitives.class, IGNORE);
        assertFalse(w.b());
        assertEquals('\0', w.c());
        assertEquals((byte) 0, w.by());
        assertEquals((short) 0, w.s());
        assertEquals(0, w.i());
        assertEquals(0L, w.l());
        assertEquals(0f, w.f());
        assertEquals(0d, w.d());
    }

    @Test
    void when_all_primitive_fields_present_then_mapped() {
        // Exercises mapNumberToPrimitive for byte/short/int/long/float/double/char primitive classes.
        // (boolean is mapped via mapBoolean, not mapNumberToPrimitive.)
        JsonValue v = obj(Map.of(
                "b", new io.github.mnesimiyilmaz.sql4json.json.JsonBooleanValue(true),
                "c", new JsonLongValue(65L),
                "by", new JsonLongValue(7L),
                "s", new JsonLongValue(1000L),
                "i", new JsonLongValue(123L),
                "l", new JsonLongValue(456L),
                "f", new JsonDoubleValue(1.5),
                "d", new JsonDoubleValue(2.5)));
        AllPrimitives w = JsonValueMapper.INSTANCE.map(v, AllPrimitives.class, IGNORE);
        assertTrue(w.b());
        assertEquals('A', w.c());
        assertEquals((byte) 7, w.by());
        assertEquals((short) 1000, w.s());
        assertEquals(123, w.i());
        assertEquals(456L, w.l());
        assertEquals(1.5f, w.f());
        assertEquals(2.5, w.d());
    }
}
