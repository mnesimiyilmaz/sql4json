// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.mapper;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonLongValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.settings.MappingSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonValueMapperCollectionTest {

    private static final MappingSettings S = MappingSettings.defaults();

    private static JsonValue nums(long... values) {
        return new JsonArrayValue(java.util.Arrays.stream(values)
                .mapToObj(v -> (JsonValue) new JsonLongValue(v))
                .toList());
    }

    private static JsonValue strs(String... values) {
        return new JsonArrayValue(java.util.Arrays.stream(values)
                .map(v -> (JsonValue) new JsonStringValue(v))
                .toList());
    }

    @Test
    void when_json_array_mapped_to_primitive_int_array() {
        int[] out = JsonValueMapper.INSTANCE.map(nums(1, 2, 3), int[].class, S);
        assertArrayEquals(new int[] {1, 2, 3}, out);
    }

    @Test
    void when_json_array_mapped_to_boxed_int_array() {
        Integer[] out = JsonValueMapper.INSTANCE.map(nums(4, 5), Integer[].class, S);
        assertArrayEquals(new Integer[] {4, 5}, out);
    }

    @Test
    void when_json_array_mapped_to_string_array() {
        String[] out = JsonValueMapper.INSTANCE.map(strs("a", "b"), String[].class, S);
        assertArrayEquals(new String[] {"a", "b"}, out);
    }

    @Test
    void when_empty_json_array_then_zero_length_array() {
        int[] out = JsonValueMapper.INSTANCE.map(new JsonArrayValue(List.of()), int[].class, S);
        assertEquals(0, out.length);
    }

    // Records exist only to expose Type metadata for testing container element resolution.
    record Box(
            List<Integer> ints, java.util.Set<String> tags, java.util.Collection<Double> nums, Iterable<String> its) {}

    private static java.lang.reflect.Type typeOf(String fieldName) {
        for (var rc : Box.class.getRecordComponents()) {
            if (rc.getName().equals(fieldName)) return rc.getGenericType();
        }
        throw new IllegalStateException(fieldName);
    }

    @Test
    void when_json_array_mapped_to_list_of_int_wrappers() {
        Object out = JsonValueMapper.INSTANCE.mapInternal(
                nums(1, 2, 3), typeOf("ints"), MappingPath.root(), new VisitedStack(), S);
        assertEquals(List.of(1, 2, 3), out);
        assertInstanceOf(java.util.ArrayList.class, out);
    }

    @Test
    void when_json_array_mapped_to_set_of_string_preserves_order() {
        Object out = JsonValueMapper.INSTANCE.mapInternal(
                strs("a", "b", "a"), typeOf("tags"), MappingPath.root(), new VisitedStack(), S);
        assertInstanceOf(java.util.LinkedHashSet.class, out);
        assertEquals(new java.util.LinkedHashSet<>(java.util.List.of("a", "b")), out);
    }

    @Test
    void when_json_array_mapped_to_collection_then_arraylist() {
        Object out = JsonValueMapper.INSTANCE.mapInternal(
                nums(1, 2), typeOf("nums"), MappingPath.root(), new VisitedStack(), S);
        assertInstanceOf(java.util.ArrayList.class, out);
    }

    @Test
    void when_json_array_mapped_to_iterable_then_arraylist() {
        Object out = JsonValueMapper.INSTANCE.mapInternal(
                strs("x"), typeOf("its"), MappingPath.root(), new VisitedStack(), S);
        assertInstanceOf(java.util.ArrayList.class, out);
    }

    record LinkedHashSetBox(java.util.LinkedHashSet<String> tags) {}

    @Test
    void when_json_array_mapped_to_linkedHashSet_explicitly() {
        java.lang.reflect.Type t = LinkedHashSetBox.class.getRecordComponents()[0].getGenericType();
        Object out = JsonValueMapper.INSTANCE.mapInternal(strs("a", "b"), t, MappingPath.root(), new VisitedStack(), S);
        assertInstanceOf(java.util.LinkedHashSet.class, out);
    }

    record MapBox(java.util.Map<String, Integer> counts, java.util.Map<Integer, String> wrongKey) {}

    private static java.lang.reflect.Type mapTypeOf(String fieldName) {
        for (var rc : MapBox.class.getRecordComponents()) {
            if (rc.getName().equals(fieldName)) return rc.getGenericType();
        }
        throw new IllegalStateException(fieldName);
    }

    @Test
    void when_json_object_mapped_to_map_string_integer() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue(
                new java.util.LinkedHashMap<>(java.util.Map.of(
                        "a", new JsonLongValue(1L),
                        "b", new JsonLongValue(2L))));
        Object out =
                JsonValueMapper.INSTANCE.mapInternal(v, mapTypeOf("counts"), MappingPath.root(), new VisitedStack(), S);
        assertInstanceOf(java.util.LinkedHashMap.class, out);
        assertEquals(java.util.Map.of("a", 1, "b", 2), out);
    }

    @Test
    void when_map_key_type_not_string_then_exception() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue(
                new java.util.LinkedHashMap<>(java.util.Map.of("x", new JsonStringValue("y"))));
        assertThrows(
                io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.mapInternal(
                        v, mapTypeOf("wrongKey"), MappingPath.root(), new VisitedStack(), S));
    }
}
