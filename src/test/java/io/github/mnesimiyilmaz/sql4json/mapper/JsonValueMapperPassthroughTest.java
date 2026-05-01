package io.github.mnesimiyilmaz.sql4json.mapper;

import io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonLongValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.settings.MappingSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonValueMapperPassthroughTest {

    private static final MappingSettings S = MappingSettings.defaults();

    @Test
    void when_target_is_json_value_then_passthrough_same_instance() {
        JsonValue v = new JsonStringValue("x");
        assertSame(v, JsonValueMapper.INSTANCE.map(v, JsonValue.class, S));
    }

    @Test
    void when_target_is_subtype_of_json_value_then_passthrough() {
        JsonValue v = new JsonStringValue("x");
        Object out = JsonValueMapper.INSTANCE.map(v, JsonStringValue.class, S);
        assertSame(v, out);
    }

    @Test
    void when_target_is_object_for_json_object_then_linkedhashmap() {
        JsonValue v = new JsonObjectValue(new LinkedHashMap<>(Map.of(
                "a", new JsonLongValue(1L),
                "b", new JsonStringValue("hi"))));
        Object out = JsonValueMapper.INSTANCE.map(v, Object.class, S);
        assertInstanceOf(LinkedHashMap.class, out);
        Map<?, ?> m = (Map<?, ?>) out;
        assertEquals(1, ((Number) m.get("a")).intValue());
        assertEquals("hi", m.get("b"));
    }

    @Test
    void when_target_is_object_for_json_array_then_arraylist() {
        JsonValue v = new JsonArrayValue(List.of(
                new JsonLongValue(1L), new JsonStringValue("x")));
        Object out = JsonValueMapper.INSTANCE.map(v, Object.class, S);
        assertInstanceOf(ArrayList.class, out);
        List<?> l = (List<?>) out;
        assertEquals(2, l.size());
    }
}
