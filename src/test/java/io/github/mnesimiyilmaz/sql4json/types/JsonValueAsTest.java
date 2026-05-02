// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.mnesimiyilmaz.sql4json.json.JsonLongValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.settings.MissingFieldPolicy;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonValueAsTest {

    record Point(int x, int y) {}

    @Test
    void when_as_called_with_default_settings_then_mapped() {
        JsonValue v = new JsonObjectValue(
                new LinkedHashMap<>(Map.of("x", new JsonLongValue(1L), "y", new JsonLongValue(2L))));
        assertEquals(new Point(1, 2), v.as(Point.class));
    }

    @Test
    void when_as_called_with_fail_settings_and_missing_field_then_exception() {
        JsonValue v = new JsonObjectValue(new LinkedHashMap<>(Map.of("x", new JsonLongValue(1L))));
        Sql4jsonSettings strict = Sql4jsonSettings.builder()
                .mapping(m -> m.missingFieldPolicy(MissingFieldPolicy.FAIL))
                .build();
        assertThrows(
                io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException.class,
                () -> v.as(Point.class, strict));
    }

    @Test
    void when_as_called_on_string_value_for_string_target() {
        JsonValue v = new JsonStringValue("hello");
        assertEquals("hello", v.as(String.class));
    }
}
