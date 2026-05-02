// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

class JsonParserRegionTest {

    @Test
    void parseRegion_object_in_middle_of_string() {
        String json = "XXXXX{\"name\":\"Alice\",\"age\":30}YYYYY";
        JsonValue v = JsonParser.parseRegion(json, 5, 30);
        assertEquals("Alice", v.asObject().orElseThrow().get("name").asString().orElseThrow());
        assertEquals(
                30,
                v.asObject().orElseThrow().get("age").asNumber().orElseThrow().intValue());
    }

    @Test
    void parseRegion_number_in_middle() {
        String json = "[42,100,200]";
        JsonValue v = JsonParser.parseRegion(json, 1, 3);
        assertEquals(42, v.asNumber().orElseThrow().intValue());
    }

    @Test
    void parseRegion_string_in_middle() {
        String json = "[\"hello\",\"world\"]";
        JsonValue v = JsonParser.parseRegion(json, 1, 8);
        assertEquals("hello", v.asString().orElseThrow());
    }

    @Test
    void parseRegion_boolean_true_at_boundary() {
        String json = "XXXXtrueYYYY";
        JsonValue v = JsonParser.parseRegion(json, 4, 8);
        assertTrue(v.asBoolean().orElseThrow());
    }

    @Test
    void parseRegion_boolean_false_at_boundary() {
        String json = "XXXXfalseYYY";
        JsonValue v = JsonParser.parseRegion(json, 4, 9);
        assertFalse(v.asBoolean().orElseThrow());
    }

    @Test
    void parseRegion_null_at_boundary() {
        String json = "XXXXnullYYYY";
        JsonValue v = JsonParser.parseRegion(json, 4, 8);
        assertTrue(v.isNull());
    }

    @Test
    void parseRegion_boolean_true_would_read_past_endPos() {
        String json = "true";
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parseRegion(json, 0, 3));
    }

    @Test
    void parseRegion_boolean_false_would_read_past_endPos() {
        String json = "false";
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parseRegion(json, 0, 4));
    }

    @Test
    void parseRegion_null_would_read_past_endPos() {
        String json = "null";
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parseRegion(json, 0, 3));
    }

    @Test
    void parseRegion_trailing_content_throws() {
        String json = "42 extra";
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parseRegion(json, 0, 8));
    }

    @Test
    void parseRegion_with_whitespace_padding() {
        String json = "XXXX  42  YYYY";
        JsonValue v = JsonParser.parseRegion(json, 4, 10);
        assertEquals(42, v.asNumber().orElseThrow().intValue());
    }

    @Test
    void parseRegion_nested_array() {
        String json = "PREFIX[1,[2,3],4]SUFFIX";
        JsonValue v = JsonParser.parseRegion(json, 6, 17);
        assertEquals(3, v.asArray().orElseThrow().size());
    }

    @Test
    void parseRegion_unicode_escape_at_endPos_boundary() {
        String json = "\"\\u0041\"XXXX";
        JsonValue v = JsonParser.parseRegion(json, 0, 8);
        assertEquals("A", v.asString().orElseThrow());
    }

    @Test
    void parseRegion_full_range_matches_parse() {
        String json = "{\"x\":true,\"y\":[1,2,3]}";
        JsonValue fromParse = JsonParser.parse(json);
        JsonValue fromRegion = JsonParser.parseRegion(json, 0, json.length());
        assertEquals(JsonSerializer.serialize(fromParse), JsonSerializer.serialize(fromRegion));
    }
}
