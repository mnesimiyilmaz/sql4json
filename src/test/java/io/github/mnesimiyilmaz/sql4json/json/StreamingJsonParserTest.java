// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.List;
import org.junit.jupiter.api.Test;

class StreamingJsonParserTest {

    @Test
    void streamArray_simple_objects() {
        String json = "[{\"id\":1},{\"id\":2},{\"id\":3}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(3, elements.size());
        assertEquals(
                1,
                elements.get(0)
                        .asObject()
                        .orElseThrow()
                        .get("id")
                        .asNumber()
                        .orElseThrow()
                        .intValue());
        assertEquals(
                3,
                elements.get(2)
                        .asObject()
                        .orElseThrow()
                        .get("id")
                        .asNumber()
                        .orElseThrow()
                        .intValue());
    }

    @Test
    void streamArray_numbers() {
        String json = "[1, 2, 3, 4, 5]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(5, elements.size());
        assertEquals(3, elements.get(2).asNumber().orElseThrow().intValue());
    }

    @Test
    void streamArray_mixed_types() {
        String json = "[1, \"hello\", true, null, 3.14, {\"x\":1}, [1,2]]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(7, elements.size());
        assertTrue(elements.get(0).isNumber());
        assertTrue(elements.get(1).isString());
        assertTrue(elements.get(2).isBoolean());
        assertTrue(elements.get(3).isNull());
        assertTrue(elements.get(4).isNumber());
        assertTrue(elements.get(5).isObject());
        assertTrue(elements.get(6).isArray());
    }

    @Test
    void streamArray_empty_array() {
        List<JsonValue> elements = StreamingJsonParser.streamArray("[]").toList();
        assertTrue(elements.isEmpty());
    }

    @Test
    void streamArray_single_element() {
        List<JsonValue> elements = StreamingJsonParser.streamArray("[42]").toList();
        assertEquals(1, elements.size());
        assertEquals(42, elements.get(0).asNumber().orElseThrow().intValue());
    }

    @Test
    void streamArray_strings_with_escaped_quotes() {
        String json = "[{\"val\":\"he said \\\"hi\\\"\"}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(1, elements.size());
        assertEquals(
                "he said \"hi\"",
                elements.get(0).asObject().orElseThrow().get("val").asString().orElseThrow());
    }

    @Test
    void streamArray_strings_with_escaped_braces_in_unicode() {
        String json = "[{\"val\":\"\\u007B\"}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(1, elements.size());
        assertEquals(
                "{",
                elements.get(0).asObject().orElseThrow().get("val").asString().orElseThrow());
    }

    @Test
    void streamArray_strings_with_escaped_backslash_before_quote() {
        String json = "[{\"val\":\"end\\\\\"}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(1, elements.size());
        assertEquals(
                "end\\",
                elements.get(0).asObject().orElseThrow().get("val").asString().orElseThrow());
    }

    @Test
    void streamArray_deeply_nested() {
        String json = "[{\"a\":{\"b\":{\"c\":1}}},{\"a\":{\"b\":{\"c\":2}}}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(2, elements.size());
    }

    @Test
    void streamArray_nested_arrays_inside_elements() {
        String json = "[{\"tags\":[1,2,3]},{\"tags\":[4,5]}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(2, elements.size());
        assertEquals(
                3,
                elements.get(0)
                        .asObject()
                        .orElseThrow()
                        .get("tags")
                        .asArray()
                        .orElseThrow()
                        .size());
    }

    @Test
    void streamArray_object_root_yields_single_element() {
        String json = "{\"name\":\"Alice\"}";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(1, elements.size());
        assertTrue(elements.get(0).isObject());
    }

    @Test
    void streamArray_primitive_root_yields_empty() {
        assertEquals(0, StreamingJsonParser.streamArray("42").toList().size());
        assertEquals(0, StreamingJsonParser.streamArray("\"hello\"").toList().size());
        assertEquals(0, StreamingJsonParser.streamArray("true").toList().size());
        assertEquals(0, StreamingJsonParser.streamArray("null").toList().size());
    }

    @Test
    void streamArray_with_extra_whitespace() {
        String json = "  [  {\"id\":1}  ,  {\"id\":2}  ]  ";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(2, elements.size());
    }

    @Test
    void streamArray_with_rootPath() {
        String json = "{\"data\":{\"items\":[{\"id\":1},{\"id\":2}]}}";
        List<JsonValue> elements =
                StreamingJsonParser.streamArray(json, "$r.data.items").toList();
        assertEquals(2, elements.size());
    }

    @Test
    void streamArray_with_rootPath_default_root() {
        String json = "[{\"id\":1},{\"id\":2}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json, "$r").toList();
        assertEquals(2, elements.size());
    }

    @Test
    void streamArray_with_rootPath_null() {
        String json = "[{\"id\":1}]";
        List<JsonValue> elements =
                StreamingJsonParser.streamArray(json, (String) null).toList();
        assertEquals(1, elements.size());
    }

    @Test
    void streamArray_with_rootPath_nonexistent_path() {
        String json = "{\"data\":{\"items\":[1,2]}}";
        List<JsonValue> elements =
                StreamingJsonParser.streamArray(json, "$r.data.missing").toList();
        assertTrue(elements.isEmpty());
    }

    @Test
    void streamArray_with_rootPath_object_at_path() {
        String json = "{\"data\":{\"profile\":{\"name\":\"Alice\"}}}";
        List<JsonValue> elements =
                StreamingJsonParser.streamArray(json, "$r.data.profile").toList();
        assertEquals(1, elements.size());
        assertTrue(elements.get(0).isObject());
    }

    @Test
    void streamArray_null_input_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> StreamingJsonParser.streamArray(null));
    }

    @Test
    void streamArray_blank_input_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> StreamingJsonParser.streamArray("   "));
    }

    @Test
    void streamArray_truncated_array_throws() {
        var stream = StreamingJsonParser.streamArray("[{\"id\":1},");
        assertThrows(SQL4JsonExecutionException.class, stream::toList);
    }

    @Test
    void streamArray_malformed_element_throws() {
        var stream = StreamingJsonParser.streamArray("[{bad json}]");
        assertThrows(SQL4JsonExecutionException.class, stream::toList);
    }

    @Test
    void streamArray_early_termination_via_limit() {
        String json = "[1,2,3,4,5,6,7,8,9,10]";
        List<JsonValue> elements =
                StreamingJsonParser.streamArray(json).limit(3).toList();
        assertEquals(3, elements.size());
        assertEquals(1, elements.get(0).asNumber().orElseThrow().intValue());
        assertEquals(3, elements.get(2).asNumber().orElseThrow().intValue());
    }

    @Test
    void streamArray_matches_tree_parser_output() {
        String json = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]";
        List<JsonValue> streamed = StreamingJsonParser.streamArray(json).toList();
        List<JsonValue> tree = JsonParser.parse(json).asArray().orElseThrow();
        assertEquals(tree.size(), streamed.size());
        for (int i = 0; i < tree.size(); i++) {
            assertEquals(JsonSerializer.serialize(tree.get(i)), JsonSerializer.serialize(streamed.get(i)));
        }
    }

    // ── Additional branch coverage tests ────────────────────────────────────

    @Test
    void streamArray_iterator_done_throws_noSuchElement() {
        // ArrayElementIterator: next() when done=true → NoSuchElementException
        var iter = StreamingJsonParser.streamArray("[1]").iterator();
        assertTrue(iter.hasNext());
        iter.next();
        assertFalse(iter.hasNext());
        assertThrows(java.util.NoSuchElementException.class, iter::next);
    }

    @Test
    void streamArray_string_with_backslashN_inside_object() {
        // skipStringContents: backslash followed by non-'u' char
        String json = "[{\"text\":\"line1\\nline2\"}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(1, elements.size());
    }

    @Test
    void streamArray_string_with_various_escapes() {
        // skipStringContents: various escape sequences
        String json = "[{\"text\":\"tab\\there\\\\back\\/slash\"}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(1, elements.size());
    }

    @Test
    void streamArray_nested_array_with_strings() {
        // skipComposite: nested array containing strings with quotes inside
        String json = "[{\"data\":[[\"a\",\"b\"],[\"c\"]]}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(1, elements.size());
    }

    @Test
    void streamArray_object_with_string_values_containing_brackets() {
        // skipComposite + skipStringContents: strings with bracket chars
        String json = "[{\"val\":\"{not a real object}\"}]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(1, elements.size());
        assertEquals(
                "{not a real object}",
                elements.get(0).asObject().orElseThrow().get("val").asString().orElseThrow());
    }

    @Test
    void streamArray_top_level_strings() {
        // skipString: top-level string elements in array
        String json = "[\"hello\", \"world\"]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(2, elements.size());
        assertEquals("hello", elements.get(0).asString().orElseThrow());
    }

    @Test
    void streamArray_negative_numbers() {
        // skipBareValue with negative numbers
        String json = "[-1, -3.14, 0]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(3, elements.size());
    }

    @Test
    void streamArray_booleans_and_nulls() {
        // skipBareValue: boolean and null literals
        String json = "[true, false, null, true]";
        List<JsonValue> elements = StreamingJsonParser.streamArray(json).toList();
        assertEquals(4, elements.size());
    }

    @Test
    void streamArray_trailing_comma_throws() {
        // ArrayElementIterator: trailing comma detection
        var stream = StreamingJsonParser.streamArray("[1,2,]");
        assertThrows(SQL4JsonExecutionException.class, stream::toList);
    }

    @Test
    void streamArray_missing_comma_throws() {
        // ArrayElementIterator: expected ',' or ']' after element
        var stream = StreamingJsonParser.streamArray("[1 2]");
        assertThrows(SQL4JsonExecutionException.class, stream::toList);
    }

    @Test
    void streamArray_with_settings() {
        // StreamingJsonParser: streamArray with settings overload
        var settings = io.github.mnesimiyilmaz.sql4json.settings.DefaultJsonCodecSettings.defaults();
        List<JsonValue> elements =
                StreamingJsonParser.streamArray("[1,2]", settings).toList();
        assertEquals(2, elements.size());
    }

    @Test
    void streamArray_maxInputLength_exceeded() {
        // StreamingJsonParser: maxInputLength exceeded
        var settings = io.github.mnesimiyilmaz.sql4json.settings.DefaultJsonCodecSettings.builder()
                .maxInputLength(5)
                .build();
        assertThrows(SQL4JsonExecutionException.class, () -> StreamingJsonParser.streamArray("[1,2,3,4,5]", settings));
    }
}
