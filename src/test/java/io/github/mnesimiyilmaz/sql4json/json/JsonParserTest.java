package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.settings.DefaultJsonCodecSettings;
import io.github.mnesimiyilmaz.sql4json.settings.DuplicateKeyPolicy;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonParserTest {

    // ── Primitives ──────────────────────────────────────────────────────

    @Test
    void parse_string() {
        JsonValue v = JsonParser.parse("\"hello\"");
        assertEquals("hello", v.asString().orElseThrow());
    }

    @Test
    void parse_string_with_escapes() {
        JsonValue v = JsonParser.parse("\"line1\\nline2\\ttab\\\\backslash\\\"quote\"");
        assertEquals("line1\nline2\ttab\\backslash\"quote", v.asString().orElseThrow());
    }

    @Test
    void parse_string_with_unicode_escape() {
        JsonValue v = JsonParser.parse("\"\\u0041\\u0042\"");
        assertEquals("AB", v.asString().orElseThrow());
    }

    @Test
    void parse_string_with_surrogate_pair() {
        // U+1F600 (grinning face) = \uD83D\uDE00
        JsonValue v = JsonParser.parse("\"\\uD83D\\uDE00\"");
        assertEquals("\uD83D\uDE00", v.asString().orElseThrow());
    }

    @Test
    void parse_string_with_solidus_escape() {
        JsonValue v = JsonParser.parse("\"\\/\"");
        assertEquals("/", v.asString().orElseThrow());
    }

    @Test
    void parse_integer() {
        JsonValue v = JsonParser.parse("42");
        assertTrue(v.isNumber());
        assertEquals(42, v.asNumber().orElseThrow().intValue());
        assertInstanceOf(Long.class, v.asNumber().orElseThrow());
    }

    @Test
    void parse_negative_integer() {
        JsonValue v = JsonParser.parse("-7");
        assertEquals(-7, v.asNumber().orElseThrow().intValue());
    }

    @Test
    void parse_long_number() {
        long big = 3_000_000_000L; // exceeds Integer.MAX_VALUE
        JsonValue v = JsonParser.parse(String.valueOf(big));
        assertInstanceOf(Long.class, v.asNumber().orElseThrow());
        assertEquals(big, v.asNumber().orElseThrow().longValue());
    }

    @Test
    void parse_decimal_number() {
        JsonValue v = JsonParser.parse("3.14");
        assertInstanceOf(Double.class, v.asNumber().orElseThrow());
        assertEquals(3.14, v.asNumber().orElseThrow().doubleValue(), 1e-15);
    }

    @Test
    void parse_scientific_notation() {
        JsonValue v = JsonParser.parse("1.5e10");
        assertInstanceOf(Double.class, v.asNumber().orElseThrow());
        assertEquals(1.5e10, v.asNumber().orElseThrow().doubleValue(), 1e-5);
    }

    @Test
    void parse_negative_exponent() {
        JsonValue v = JsonParser.parse("1.5e-3");
        assertInstanceOf(Double.class, v.asNumber().orElseThrow());
        assertEquals(0.0015, v.asNumber().orElseThrow().doubleValue(), 1e-15);
    }

    @Test
    void parse_zero() {
        JsonValue v = JsonParser.parse("0");
        assertEquals(0, v.asNumber().orElseThrow().intValue());
    }

    @Test
    void parse_true() {
        JsonValue v = JsonParser.parse("true");
        assertTrue(v.isBoolean());
        assertTrue(v.asBoolean().orElseThrow());
    }

    @Test
    void parse_false() {
        JsonValue v = JsonParser.parse("false");
        assertFalse(v.asBoolean().orElseThrow());
    }

    @Test
    void parse_null() {
        JsonValue v = JsonParser.parse("null");
        assertTrue(v.isNull());
    }

    // ── Objects ──────────────────────────────────────────────────────────

    @Test
    void parse_empty_object() {
        JsonValue v = JsonParser.parse("{}");
        assertTrue(v.isObject());
        assertTrue(v.asObject().orElseThrow().isEmpty());
    }

    @Test
    void parse_object_with_fields() {
        JsonValue v = JsonParser.parse("{\"name\":\"Alice\",\"age\":30}");
        Map<String, JsonValue> obj = v.asObject().orElseThrow();
        assertEquals("Alice", obj.get("name").asString().orElseThrow());
        assertEquals(30, obj.get("age").asNumber().orElseThrow().intValue());
    }

    @Test
    void parse_object_preserves_field_order() {
        JsonValue v = JsonParser.parse("{\"z\":1,\"a\":2,\"m\":3}");
        List<String> keys = List.copyOf(v.asObject().orElseThrow().keySet());
        assertEquals(List.of("z", "a", "m"), keys);
    }

    @Test
    void parse_object_duplicate_keys_last_wins() {
        var settings = DefaultJsonCodecSettings.builder()
                .duplicateKeyPolicy(DuplicateKeyPolicy.LAST_WINS).build();
        JsonValue v = JsonParser.parse("{\"a\":1,\"a\":2}", settings);
        assertEquals(2, v.asObject().orElseThrow().get("a").asNumber().orElseThrow().intValue());
    }

    // ── Arrays ──────────────────────────────────────────────────────────

    @Test
    void parse_empty_array() {
        JsonValue v = JsonParser.parse("[]");
        assertTrue(v.isArray());
        assertTrue(v.asArray().orElseThrow().isEmpty());
    }

    @Test
    void parse_array_with_mixed_types() {
        JsonValue v = JsonParser.parse("[1, \"two\", true, null, 3.14]");
        List<JsonValue> arr = v.asArray().orElseThrow();
        assertEquals(5, arr.size());
        assertTrue(arr.get(0).isNumber());
        assertTrue(arr.get(1).isString());
        assertTrue(arr.get(2).isBoolean());
        assertTrue(arr.get(3).isNull());
        assertTrue(arr.get(4).isNumber());
    }

    // ── Nested structures ───────────────────────────────────────────────

    @Test
    void parse_nested_object() {
        JsonValue v = JsonParser.parse("{\"profile\":{\"address\":{\"city\":\"Istanbul\"}}}");
        String city = v.asObject().orElseThrow()
                .get("profile").asObject().orElseThrow()
                .get("address").asObject().orElseThrow()
                .get("city").asString().orElseThrow();
        assertEquals("Istanbul", city);
    }

    @Test
    void parse_array_of_objects() {
        JsonValue v = JsonParser.parse("[{\"id\":1},{\"id\":2},{\"id\":3}]");
        List<JsonValue> arr = v.asArray().orElseThrow();
        assertEquals(3, arr.size());
        assertEquals(2, arr.get(1).asObject().orElseThrow().get("id").asNumber().orElseThrow().intValue());
    }

    // ── Whitespace handling ─────────────────────────────────────────────

    @Test
    void parse_with_extra_whitespace() {
        JsonValue v = JsonParser.parse("  {  \"name\"  :  \"Alice\"  }  ");
        assertEquals("Alice", v.asObject().orElseThrow().get("name").asString().orElseThrow());
    }

    // ── Immutability ────────────────────────────────────────────────────

    @Test
    void parsed_object_is_unmodifiable() {
        JsonValue v = JsonParser.parse("{\"a\":1}");
        var obj = v.asObject().orElseThrow();
        assertThrows(UnsupportedOperationException.class,
                () -> obj.put("b", JsonNullValue.INSTANCE));
    }

    @Test
    void parsed_array_is_unmodifiable() {
        JsonValue v = JsonParser.parse("[1,2]");
        var arr = v.asArray().orElseThrow();
        assertThrows(UnsupportedOperationException.class,
                () -> arr.add(JsonNullValue.INSTANCE));
    }

    // ── Error cases ─────────────────────────────────────────────────────

    @Test
    void parse_empty_string_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse(""));
    }

    @Test
    void parse_blank_string_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("   "));
    }

    @Test
    void parse_truncated_object_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("{\"a\":"));
    }

    @Test
    void parse_trailing_comma_in_array_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("[1,2,]"));
    }

    @Test
    void parse_trailing_comma_in_object_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("{\"a\":1,}"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"undefined", "NaN", "Infinity", "'single quotes'"})
    void parse_invalid_tokens_throw(String input) {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse(input));
    }

    @Test
    void parse_trailing_garbage_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("42 extra"));
    }

    // ── Security: Nesting depth ────────────────────────────────────────

    @Test
    void parse_max_nesting_depth_exceeded_throws() {
        // 257-level nesting exceeds the configured 256-level limit
        String json = "[".repeat(257) + "]".repeat(257);
        var settings = DefaultJsonCodecSettings.builder().maxNestingDepth(256).build();
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse(json, settings));
    }

    @Test
    void parse_at_max_nesting_depth_succeeds() {
        // exactly 256 levels — must succeed under the configured limit
        String json = "[".repeat(256) + "]".repeat(256);
        var settings = DefaultJsonCodecSettings.builder().maxNestingDepth(256).build();
        assertDoesNotThrow(() -> JsonParser.parse(json, settings));
    }

    // ── Security: Number token length ───────────────────────────────────

    @Test
    void parse_number_exceeding_long_range() {
        String huge = "99999999999999999999";
        JsonValue v = JsonParser.parse(huge);
        assertInstanceOf(BigDecimal.class, v.asNumber().orElseThrow());
    }

    @Test
    void parse_number_token_exceeding_max_length_throws() {
        // 1001-char integer exceeds the configured 1000-char limit
        String huge = "1" + "0".repeat(1000);
        var settings = DefaultJsonCodecSettings.builder().maxNumberLength(1000).build();
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse(huge, settings));
    }

    @Test
    void parse_bigdecimal_bomb_blocked_by_token_length() {
        // "1e" + 999 nines = 1001 chars, exceeds the configured 1000-char limit
        String bomb = "1e" + "9".repeat(999);
        var settings = DefaultJsonCodecSettings.builder().maxNumberLength(1000).build();
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse(bomb, settings));
    }

    @Test
    void parse_number_at_max_length_succeeds() {
        // exactly 1000 chars — must succeed under the configured limit
        String maxLen = "1" + "0".repeat(999);
        var settings = DefaultJsonCodecSettings.builder().maxNumberLength(1000).build();
        assertDoesNotThrow(() -> JsonParser.parse(maxLen, settings));
    }

    @Test
    void parse_compact_bigdecimal_bomb_blocked() {
        // "1e999999999" is only 11 chars but has a pathological exponent
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("1e999999999"));
    }

    @Test
    void parse_reasonable_exponent_succeeds() {
        // Exponents within 4 digits (up to 9999) are allowed
        JsonValue v = JsonParser.parse("1.5e100");
        assertInstanceOf(Double.class, v.asNumber().orElseThrow());
    }

    @Test
    void parse_null_input_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse(null));
    }

    // ── Security: String value length ───────────────────────────────────

    @Test
    void parse_string_exceeding_max_length_throws() {
        // 1_048_577-char string exceeds the default 1 MiB (1_048_576-char) limit
        String huge = "\"" + "a".repeat(1_048_577) + "\"";
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse(huge));
    }

    // ── Security: Property name length ──────────────────────────────────

    @Test
    void parse_property_name_exceeding_max_length_throws() {
        // 1_025-char key exceeds the default 1_024-char limit
        String hugeKey = "\"" + "k".repeat(1_025) + "\"";
        String json = "{" + hugeKey + ":1}";
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse(json));
    }

    // ── Additional edge cases for coverage ─────────────────────────────────

    @Test
    void parse_unescaped_null_byte_in_string_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("\"hello\u0000world\""));
    }

    @Test
    void parse_unescaped_tab_in_string_throws() {
        // Raw tab (0x09) is a control char < 0x20 and must be escaped
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("\"hello\tworld\""));
    }

    @Test
    void parse_invalid_escape_sequence_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("\"\\x\""));
    }

    @Test
    void parse_unterminated_string_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("\"no end"));
    }

    @Test
    void parse_incomplete_unicode_escape_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("\"\\u00\""));
    }

    @Test
    void parse_invalid_unicode_hex_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("\"\\u00GG\""));
    }

    @Test
    void parse_lone_high_surrogate_accepted() {
        // Lone high surrogate without low pair — parser accepts it as a raw char
        JsonValue v = JsonParser.parse("\"\\uD83D\"");
        assertNotNull(v.asString().orElseThrow());
    }

    @Test
    void parse_number_with_fraction_and_exponent() {
        JsonValue v = JsonParser.parse("1.5e2");
        assertInstanceOf(Double.class, v.asNumber().orElseThrow());
        assertEquals(150.0, v.asNumber().orElseThrow().doubleValue(), 1e-10);
    }

    @Test
    void parse_negative_zero() {
        JsonValue v = JsonParser.parse("-0");
        assertEquals(0, v.asNumber().orElseThrow().intValue());
    }

    @Test
    void parse_integer_max_boundary() {
        JsonValue v = JsonParser.parse("2147483647"); // Integer.MAX_VALUE
        assertInstanceOf(Long.class, v.asNumber().orElseThrow());
        assertEquals(Integer.MAX_VALUE, v.asNumber().orElseThrow().intValue());
    }

    @Test
    void parse_integer_max_plus_one_is_long() {
        JsonValue v = JsonParser.parse("2147483648"); // Integer.MAX_VALUE + 1
        assertInstanceOf(Long.class, v.asNumber().orElseThrow());
        assertEquals(2147483648L, v.asNumber().orElseThrow().longValue());
    }

    @Test
    void parse_long_max_boundary() {
        JsonValue v = JsonParser.parse("9223372036854775807"); // Long.MAX_VALUE
        assertInstanceOf(Long.class, v.asNumber().orElseThrow());
        assertEquals(Long.MAX_VALUE, v.asNumber().orElseThrow().longValue());
    }

    @Test
    void parse_long_max_plus_one_is_bigdecimal() {
        JsonValue v = JsonParser.parse("9223372036854775808"); // Long.MAX_VALUE + 1
        assertInstanceOf(BigDecimal.class, v.asNumber().orElseThrow());
    }

    @Test
    void parse_negative_integer_min_boundary() {
        JsonValue v = JsonParser.parse("-2147483648"); // Integer.MIN_VALUE
        assertInstanceOf(Long.class, v.asNumber().orElseThrow());
        assertEquals(Integer.MIN_VALUE, v.asNumber().orElseThrow().intValue());
    }

    @Test
    void parse_number_missing_digit_after_decimal_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("1."));
    }

    @Test
    void parse_number_missing_digit_after_exponent_sign_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("1e+"));
    }

    @Test
    void parse_exponent_with_plus_sign() {
        JsonValue v = JsonParser.parse("1e+2");
        assertInstanceOf(Double.class, v.asNumber().orElseThrow());
    }

    @Test
    void parse_uppercase_exponent() {
        JsonValue v = JsonParser.parse("1E2");
        assertInstanceOf(Double.class, v.asNumber().orElseThrow());
    }

    @Test
    void parse_escape_backspace_and_formfeed() {
        JsonValue v = JsonParser.parse("\"\\b\\f\"");
        assertEquals("\b\f", v.asString().orElseThrow());
    }

    @Test
    void parse_escape_carriage_return() {
        JsonValue v = JsonParser.parse("\"\\r\"");
        assertEquals("\r", v.asString().orElseThrow());
    }

    @Test
    void parse_truncated_escape_at_end_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("\"\\"));
    }

    @Test
    void parse_boolean_true_returns_same_instance() {
        JsonValue v1 = JsonParser.parse("[true, true]");
        List<JsonValue> arr = v1.asArray().orElseThrow();
        assertSame(arr.get(0), arr.get(1));
    }

    @Test
    void parse_boolean_false_returns_same_instance() {
        JsonValue v1 = JsonParser.parse("[false, false]");
        List<JsonValue> arr = v1.asArray().orElseThrow();
        assertSame(arr.get(0), arr.get(1));
    }

    @Test
    void parse_duplicate_keys_interned() {
        JsonValue v = JsonParser.parse("[{\"name\":\"A\"},{\"name\":\"B\"}]");
        List<JsonValue> arr = v.asArray().orElseThrow();
        Map<String, JsonValue> obj1 = arr.get(0).asObject().orElseThrow();
        Map<String, JsonValue> obj2 = arr.get(1).asObject().orElseThrow();
        String key1 = obj1.keySet().iterator().next();
        String key2 = obj2.keySet().iterator().next();
        assertSame(key1, key2);
    }

    @Test
    void parse_small_integer_returns_cached_instance() {
        JsonValue v = JsonParser.parse("[42, 42]");
        List<JsonValue> arr = v.asArray().orElseThrow();
        assertSame(arr.get(0), arr.get(1));
    }

    // ── Additional branch-coverage edge cases ──────────────────────────────

    @Test
    void parse_unterminated_object_after_value_throws() {
        // EOF after value, before ',' or '}' — exercises L145 in parseObject
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("{\"a\":1"));
    }

    @Test
    void parse_unterminated_array_after_element_throws() {
        // EOF after element, before ',' or ']' — exercises L192 in parseArray
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("[1"));
    }

    @Test
    void parse_unterminated_array_open_only_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("["));
    }

    @Test
    void parse_object_missing_colon_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("{\"a\" 1}"));
    }

    @Test
    void parse_object_non_string_key_throws() {
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("{1:2}"));
    }

    @Test
    void parse_high_surrogate_followed_by_non_escape_kept_as_raw() {
        // High surrogate followed by something other than backslash-u — falls through to else
        // branch in appendUnicodeChar
        JsonValue v = JsonParser.parse("\"\\uD83Dx\"");
        assertNotNull(v.asString().orElseThrow());
    }

    @Test
    void parse_high_surrogate_at_eof_kept_as_raw() {
        // High surrogate at end of string — pos+1 not < endPos branch
        JsonValue v = JsonParser.parse("\"\\uD83D\"");
        assertNotNull(v.asString().orElseThrow());
    }

    @Test
    void parse_just_minus_throws() {
        // '-' followed by EOF — consumeIntegerPart sees pos>=endPos
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("-"));
    }

    @Test
    void parse_invalid_number_starting_letter_throws() {
        // Triggers consumeIntegerPart "Invalid number" branch (L313)
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("-x"));
    }

    @Test
    void parse_truncated_true_throws() {
        // 'tru' — pos+4 > endPos in parseBoolean
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("tru"));
    }

    @Test
    void parse_truncated_false_throws() {
        // 'fals' — pos+5 > endPos in parseBoolean
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("fals"));
    }

    @Test
    void parse_truncated_null_throws() {
        // 'nul' — pos+4 > endPos in parseNull
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("nul"));
    }

    @Test
    void parse_misspelled_true_throws() {
        // 'trux' — startsWith fails in parseBoolean
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("trux"));
    }

    @Test
    void parse_misspelled_null_throws() {
        // 'nuxx' — startsWith fails in parseNull
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("nuxx"));
    }

    @Test
    void parse_expect_at_eof_throws() {
        // After parsing "1", expect(',') called at EOF — exercises expect() pos>=endPos branch
        assertThrows(SQL4JsonExecutionException.class, () -> JsonParser.parse("[1 "));
    }

    // ── Shape sharing — Phase B ──────────────────────────────────────────────

    @Test
    void parseObject_sameShapeRecords_shareKeyArray() {
        String json = "[" +
                "{\"id\":1,\"name\":\"A\"}," +
                "{\"id\":2,\"name\":\"B\"}," +
                "{\"id\":3,\"name\":\"C\"}]";
        JsonValue parsed = JsonParser.parse(json);
        var arr = ((JsonArrayValue) parsed).elements();
        String[] k0 = extractKeyArray(arr.get(0));
        String[] k1 = extractKeyArray(arr.get(1));
        String[] k2 = extractKeyArray(arr.get(2));
        assertSame(k0, k1, "same-shape records must share key array");
        assertSame(k1, k2);
    }

    @Test
    void parseObject_differentShapes_doNotShareKeyArray() {
        String json = "[" +
                "{\"id\":1,\"name\":\"A\"}," +
                "{\"id\":2,\"city\":\"X\"}]";
        JsonValue parsed = JsonParser.parse(json);
        var arr = ((JsonArrayValue) parsed).elements();
        assertNotSame(extractKeyArray(arr.get(0)), extractKeyArray(arr.get(1)));
    }

    @Test
    void parseObject_sameShapeNestedObjects_alsoShareKeyArray() {
        String json = "[" +
                "{\"a\":{\"x\":1,\"y\":2}}," +
                "{\"a\":{\"x\":3,\"y\":4}}]";
        JsonValue parsed = JsonParser.parse(json);
        var arr = ((JsonArrayValue) parsed).elements();
        JsonValue nested0 = ((JsonObjectValue) arr.get(0)).fields().get("a");
        JsonValue nested1 = ((JsonObjectValue) arr.get(1)).fields().get("a");
        assertSame(extractKeyArray(nested0), extractKeyArray(nested1));
    }

    @Test
    void parseObject_exhaustsShapeRegistry_returnsExactFitWithoutSharing() {
        StringBuilder sb = new StringBuilder("[");
        // 257 distinct shapes — one more than MAX_SHAPES.
        for (int i = 0; i < 257; i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"k").append(i).append("\":1}");
        }
        // 258th distinct shape — exhausts cap; must still produce exact-fit array.
        sb.append(",{\"k257\":1}]");
        JsonValue parsed = JsonParser.parse(sb.toString());
        var arr = ((JsonArrayValue) parsed).elements();
        assertEquals(258, arr.size());
        String[] last = extractKeyArray(arr.get(arr.size() - 1));
        assertEquals(1, last.length, "bypass path must still return exact-fit");
    }

    // Reach the internal CompactStringMap.keys array via reflection.
    // Justification: shape sharing is an internal optimisation; the public API doesn't
    // expose it, but tests must verify identity to lock in the optimisation.
    private static String[] extractKeyArray(JsonValue obj) {
        try {
            var fields = ((JsonObjectValue) obj).fields();
            // CompactStringMap is package-private; reach via reflection.
            java.lang.reflect.Field f = fields.getClass().getDeclaredField("keys");
            f.setAccessible(true);
            return (String[]) f.get(fields);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("CompactStringMap layout changed: " + e);
        }
    }
}
