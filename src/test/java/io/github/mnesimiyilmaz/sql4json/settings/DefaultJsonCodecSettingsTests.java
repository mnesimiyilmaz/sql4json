// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.settings;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.SQL4Json;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import org.junit.jupiter.api.Test;

class DefaultJsonCodecSettingsTests {

    @Test
    void defaults_have_safe_values() {
        var s = DefaultJsonCodecSettings.defaults();
        assertEquals(10 * 1024 * 1024, s.maxInputLength());
        assertEquals(64, s.maxNestingDepth());
        assertEquals(1 * 1024 * 1024, s.maxStringLength());
        assertEquals(64, s.maxNumberLength());
        assertEquals(1_024, s.maxPropertyNameLength());
        assertEquals(1_000_000, s.maxArrayElements());
        assertEquals(DuplicateKeyPolicy.REJECT, s.duplicateKeyPolicy());
    }

    @Test
    void builder_starts_from_defaults_and_overrides_single_field() {
        var s = DefaultJsonCodecSettings.builder()
                .maxStringLength(5 * 1024 * 1024)
                .build();
        assertEquals(5 * 1024 * 1024, s.maxStringLength());
        assertEquals(64, s.maxNestingDepth());
    }

    @Test
    void builder_rejects_zero_or_negative_values_on_every_field() {
        var b = DefaultJsonCodecSettings.builder();
        assertThrows(IllegalArgumentException.class, () -> b.maxInputLength(0));
        assertThrows(IllegalArgumentException.class, () -> b.maxInputLength(-1));
        assertThrows(IllegalArgumentException.class, () -> b.maxNestingDepth(0));
        assertThrows(IllegalArgumentException.class, () -> b.maxStringLength(0));
        assertThrows(IllegalArgumentException.class, () -> b.maxNumberLength(0));
        assertThrows(IllegalArgumentException.class, () -> b.maxPropertyNameLength(0));
        assertThrows(IllegalArgumentException.class, () -> b.maxArrayElements(0));
    }

    @Test
    void builder_rejects_null_duplicate_key_policy() {
        var b = DefaultJsonCodecSettings.builder();
        assertThrows(NullPointerException.class, () -> b.duplicateKeyPolicy(null));
    }

    @Test
    void compact_constructor_rejects_null_duplicate_key_policy() {
        assertThrows(NullPointerException.class, () -> new DefaultJsonCodecSettings(1, 1, 1, 1, 1, 1, null));
    }

    @Test
    void compact_constructor_rejects_non_positive_limits() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DefaultJsonCodecSettings(0, 1, 1, 1, 1, 1, DuplicateKeyPolicy.REJECT));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DefaultJsonCodecSettings(1, 1, 1, 1, 1, -5, DuplicateKeyPolicy.REJECT));
    }

    @Test
    void parse_rejects_input_above_maxInputLength() {
        var limits = DefaultJsonCodecSettings.builder().maxInputLength(16).build();
        String json = "{\"key\":\"longer than sixteen chars total\"}";
        var codec = new DefaultJsonCodec(limits);
        var ex = assertThrows(SQL4JsonExecutionException.class, () -> codec.parse(json));
        assertTrue(ex.getMessage().contains("input length exceeds configured maximum"));
    }

    @Test
    void parse_accepts_input_at_exactly_maxInputLength() {
        String json = "{\"a\":1}";
        var limits =
                DefaultJsonCodecSettings.builder().maxInputLength(json.length()).build();
        var codec = new DefaultJsonCodec(limits);
        assertDoesNotThrow(() -> codec.parse(json));
    }

    @Test
    void parse_rejects_array_above_maxArrayElements() {
        var limits = DefaultJsonCodecSettings.builder().maxArrayElements(3).build();
        var codec = new DefaultJsonCodec(limits);
        var ex = assertThrows(SQL4JsonExecutionException.class, () -> codec.parse("[1,2,3,4]"));
        assertTrue(ex.getMessage().contains("exceeds configured maximum"));
    }

    @Test
    void parse_accepts_array_at_exactly_maxArrayElements() {
        var limits = DefaultJsonCodecSettings.builder().maxArrayElements(3).build();
        var codec = new DefaultJsonCodec(limits);
        assertDoesNotThrow(() -> codec.parse("[1,2,3]"));
    }

    @Test
    void parse_rejects_duplicate_keys_by_default() {
        var codec = new DefaultJsonCodec(); // defaults: REJECT
        var ex = assertThrows(SQL4JsonExecutionException.class, () -> codec.parse("{\"a\":1,\"a\":2}"));
        assertTrue(ex.getMessage().toLowerCase().contains("duplicate"));
    }

    @Test
    void parse_last_wins_policy() {
        var limits = DefaultJsonCodecSettings.builder()
                .duplicateKeyPolicy(DuplicateKeyPolicy.LAST_WINS)
                .build();
        var codec = new DefaultJsonCodec(limits);
        var result = codec.parse("{\"a\":1,\"a\":2}");
        // verify 'a' = 2 (LAST_WINS)
        assertEquals("2", extractFieldAsString(result, "a"));
    }

    @Test
    void parse_first_wins_policy() {
        var limits = DefaultJsonCodecSettings.builder()
                .duplicateKeyPolicy(DuplicateKeyPolicy.FIRST_WINS)
                .build();
        var codec = new DefaultJsonCodec(limits);
        var result = codec.parse("{\"a\":1,\"a\":2}");
        assertEquals("1", extractFieldAsString(result, "a"));
    }

    @Test
    void custom_settings_enforced_on_streaming_path() {
        // maxInputLength(16) means any JSON longer than 16 chars must be rejected.
        var limits = DefaultJsonCodecSettings.builder().maxInputLength(16).build();
        var codec = new DefaultJsonCodec(limits);
        // This array is well beyond 16 characters.
        String largeJson = "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]";
        var settings = Sql4jsonSettings.builder().codec(codec).build();
        var ex = assertThrows(
                SQL4JsonExecutionException.class, () -> SQL4Json.query("SELECT id FROM $r", largeJson, settings));
        assertTrue(
                ex.getMessage().contains("input length exceeds configured maximum"),
                "Expected limit message but got: " + ex.getMessage());
    }

    // Helper: read a top-level numeric field from a JsonValue as string
    private static String extractFieldAsString(io.github.mnesimiyilmaz.sql4json.types.JsonValue v, String field) {
        return v.asObject()
                .orElseThrow()
                .get(field)
                .asNumber()
                .orElseThrow()
                .toString()
                .replaceAll("\\..*", "");
    }
}
