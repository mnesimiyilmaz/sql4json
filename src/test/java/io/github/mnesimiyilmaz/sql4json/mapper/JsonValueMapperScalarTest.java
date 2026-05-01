package io.github.mnesimiyilmaz.sql4json.mapper;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException;
import io.github.mnesimiyilmaz.sql4json.json.JsonNullValue;
import io.github.mnesimiyilmaz.sql4json.settings.MappingSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class JsonValueMapperScalarTest {

    private static final MappingSettings S = MappingSettings.defaults();

    enum Color {RED, GREEN, BLUE}

    @Test
    void when_null_mapped_to_reference_type_then_null() {
        JsonValue v = new JsonNullValue();
        assertNull(JsonValueMapper.INSTANCE.map(v, String.class, S));
    }

    @Test
    void when_null_mapped_to_optional_then_empty() {
        JsonValue v = new JsonNullValue();
        Optional<?> result = JsonValueMapper.INSTANCE.map(v, Optional.class, S);
        assertTrue(result.isEmpty());
    }

    @Test
    void when_null_mapped_to_primitive_wrapper_then_null() {
        JsonValue v = new JsonNullValue();
        assertNull(JsonValueMapper.INSTANCE.map(v, Integer.class, S));
    }

    @Test
    void when_json_boolean_mapped_to_boolean_then_unboxed() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonBooleanValue(true);
        assertEquals(Boolean.TRUE, JsonValueMapper.INSTANCE.map(v, Boolean.class, S));
    }

    @Test
    void when_json_boolean_mapped_to_string_then_lowercase_string() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonBooleanValue(false);
        assertEquals("false", JsonValueMapper.INSTANCE.map(v, String.class, S));
    }

    @Test
    void when_json_boolean_mapped_to_int_then_exception() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonBooleanValue(true);
        assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, Integer.class, S));
    }

    @Test
    void when_json_number_mapped_to_primitive_int_wrapper() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(42L);
        assertEquals(Integer.valueOf(42), JsonValueMapper.INSTANCE.map(v, Integer.class, S));
        assertEquals(Long.valueOf(42L), JsonValueMapper.INSTANCE.map(v, Long.class, S));
        assertEquals(Double.valueOf(42.0), JsonValueMapper.INSTANCE.map(v, Double.class, S));
    }

    @Test
    void when_json_number_mapped_to_byte() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(42L);
        assertEquals(Byte.valueOf((byte) 42), JsonValueMapper.INSTANCE.map(v, Byte.class, S));
    }

    @Test
    void when_json_number_mapped_to_short() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(1000L);
        assertEquals(Short.valueOf((short) 1000), JsonValueMapper.INSTANCE.map(v, Short.class, S));
    }

    @Test
    void when_json_number_mapped_to_float() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonDoubleValue(3.14);
        assertEquals(Float.valueOf(3.14f), JsonValueMapper.INSTANCE.map(v, Float.class, S));
    }

    @Test
    void when_json_number_mapped_to_character() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(65L);
        assertEquals(Character.valueOf('A'), JsonValueMapper.INSTANCE.map(v, Character.class, S));
    }

    @Test
    void when_json_number_mapped_to_number_passthrough() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(99L);
        assertEquals(99L, ((Number) JsonValueMapper.INSTANCE.map(v, Number.class, S)).longValue());
    }

    @Test
    void when_json_number_mapped_to_object_passthrough() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(7L);
        Object out = JsonValueMapper.INSTANCE.map(v, Object.class, S);
        assertEquals(7, ((Number) out).intValue());
    }

    @Test
    void when_json_string_mapped_to_charSequence() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("hi");
        CharSequence out = JsonValueMapper.INSTANCE.map(v, CharSequence.class, S);
        assertEquals("hi", out.toString());
    }

    @Test
    void when_json_number_non_integer_mapped_to_big_integer_throws() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonDoubleValue(3.14);
        assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, java.math.BigInteger.class, S));
    }

    @Test
    void when_json_number_mapped_to_unsupported_type_throws() {
        // java.util.UUID has no number conversion path
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(7L);
        assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, java.util.UUID.class, S));
    }

    @Test
    void when_json_string_mapped_to_unsupported_type_throws() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("foo");
        assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, java.util.UUID.class, S));
    }

    @Test
    void when_invalid_iso_date_then_exception() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("not-a-date");
        assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, java.time.LocalDate.class, S));
    }

    @Test
    void when_invalid_iso_datetime_then_exception() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("not-a-datetime");
        assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, java.time.LocalDateTime.class, S));
    }

    @Test
    void when_invalid_iso_instant_then_exception() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("not-an-instant");
        assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, java.time.Instant.class, S));
    }

    @Test
    void when_invalid_string_to_big_decimal_then_exception() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("not a number");
        assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, java.math.BigDecimal.class, S));
    }

    @Test
    void when_string_to_big_integer_valid() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("42");
        assertEquals(new java.math.BigInteger("42"),
                JsonValueMapper.INSTANCE.map(v, java.math.BigInteger.class, S));
    }

    @Test
    void when_invalid_string_to_big_integer_then_exception() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("3.14");
        assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, java.math.BigInteger.class, S));
    }

    @Test
    void when_json_boolean_mapped_to_charSequence() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonBooleanValue(true);
        CharSequence out = JsonValueMapper.INSTANCE.map(v, CharSequence.class, S);
        assertEquals("true", out.toString());
    }

    @Test
    void when_json_boolean_mapped_to_object_passthrough() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonBooleanValue(false);
        Object out = JsonValueMapper.INSTANCE.map(v, Object.class, S);
        assertEquals(Boolean.FALSE, out);
    }

    @Test
    void when_json_number_mapped_to_big_decimal_lossless() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(Long.MAX_VALUE);
        assertEquals(new java.math.BigDecimal(String.valueOf(Long.MAX_VALUE)),
                JsonValueMapper.INSTANCE.map(v, java.math.BigDecimal.class, S));
    }

    @Test
    void when_json_number_mapped_to_big_integer_integer_valued_ok() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(1234567890L);
        assertEquals(new java.math.BigInteger("1234567890"),
                JsonValueMapper.INSTANCE.map(v, java.math.BigInteger.class, S));
    }

    @Test
    void when_json_number_mapped_to_instant_epoch_millis() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(1_700_000_000_000L);
        assertEquals(java.time.Instant.ofEpochMilli(1_700_000_000_000L),
                JsonValueMapper.INSTANCE.map(v, java.time.Instant.class, S));
    }

    @Test
    void when_json_number_mapped_to_string_then_to_string() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonDoubleValue(3.14);
        assertEquals("3.14", JsonValueMapper.INSTANCE.map(v, String.class, S));
    }

    @Test
    void when_json_string_mapped_to_string() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("hello");
        assertEquals("hello", JsonValueMapper.INSTANCE.map(v, String.class, S));
    }

    @Test
    void when_json_string_mapped_to_enum_valid_constant() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("RED");
        assertEquals(Color.RED, JsonValueMapper.INSTANCE.map(v, Color.class, S));
    }

    @Test
    void when_json_string_mapped_to_enum_invalid_constant_then_exception_lists_available() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("PURPLE");
        SQL4JsonMappingException e = assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, Color.class, S));
        assertTrue(e.getMessage().contains("PURPLE"));
        assertTrue(e.getMessage().contains("RED"));
    }

    @Test
    void when_iso_date_string_mapped_to_local_date() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("2026-04-21");
        assertEquals(java.time.LocalDate.of(2026, 4, 21),
                JsonValueMapper.INSTANCE.map(v, java.time.LocalDate.class, S));
    }

    @Test
    void when_iso_datetime_string_mapped_to_local_datetime() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("2026-04-21T10:15:30");
        assertEquals(java.time.LocalDateTime.of(2026, 4, 21, 10, 15, 30),
                JsonValueMapper.INSTANCE.map(v, java.time.LocalDateTime.class, S));
    }

    @Test
    void when_iso_instant_string_mapped_to_instant() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("2026-04-21T10:15:30Z");
        assertEquals(java.time.Instant.parse("2026-04-21T10:15:30Z"),
                JsonValueMapper.INSTANCE.map(v, java.time.Instant.class, S));
    }

    @Test
    void when_numeric_string_mapped_to_big_decimal() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("12345.678");
        assertEquals(new java.math.BigDecimal("12345.678"),
                JsonValueMapper.INSTANCE.map(v, java.math.BigDecimal.class, S));
    }

    @Test
    void when_bad_iso_date_string_then_exception_includes_input() {
        JsonValue v = new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("21/04/2026");
        SQL4JsonMappingException e = assertThrows(SQL4JsonMappingException.class,
                () -> JsonValueMapper.INSTANCE.map(v, java.time.LocalDate.class, S));
        assertTrue(e.getMessage().contains("21/04/2026"));
    }
}
