package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.json.*;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ArrayPathNavigatorTest {

    private final FieldKey.Interner interner = new FieldKey.Interner();

    @Test
    void original_navigation_returns_JsonArrayValue_for_top_level_array_field() {
        JsonValue obj = new JsonObjectValue(Map.of(
                "tags", new JsonArrayValue(List.of(
                        new JsonStringValue("admin"),
                        new JsonStringValue("editor")))));
        Row row = Row.lazy(obj, interner);

        JsonArrayValue arr = ArrayPathNavigator.navigateToArray(row, "tags");

        assertNotNull(arr);
        assertEquals(2, arr.elements().size());
    }

    @Test
    void original_navigation_returns_null_for_missing_field() {
        JsonValue obj = new JsonObjectValue(Map.of("name", new JsonStringValue("a")));
        Row row = Row.lazy(obj, interner);

        assertNull(ArrayPathNavigator.navigateToArray(row, "tags"));
    }

    @Test
    void original_navigation_returns_null_for_explicit_json_null() {
        JsonValue obj = new JsonObjectValue(Map.of("tags", JsonNullValue.INSTANCE));
        Row row = Row.lazy(obj, interner);

        assertNull(ArrayPathNavigator.navigateToArray(row, "tags"));
    }

    @Test
    void original_navigation_returns_null_for_scalar_field() {
        JsonValue obj = new JsonObjectValue(Map.of("tags", new JsonStringValue("admin")));
        Row row = Row.lazy(obj, interner);

        assertNull(ArrayPathNavigator.navigateToArray(row, "tags"));
    }

    @Test
    void original_navigation_walks_nested_path() {
        JsonValue obj = new JsonObjectValue(Map.of(
                "user", new JsonObjectValue(Map.of(
                        "tags", new JsonArrayValue(List.of(new JsonStringValue("x")))))));
        Row row = Row.lazy(obj, interner);

        JsonArrayValue arr = ArrayPathNavigator.navigateToArray(row, "user.tags");
        assertNotNull(arr);
        assertEquals(1, arr.elements().size());
    }

    @Test
    void original_navigation_returns_null_for_intermediate_non_object() {
        // path "user.tags" but "user" is a scalar — must not throw, just return null
        JsonValue obj = new JsonObjectValue(Map.of("user", new JsonStringValue("alice")));
        Row row = Row.lazy(obj, interner);

        assertNull(ArrayPathNavigator.navigateToArray(row, "user.tags"));
    }

    @Test
    void numeric_array_navigation_preserves_types() {
        JsonValue obj = new JsonObjectValue(Map.of(
                "scores", new JsonArrayValue(List.of(
                        new JsonLongValue(1L),
                        new JsonLongValue(2L)))));
        Row row = Row.lazy(obj, interner);

        JsonArrayValue arr = ArrayPathNavigator.navigateToArray(row, "scores");
        assertNotNull(arr);
        assertEquals(2, arr.elements().size());
        assertTrue(arr.elements().get(0) instanceof JsonNumberValue);
    }

    @Test
    void indexed_path_returns_null() {
        JsonValue obj = new JsonObjectValue(Map.of(
                "tags", new JsonArrayValue(List.of(new JsonStringValue("admin")))));
        Row row = Row.lazy(obj, interner);

        assertNull(ArrayPathNavigator.navigateToArray(row, "tags[0]"));
    }

    @Test
    void flat_key_reassembly_used_when_row_has_no_original() {
        Map<FieldKey, SqlValue> flat = new LinkedHashMap<>();
        flat.put(interner.internFieldKey("tags[0]"), new SqlString("admin"));
        flat.put(interner.internFieldKey("tags[1]"), new SqlString("editor"));
        Row row = Row.eager(flat);

        JsonArrayValue arr = ArrayPathNavigator.navigateToArray(row, "tags");
        assertNotNull(arr);
        assertEquals(2, arr.elements().size());
        assertEquals("admin", ((JsonStringValue) arr.elements().get(0)).value());
        assertEquals("editor", ((JsonStringValue) arr.elements().get(1)).value());
    }

    @Test
    void flat_key_reassembly_returns_null_when_no_matching_family() {
        Map<FieldKey, SqlValue> flat = new LinkedHashMap<>();
        flat.put(interner.internFieldKey("name"), new SqlString("x"));
        Row row = Row.eager(flat);

        assertNull(ArrayPathNavigator.navigateToArray(row, "tags"));
    }

    @Test
    void flat_key_reassembly_with_numeric_array() {
        Map<FieldKey, SqlValue> flat = new LinkedHashMap<>();
        flat.put(interner.internFieldKey("scores[0]"), SqlNumber.of(BigDecimal.valueOf(10)));
        flat.put(interner.internFieldKey("scores[1]"), SqlNumber.of(BigDecimal.valueOf(20)));
        Row row = Row.eager(flat);

        JsonArrayValue arr = ArrayPathNavigator.navigateToArray(row, "scores");
        assertNotNull(arr);
        assertEquals(2, arr.elements().size());
        assertTrue(arr.elements().get(0) instanceof JsonNumberValue);
    }
}
