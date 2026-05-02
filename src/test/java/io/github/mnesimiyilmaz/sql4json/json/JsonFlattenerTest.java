// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for json/JsonFlattener — the NEW streaming flattener. Distinct from utils/JsonFlattenerTest (which tests the
 * old pipeline's JsonFlattener).
 */
class JsonFlattenerTest {

    // ── Helpers ──────────────────────────────────────────────────────────────

    /** [{name: "A"}, {name: "B"}] */
    private static JsonArrayValue twoElementArray() {
        return new JsonArrayValue(List.of(
                new JsonObjectValue(Map.of("name", new JsonStringValue("A"))),
                new JsonObjectValue(Map.of("name", new JsonStringValue("B")))));
    }

    /** {name: "Alice", age: 30} */
    private static JsonObjectValue simpleObj() {
        var m = new java.util.LinkedHashMap<String, JsonValue>();
        m.put("name", new JsonStringValue("Alice"));
        m.put("age", new JsonLongValue(30L));
        return new JsonObjectValue(m);
    }

    /** {data: {items: [{id: 1}, {id: 2}]}} */
    private static JsonObjectValue nestedObj() {
        return new JsonObjectValue(Map.of(
                "data",
                new JsonObjectValue(Map.of(
                        "items",
                        new JsonArrayValue(List.of(
                                new JsonObjectValue(Map.of("id", new JsonLongValue(1L))),
                                new JsonObjectValue(Map.of("id", new JsonLongValue(2L)))))))));
    }

    // ── streamLazy() ─────────────────────────────────────────────────────────

    @Test
    void streamLazy_arrayInput_producesOneRowPerElement() {
        FieldKey.Interner interner = new FieldKey.Interner();
        List<Row> rows =
                JsonFlattener.streamLazy(twoElementArray(), "$r", interner).toList();
        assertEquals(2, rows.size());
    }

    @Test
    void streamLazy_objectInput_producesSingleRow() {
        FieldKey.Interner interner = new FieldKey.Interner();
        List<Row> rows = JsonFlattener.streamLazy(simpleObj(), "$r", interner).toList();
        assertEquals(1, rows.size());
    }

    @Test
    void streamLazy_eachRow_holdsOriginalJsonValue() {
        FieldKey.Interner interner = new FieldKey.Interner();
        List<Row> rows =
                JsonFlattener.streamLazy(twoElementArray(), "$r", interner).toList();
        // Every lazy row must have original reference (not pre-flattened)
        rows.forEach(r -> assertTrue(r.originalValue().isPresent(), "Each lazy row should hold original JsonValue"));
    }

    @Test
    void streamLazy_laziness_originalValuePreservedNotPreFlattened() {
        FieldKey.Interner interner = new FieldKey.Interner();
        List<Row> rows =
                JsonFlattener.streamLazy(twoElementArray(), "$r", interner).toList();
        // Each row holds original reference and is not modified (not pre-flattened)
        rows.forEach(r -> {
            assertTrue(r.originalValue().isPresent(), "Lazy row should preserve original JsonValue reference");
            assertFalse(r.isModified(), "Lazy row should not be marked as modified");
        });
    }

    @Test
    void streamLazy_emptyArray_producesEmptyStream() {
        FieldKey.Interner interner = new FieldKey.Interner();
        long count = JsonFlattener.streamLazy(new JsonArrayValue(List.of()), "$r", interner)
                .count();
        assertEquals(0, count);
    }

    @Test
    void streamLazy_navigatesToPath() {
        FieldKey.Interner interner = new FieldKey.Interner();
        // nestedObj has data.items = [{id:1},{id:2}]
        List<Row> rows =
                JsonFlattener.streamLazy(nestedObj(), "$r.data.items", interner).toList();
        assertEquals(2, rows.size());
    }

    // ── flattenInto() ─────────────────────────────────────────────────────────

    @Test
    void flattenInto_topLevelFields() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Map<FieldKey, SqlValue> target = new HashMap<>();
        JsonFlattener.flattenInto(simpleObj(), target, interner);

        assertEquals(2, target.size());
        assertEquals(new SqlString("Alice"), target.get(FieldKey.of("name", interner)));
        assertEquals(SqlNumber.of(30L), target.get(FieldKey.of("age", interner)));
    }

    @Test
    void flattenInto_nestedObject_dotSeparatedKeys() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Map<FieldKey, SqlValue> target = new HashMap<>();
        JsonFlattener.flattenInto(
                new JsonObjectValue(Map.of("address", new JsonObjectValue(Map.of("city", new JsonStringValue("LA"))))),
                target,
                interner);

        assertTrue(target.containsKey(FieldKey.of("address.city", interner)));
        assertEquals(new SqlString("LA"), target.get(FieldKey.of("address.city", interner)));
    }

    @Test
    void flattenInto_arrayField_indexedKeys() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Map<FieldKey, SqlValue> target = new HashMap<>();
        JsonFlattener.flattenInto(
                new JsonObjectValue(Map.of(
                        "tags", new JsonArrayValue(List.of(new JsonStringValue("a"), new JsonStringValue("b"))))),
                target,
                interner);

        assertTrue(target.containsKey(FieldKey.of("tags[0]", interner)));
        assertTrue(target.containsKey(FieldKey.of("tags[1]", interner)));
        assertEquals(new SqlString("a"), target.get(FieldKey.of("tags[0]", interner)));
    }

    // ── navigateToPath() ─────────────────────────────────────────────────────

    @Test
    void navigateToPath_root_returnsRoot() {
        JsonObjectValue root = simpleObj();
        assertSame(root, JsonFlattener.navigateToPath(root, "$r"));
    }

    @Test
    void navigateToPath_null_returnsRoot() {
        JsonObjectValue root = simpleObj();
        assertSame(root, JsonFlattener.navigateToPath(root, null));
    }

    @Test
    void navigateToPath_nestedPath() {
        JsonObjectValue root = nestedObj();
        JsonValue items = JsonFlattener.navigateToPath(root, "$r.data.items");
        assertInstanceOf(JsonArrayValue.class, items);
        assertEquals(2, ((JsonArrayValue) items).elements().size());
    }

    @Test
    void navigateToPath_nonexistent_returnsJsonNull() {
        JsonValue result = JsonFlattener.navigateToPath(simpleObj(), "$r.nonexistent");
        assertSame(JsonNullValue.INSTANCE, result);
    }
}
