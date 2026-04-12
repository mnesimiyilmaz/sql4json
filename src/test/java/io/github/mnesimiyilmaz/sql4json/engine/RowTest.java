package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.json.JsonNumberValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RowTest {

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Two-field JSON object: {name: "Alice", age: 30}
     */
    private static JsonObjectValue twoFieldObj() {
        var fields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        fields.put("name", new JsonStringValue("Alice"));
        fields.put("age", new JsonNumberValue(30));
        return new JsonObjectValue(fields);
    }

    /**
     * Three-field JSON object: {name: "Bob", city: "NYC", score: 95}
     */
    private static JsonObjectValue threeFieldObj() {
        var fields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        fields.put("name", new JsonStringValue("Bob"));
        fields.put("city", new JsonStringValue("NYC"));
        fields.put("score", new JsonNumberValue(95));
        return new JsonObjectValue(fields);
    }

    // ── lazy() ────────────────────────────────────────────────────────────────

    @Test
    void lazy_originalValuePresent() {
        JsonObjectValue obj = twoFieldObj();
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(obj, interner);

        assertTrue(row.originalValue().isPresent());
        assertSame(obj, row.originalValue().get());
    }

    @Test
    void lazy_isNotModified() {
        Row row = Row.lazy(twoFieldObj(), new FieldKey.Interner());
        assertFalse(row.isModified());
    }

    @Test
    void lazy_cacheIsEmptyInitially() {
        Row row = Row.lazy(threeFieldObj(), new FieldKey.Interner());
        assertEquals(0, row.cachedFieldCount());
    }

    // ── get() — lazy resolution ───────────────────────────────────────────────

    @Test
    void get_existingField_returnsCorrectValue() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(twoFieldObj(), interner);

        FieldKey nameKey = FieldKey.of("name", interner);
        SqlValue result = row.get(nameKey);
        assertInstanceOf(SqlString.class, result);
        assertEquals("Alice", ((SqlString) result).value());
    }

    @Test
    void get_populatesCache_onFirstAccess() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(threeFieldObj(), interner);

        // Before any get, cache is empty
        assertEquals(0, row.cachedFieldCount());

        // After getting one field, only one entry in cache
        row.get(FieldKey.of("name", interner));
        assertEquals(1, row.cachedFieldCount());

        // Getting another field adds to cache
        row.get(FieldKey.of("city", interner));
        assertEquals(2, row.cachedFieldCount());
    }

    @Test
    void get_absentField_returnsSqlNull() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(twoFieldObj(), interner);

        SqlValue result = row.get(FieldKey.of("nonexistent", interner));
        assertSame(SqlNull.INSTANCE, result);
    }

    @Test
    void get_cachedField_returnsSameInstance() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(twoFieldObj(), interner);

        FieldKey nameKey = FieldKey.of("name", interner);
        SqlValue first = row.get(nameKey);
        SqlValue second = row.get(nameKey);
        // Same reference from cache
        assertSame(first, second);
    }

    // ── ensureFullyFlattened() ────────────────────────────────────────────────

    @Test
    void ensureFullyFlattened_populatesAllFields() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(threeFieldObj(), interner);

        // Before flatten, cache empty
        assertEquals(0, row.cachedFieldCount());

        row.ensureFullyFlattened();

        // After flatten, all 3 fields in cache
        assertEquals(3, row.cachedFieldCount());
    }

    @Test
    void ensureFullyFlattened_calledTwice_idempotent() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(twoFieldObj(), interner);
        row.ensureFullyFlattened();
        int count = row.cachedFieldCount();
        row.ensureFullyFlattened(); // second call
        assertEquals(count, row.cachedFieldCount());
    }

    @Test
    void keys_triggersFullFlatten() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(threeFieldObj(), interner);
        Set<FieldKey> keys = row.keys();
        assertEquals(3, keys.size());
    }

    // ── eager() ───────────────────────────────────────────────────────────────

    @Test
    void eager_originalValueAbsent() {
        FieldKey nameKey = FieldKey.of("name");
        Row row = Row.eager(Map.of(nameKey, new SqlString("Charlie")));
        assertTrue(row.originalValue().isEmpty());
    }

    @Test
    void eager_isModified() {
        Row row = Row.eager(Map.of(FieldKey.of("x"), SqlNumber.of(1)));
        assertTrue(row.isModified());
    }

    @Test
    void eager_get_returnsValueFromMap() {
        FieldKey ageKey = FieldKey.of("age");
        Row row = Row.eager(Map.of(ageKey, SqlNumber.of(42)));
        SqlValue result = row.get(ageKey);
        assertInstanceOf(SqlNumber.class, result);
        assertEquals(42.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void eager_get_absentKey_returnsSqlNull() {
        Row row = Row.eager(Map.of(FieldKey.of("x"), SqlNull.INSTANCE));
        assertSame(SqlNull.INSTANCE, row.get(FieldKey.of("missing")));
    }

    // ── project() ─────────────────────────────────────────────────────────────

    @Test
    void project_returnsRowWithOnlyRequestedFields() {
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(threeFieldObj(), interner);

        FieldKey nameKey = FieldKey.of("name", interner);
        Row projected = row.project(Set.of(nameKey));

        // Only requested field
        assertInstanceOf(SqlString.class, projected.get(nameKey));
        assertSame(SqlNull.INSTANCE, projected.get(FieldKey.of("city", interner)));
    }

    @Test
    void project_preservesOriginalReference() {
        FieldKey.Interner interner = new FieldKey.Interner();
        JsonObjectValue obj = twoFieldObj();
        Row row = Row.lazy(obj, interner);
        Row projected = row.project(Set.of(FieldKey.of("name", interner)));
        // Original reference preserved for cherry-pick unflatten
        assertTrue(projected.originalValue().isPresent());
    }

    // ── navigateAndConvert: null propagation ──────────────────────────────────

    @Test
    void get_missingIntermediateSegment_returnsSqlNull() {
        // Verifies that JsonNullValue.asObject() returns Optional.empty()
        // and the navigation short-circuits correctly at each missing step.
        FieldKey.Interner interner = new FieldKey.Interner();
        // obj has no "address" field, so "address.city" path should return SqlNull
        Row row = Row.lazy(twoFieldObj(), interner);
        SqlValue result = row.get(FieldKey.of("address.city", interner));
        assertSame(SqlNull.INSTANCE, result,
                "Missing intermediate path segment should return SqlNull");
    }

    @Test
    void get_arrayIndexOutOfBounds_returnsSqlNull() {
        FieldKey.Interner interner = new FieldKey.Interner();
        // obj has "name" (string), not an array — accessing [0] should return SqlNull
        Row row = Row.lazy(twoFieldObj(), interner);
        SqlValue result = row.get(FieldKey.of("name[0]", interner));
        assertSame(SqlNull.INSTANCE, result,
                "Array index on non-array field should return SqlNull");
    }

    @Test
    void get_arrayIndexTrulyOutOfBounds_returnsSqlNull() {
        // Build: {tags: ["a", "b"]} and access tags[99]
        FieldKey.Interner interner = new FieldKey.Interner();
        var fields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        fields.put("tags", new io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue(
                List.of(
                        new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("a"),
                        new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("b")
                )));
        Row row = Row.lazy(new io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue(fields), interner);
        SqlValue result = row.get(FieldKey.of("tags[99]", interner));
        assertSame(SqlNull.INSTANCE, result, "True out-of-bounds array index should return SqlNull");
    }

    // ── putWindowResult() / window result storage ─────────────────────────────

    @Test
    void putWindowResult_is_returned_by_get() {
        Row row = Row.eager(Map.of(FieldKey.of("name"), new SqlString("Alice")));
        FieldKey winKey = FieldKey.of("row_num");

        row.putWindowResult(winKey, SqlNumber.of(1));

        assertEquals(SqlNumber.of(1), row.get(winKey));
    }

    @Test
    void window_result_takes_precedence_over_cache() {
        Row row = Row.eager(Map.of(FieldKey.of("val"), SqlNumber.of(100)));
        FieldKey key = FieldKey.of("val");

        row.putWindowResult(key, SqlNumber.of(999));

        assertEquals(SqlNumber.of(999), row.get(key));
    }

    @Test
    void get_returns_null_for_absent_window_key() {
        Row row = Row.eager(Map.of(FieldKey.of("name"), new SqlString("Alice")));

        assertEquals(SqlNull.INSTANCE, row.get(FieldKey.of("no_such_window")));
    }

    @Test
    void project_preserves_window_results() {
        Row row = Row.eager(Map.of(
                FieldKey.of("name"), new SqlString("Alice"),
                FieldKey.of("salary"), SqlNumber.of(90000)));
        row.putWindowResult(FieldKey.of("row_num"), SqlNumber.of(1));

        Row projected = row.project(Set.of(FieldKey.of("name"), FieldKey.of("row_num")));

        assertEquals(new SqlString("Alice"), projected.get(FieldKey.of("name")));
        assertEquals(SqlNumber.of(1), projected.get(FieldKey.of("row_num")));
    }
}
