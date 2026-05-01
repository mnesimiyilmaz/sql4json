package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonUnflattenerTest {

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static JsonObjectValue personObj(String name, int age) {
        var fields = new LinkedHashMap<String, JsonValue>();
        fields.put("name", new JsonStringValue(name));
        fields.put("age", new JsonLongValue(age));
        return new JsonObjectValue(fields);
    }

    private static Row lazyRow(JsonObjectValue obj) {
        return Row.lazy(obj, new FieldKey.Interner());
    }

    private static Row eagerRow(Map<FieldKey, SqlValue> data) {
        return Row.eager(data);
    }

    // ── SELECT * — cherry-pick: return original ───────────────────────────────

    @Test
    void selectAll_singleRow_returnsOriginalObject() {
        JsonObjectValue original = personObj("Alice", 30);
        Row row = lazyRow(original);

        JsonValue result = JsonUnflattener.unflatten(
                List.of(row), List.of(SelectColumnDef.asterisk()));

        assertInstanceOf(JsonArrayValue.class, result);
        List<JsonValue> arr = ((JsonArrayValue) result).elements();
        assertEquals(1, arr.size());
        // Cherry-pick SELECT * returns the original object
        assertSame(original, arr.get(0));
    }

    @Test
    void selectAll_multipleRows_returnsAllOriginals() {
        JsonObjectValue obj1 = personObj("Alice", 30);
        JsonObjectValue obj2 = personObj("Bob", 25);

        JsonValue result = JsonUnflattener.unflatten(
                List.of(lazyRow(obj1), lazyRow(obj2)),
                List.of(SelectColumnDef.asterisk()));

        assertEquals(2, ((JsonArrayValue) result).elements().size());
        assertSame(obj1, ((JsonArrayValue) result).elements().get(0));
        assertSame(obj2, ((JsonArrayValue) result).elements().get(1));
    }

    // ── SELECT specific columns — cherry-pick: navigate original ─────────────

    @Test
    void selectSpecificColumn_returnsOnlySelectedField() {
        JsonObjectValue original = personObj("Alice", 30);
        Row row = lazyRow(original);

        JsonValue result = JsonUnflattener.unflatten(
                List.of(row), List.of(SelectColumnDef.column("name")));

        JsonArrayValue arr = (JsonArrayValue) result;
        JsonObjectValue resultObj = (JsonObjectValue) arr.elements().get(0);
        assertTrue(resultObj.fields().containsKey("name"));
        assertFalse(resultObj.fields().containsKey("age"),
                "age should NOT be in result when only name is selected");
        assertEquals("Alice", ((JsonStringValue) resultObj.fields().get("name")).value());
    }

    @Test
    void selectWithAlias_outputFieldHasAliasName() {
        JsonObjectValue original = personObj("Alice", 30);
        Row row = lazyRow(original);

        JsonValue result = JsonUnflattener.unflatten(
                List.of(row), List.of(SelectColumnDef.column("name", "personName")));

        JsonObjectValue resultObj = (JsonObjectValue) ((JsonArrayValue) result).elements().get(0);
        assertTrue(resultObj.fields().containsKey("personName"),
                "Alias 'personName' should be the output key");
        assertFalse(resultObj.fields().containsKey("name"),
                "Original 'name' should NOT appear when alias is set");
    }

    @Test
    void selectMultipleColumns_returnsOnlySelectedFields() {
        var fields = new LinkedHashMap<String, JsonValue>();
        fields.put("name", new JsonStringValue("Charlie"));
        fields.put("age", new JsonLongValue(40L));
        fields.put("city", new JsonStringValue("LA"));
        JsonObjectValue original = new JsonObjectValue(fields);

        Row row = lazyRow(original);

        JsonValue result = JsonUnflattener.unflatten(
                List.of(row),
                List.of(SelectColumnDef.column("name"), SelectColumnDef.column("age")));

        JsonObjectValue resultObj = (JsonObjectValue) ((JsonArrayValue) result).elements().get(0);
        assertEquals(2, resultObj.fields().size());
        assertTrue(resultObj.fields().containsKey("name"));
        assertTrue(resultObj.fields().containsKey("age"));
        assertFalse(resultObj.fields().containsKey("city"));
    }

    @Test
    void selectNestedColumn_cherryPickNavigatesDotPath() {
        var address = new JsonObjectValue(java.util.Map.of(
                "city", new JsonStringValue("LA"),
                "zip", new JsonStringValue("90001")));
        var root = new JsonObjectValue(java.util.Map.of("address", address));
        Row row = lazyRow(root);

        JsonValue result = JsonUnflattener.unflatten(
                List.of(row), List.of(SelectColumnDef.column("address.city")));

        JsonObjectValue resultObj = (JsonObjectValue) ((JsonArrayValue) result).elements().get(0);
        assertTrue(resultObj.fields().containsKey("address.city"),
                "Column 'address.city' should appear in output");
        assertEquals("LA",
                ((JsonStringValue) resultObj.fields().get("address.city")).value(),
                "navigateToField should follow dot-separated path");
    }

    // ── Reconstruct path — modified (aggregated) rows ────────────────────────

    @Test
    void reconstruct_modifiedRow_buildsFromFlatMap() {
        FieldKey deptKey = FieldKey.of("dept");
        FieldKey countKey = FieldKey.of("cnt");
        Row row = eagerRow(Map.of(
                deptKey, new SqlString("Engineering"),
                countKey, SqlNumber.of(15)));

        JsonValue result = JsonUnflattener.unflatten(
                List.of(row),
                List.of(SelectColumnDef.column("dept"), SelectColumnDef.column("cnt")));

        JsonArrayValue arr = (JsonArrayValue) result;
        assertEquals(1, arr.elements().size());
        JsonObjectValue resultObj = (JsonObjectValue) arr.elements().get(0);
        assertEquals("Engineering",
                ((JsonStringValue) resultObj.fields().get("dept")).value());
        assertEquals(15.0,
                ((JsonNumberValue) resultObj.fields().get("cnt")).numberValue().doubleValue(), 1e-10);
    }

    @Test
    void reconstruct_selectAll_dumpsAllFields() {
        FieldKey k1 = FieldKey.of("x");
        FieldKey k2 = FieldKey.of("y");
        Row row = eagerRow(Map.of(k1, new SqlString("a"), k2, SqlNumber.of(1)));

        JsonValue result = JsonUnflattener.unflatten(
                List.of(row), List.of(SelectColumnDef.asterisk()));

        JsonObjectValue resultObj = (JsonObjectValue) ((JsonArrayValue) result).elements().get(0);
        assertEquals(2, resultObj.fields().size());
    }

    // ── Empty result set ──────────────────────────────────────────────────────

    @Test
    void emptyRowList_returnsEmptyArray() {
        JsonValue result = JsonUnflattener.unflatten(
                List.of(), List.of(SelectColumnDef.asterisk()));
        assertInstanceOf(JsonArrayValue.class, result);
        assertEquals(0, ((JsonArrayValue) result).elements().size());
    }

    // ── cherryPick: null functionRegistry, non-ColumnRef expression ─────────

    @Test
    void cherryPick_nullRegistry_scalarFn_fallsBackToInnermostColumn() {
        // When functionRegistry is null and expression is ScalarFnCall,
        // JsonUnflattener falls back to innermost column path
        var original = personObj("Alice", 30);
        Row row = lazyRow(original);

        // ScalarFnCall("upper", [ColumnRef("name")]) without registry → navigate by columnName()
        var expr = new io.github.mnesimiyilmaz.sql4json.engine.Expression.ScalarFnCall(
                "upper", List.of(new io.github.mnesimiyilmaz.sql4json.engine.Expression.ColumnRef("name")));
        var col = SelectColumnDef.of(expr, "upper_name");

        JsonValue result = JsonUnflattener.unflatten(
                List.of(row), List.of(col));

        JsonObjectValue resultObj = (JsonObjectValue) ((JsonArrayValue) result).elements().get(0);
        assertTrue(resultObj.fields().containsKey("upper_name"));
    }

    @Test
    void cherryPick_nullRegistry_literalExpr_noColumnPath_returnsNull() {
        // When functionRegistry is null and expression has no column path → JsonNullValue
        var original = personObj("Alice", 30);
        Row row = lazyRow(original);

        var expr = new io.github.mnesimiyilmaz.sql4json.engine.Expression.LiteralVal(SqlNumber.of(42));
        var col = SelectColumnDef.of(expr, "const");

        JsonValue result = JsonUnflattener.unflatten(
                List.of(row), List.of(col));

        JsonObjectValue resultObj = (JsonObjectValue) ((JsonArrayValue) result).elements().get(0);
        assertTrue(resultObj.fields().containsKey("const"));
        assertTrue(resultObj.fields().get("const").isNull());
    }

    // ── Decision: modified=true → reconstruct even with original present ──────

    @Test
    void modifiedFlag_forcesReconstructPath() {
        // Row.eager() sets modified=true. Even if we somehow had an original,
        // the modified flag means reconstruct is used.
        FieldKey nameKey = FieldKey.of("name");
        Row modifiedRow = eagerRow(Map.of(nameKey, new SqlString("Modified")));
        assertTrue(modifiedRow.isModified());

        JsonValue result = JsonUnflattener.unflatten(
                List.of(modifiedRow), List.of(SelectColumnDef.column("name")));

        JsonObjectValue obj = (JsonObjectValue) ((JsonArrayValue) result).elements().get(0);
        assertEquals("Modified", ((JsonStringValue) obj.fields().get("name")).value());
    }
}
