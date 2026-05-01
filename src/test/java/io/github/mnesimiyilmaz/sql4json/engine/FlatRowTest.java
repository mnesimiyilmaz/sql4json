package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FlatRowTest {

    private static final RowSchema SCHEMA = RowSchema.of(List.of(
            FieldKey.of("name"),
            FieldKey.of("age"),
            FieldKey.of("city")));

    @Test
    void getByOrdinal_returnsValue() {
        Object[] vals = { new SqlString("Alice"), SqlNumber.of(30L), null };
        FlatRow r = FlatRow.of(SCHEMA, vals);
        assertEquals("Alice", ((SqlString) r.get(0)).value());
        assertEquals(30L, ((SqlNumber) r.get(1)).longValue());
    }

    @Test
    void nullSlot_decodesToSqlNull() {
        Object[] vals = { null, null, null };
        FlatRow r = FlatRow.of(SCHEMA, vals);
        SqlValue v = r.get(0);
        assertSame(SqlNull.INSTANCE, v);
    }

    @Test
    void getByFieldKey_resolvesViaSchema() {
        Object[] vals = { new SqlString("Alice"), null, new SqlString("NYC") };
        FlatRow r = FlatRow.of(SCHEMA, vals);
        assertEquals("NYC", ((SqlString) r.get(FieldKey.of("city"))).value());
    }

    @Test
    void getByFieldKey_absent_returnsSqlNull() {
        Object[] vals = { null, null, null };
        FlatRow r = FlatRow.of(SCHEMA, vals);
        assertSame(SqlNull.INSTANCE, r.get(FieldKey.of("missing")));
    }

    @Test
    void valuesByFamily_returnsOrdinalSubset() {
        RowSchema s = RowSchema.of(List.of(
                FieldKey.of("items[0].name"),
                FieldKey.of("items[1].name"),
                FieldKey.of("city")));
        Object[] vals = { new SqlString("a"), new SqlString("b"), new SqlString("NYC") };
        FlatRow r = FlatRow.of(s, vals);
        var fam = r.valuesByFamily("items.name");
        assertEquals(2, fam.size());
    }

    @Test
    void valuesByFamily_returnsEmptyListForUnknownFamily() {
        FlatRow r = FlatRow.of(SCHEMA, new Object[3]);
        assertTrue(r.valuesByFamily("missing").isEmpty());
    }

    @Test
    void originalValue_alwaysEmpty_forBareFlatRow() {
        FlatRow r = FlatRow.of(SCHEMA, new Object[3]);
        assertTrue(r.originalValue().isEmpty());
    }

    @Test
    void aggregated_carriesSourceGroup() {
        Object[] vals = { null, null, null };
        FlatRow r = FlatRow.aggregated(SCHEMA, vals, List.of());
        assertTrue(r.isAggregated());
        assertTrue(r.sourceGroup().isPresent());
    }

    @Test
    void preFlattened_isNotAggregated() {
        FlatRow r = FlatRow.preFlattened(SCHEMA, new Object[]{ null, null, null });
        assertFalse(r.isAggregated());
    }

    @Test
    void schemaAccessor_returnsTheSameInstance() {
        FlatRow r = FlatRow.of(SCHEMA, new Object[3]);
        assertSame(SCHEMA, r.schema());
    }

    @Test
    void implementsRowAccessor() {
        FlatRow r = FlatRow.of(SCHEMA, new Object[3]);
        assertInstanceOf(RowAccessor.class, r);
    }

    @Test
    void hasWindowResults_falseWithoutWindowSlots() {
        FlatRow r = FlatRow.of(SCHEMA, new Object[3]);
        assertFalse(r.hasWindowResults());
        assertNull(r.getWindowResult(new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()))));
    }

    @Test
    void getWindowResult_returnsValueWhenSlotPresent() {
        Expression.WindowFnCall call = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        RowSchema schemaWithSlot = RowSchema.of(List.of(FieldKey.of("a")))
                .withWindowSlots(List.of(call));
        Object[] vals = { new SqlString("alpha"), SqlNumber.of(7L) };
        FlatRow r = FlatRow.of(schemaWithSlot, vals);
        assertTrue(r.hasWindowResults());
        SqlValue v = r.getWindowResult(call);
        assertEquals(7L, ((SqlNumber) v).longValue());
    }

    @Test
    void materialize_copiesValuesAndRetainsOriginal() {
        var fields = new java.util.LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        fields.put("name", new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("Bob"));
        fields.put("age", new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(42L));
        Row lazy = Row.lazy(new io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue(fields),
                new FieldKey.Interner());
        RowSchema schema = RowSchema.of(List.of(
                FieldKey.of("name"),
                FieldKey.of("age"),
                FieldKey.of("missing")));
        FlatRow flat = FlatRow.materialize(lazy, schema);
        assertEquals("Bob", ((SqlString) flat.get(FieldKey.of("name"))).value());
        assertEquals(42L, ((SqlNumber) flat.get(FieldKey.of("age"))).longValue());
        assertSame(SqlNull.INSTANCE, flat.get(FieldKey.of("missing")));
        assertTrue(flat.originalValue().isPresent());
    }
}
