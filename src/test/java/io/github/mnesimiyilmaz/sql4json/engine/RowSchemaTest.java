package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RowSchemaTest {

    @Test
    void of_emptyList_returnsZeroSize() {
        RowSchema s = RowSchema.of(List.of());
        assertEquals(0, s.size());
    }

    @Test
    void of_buildsIndexAndFamilyMap() {
        RowSchema s = RowSchema.of(List.of(
                FieldKey.of("name"),
                FieldKey.of("items[0].name"),
                FieldKey.of("items[1].name")));
        assertEquals(3, s.size());
        assertEquals(0, s.indexOf(FieldKey.of("name")));
        assertEquals(2, s.indexOf(FieldKey.of("items[1].name")));
        assertArrayEquals(new int[]{1, 2}, s.familyIndexes("items.name"));
    }

    @Test
    void indexOf_absentReturnsMinusOne() {
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a")));
        assertEquals(-1, s.indexOf(FieldKey.of("b")));
    }

    @Test
    void of_duplicateColumnThrows() {
        assertThrows(SQL4JsonExecutionException.class,
                () -> RowSchema.of(List.of(FieldKey.of("a"), FieldKey.of("a"))));
    }

    @Test
    void project_retainsSubset() {
        RowSchema s = RowSchema.of(List.of(
                FieldKey.of("a"), FieldKey.of("b"), FieldKey.of("c")));
        var keep = new LinkedHashSet<FieldKey>();
        keep.add(FieldKey.of("a"));
        keep.add(FieldKey.of("c"));
        RowSchema p = s.project(keep);
        assertEquals(2, p.size());
        assertEquals(0, p.indexOf(FieldKey.of("a")));
        assertEquals(1, p.indexOf(FieldKey.of("c")));
    }

    @Test
    void concat_appendsOther() {
        RowSchema l = RowSchema.of(List.of(FieldKey.of("u.id"), FieldKey.of("u.name")));
        RowSchema r = RowSchema.of(List.of(FieldKey.of("o.user_id"), FieldKey.of("o.amount")));
        RowSchema cat = l.concat(r);
        assertEquals(4, cat.size());
        assertEquals(0, cat.indexOf(FieldKey.of("u.id")));
        assertEquals(2, cat.indexOf(FieldKey.of("o.user_id")));
    }

    @Test
    void concat_collisionThrows() {
        RowSchema l = RowSchema.of(List.of(FieldKey.of("id")));
        RowSchema r = RowSchema.of(List.of(FieldKey.of("id")));
        assertThrows(SQL4JsonExecutionException.class, () -> l.concat(r));
    }

    @Test
    void withWindowSlots_emptyList_returnsSchemaWithNoWindowSlots() {
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a"))).withWindowSlots(List.of());
        assertFalse(s.hasWindowSlots());
    }

    @Test
    void withWindowSlots_nonEmpty_appendsAndIndexes() {
        Expression.WindowFnCall call = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(),
                new WindowSpec(List.of(), List.of()));
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a"))).withWindowSlots(List.of(call));
        assertTrue(s.hasWindowSlots());
        assertEquals(1, s.windowSlot(call).getAsInt());
        assertEquals(2, s.size());
    }

    @Test
    void withWindowSlots_aliasMappingMakesAliasResolveToSlot() {
        Expression.WindowFnCall call = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(),
                new WindowSpec(List.of(), List.of()));
        FieldKey alias = FieldKey.of("rk");
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a")))
                .withWindowSlots(List.of(call), java.util.Map.of(call, alias));
        assertEquals(1, s.indexOf(alias));
        assertEquals(s.windowSlot(call).getAsInt(), s.indexOf(alias));
    }

    @Test
    void builder_addAndContains() {
        var b = new RowSchema.Builder();
        b.add(FieldKey.of("x"));
        assertTrue(b.contains(FieldKey.of("x")));
        b.add(FieldKey.of("x"));
        RowSchema built = b.build();
        assertEquals(1, built.size());
    }

    @Test
    void columns_returnsImmutableList() {
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a"), FieldKey.of("b")));
        var cols = s.columns();
        assertEquals(2, cols.size());
        assertThrows(UnsupportedOperationException.class, () -> cols.add(FieldKey.of("c")));
    }

    @Test
    void columnAt_returnsKeyAtOrdinal() {
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a"), FieldKey.of("b")));
        assertEquals(FieldKey.of("a"), s.columnAt(0));
        assertEquals(FieldKey.of("b"), s.columnAt(1));
    }

    @Test
    void hasWindowSlots_falseOnSchemaCreatedWithoutWithWindowSlots() {
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a")));
        assertFalse(s.hasWindowSlots());
    }

    @Test
    void windowSlot_returnsEmptyWhenSchemaHasNoSlotsMap() {
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a")));
        Expression.WindowFnCall call = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        assertTrue(s.windowSlot(call).isEmpty());
    }

    @Test
    void windowSlot_returnsEmptyForUnknownCall() {
        Expression.WindowFnCall present = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        Expression.WindowFnCall absent = new Expression.WindowFnCall(
                "RANK", List.of(), new WindowSpec(List.of(), List.of()));
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a")))
                .withWindowSlots(List.of(present));
        assertTrue(s.windowSlot(absent).isEmpty());
    }

    @Test
    void withWindowSlots_nullAliasMap_uses_synthetic_columnKeys() {
        Expression.WindowFnCall call = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a")))
                .withWindowSlots(List.of(call), null);
        assertTrue(s.hasWindowSlots());
        assertEquals(1, s.windowSlot(call).getAsInt());
        assertEquals(2, s.size());
    }
}
