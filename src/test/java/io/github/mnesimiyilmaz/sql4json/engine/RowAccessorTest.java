package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.json.JsonLongValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RowAccessorTest {

    @Test
    void lazyRow_isRowAccessor() {
        var fields = new LinkedHashMap<String, JsonValue>();
        fields.put("name", new JsonStringValue("Alice"));
        fields.put("age", new JsonLongValue(30L));
        Row r = Row.lazy(new JsonObjectValue(fields), new FieldKey.Interner());
        RowAccessor a = r;
        assertEquals("Alice", ((SqlString) a.get(FieldKey.of("name"))).value());
        assertSame(SqlNull.INSTANCE, a.get(FieldKey.of("missing")));
        assertNull(a.schema());
        assertFalse(a.isAggregated());
        assertFalse(a.hasWindowResults());
        assertTrue(a.sourceGroup().isEmpty());
    }

    @Test
    void flatRow_isRowAccessor() {
        RowSchema s = RowSchema.of(List.of(FieldKey.of("a"), FieldKey.of("b")));
        Object[] vals = { new SqlString("hi"), null };
        FlatRow f = FlatRow.of(s, vals);
        RowAccessor a = f;
        assertEquals("hi", ((SqlString) a.get(FieldKey.of("a"))).value());
        assertSame(SqlNull.INSTANCE, a.get(FieldKey.of("b")));
        assertSame(s, a.schema());
        assertFalse(a.isAggregated());
        assertFalse(a.hasWindowResults());
    }

    @Test
    void sealedExhaustiveSwitch() {
        RowAccessor[] rows = {
                Row.lazy(new JsonObjectValue(new LinkedHashMap<>()), new FieldKey.Interner()),
                FlatRow.of(RowSchema.of(List.of()), new Object[0])
        };
        for (RowAccessor r : rows) {
            String tag = switch (r) {
                case Row ignored -> "row";
                case FlatRow ignored -> "flat";
            };
            assertNotNull(tag);
        }
    }
}
