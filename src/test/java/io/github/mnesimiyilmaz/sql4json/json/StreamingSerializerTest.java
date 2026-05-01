package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamingSerializerTest {

    private static final FunctionRegistry REGISTRY = FunctionRegistry.getDefault();

    @Test
    void serialize_empty_stream() {
        String result = StreamingSerializer.serialize(
                Stream.empty(),
                List.of(SelectColumnDef.asterisk()),
                REGISTRY,
                Integer.MAX_VALUE
        );
        assertEquals("[]", result);
    }

    @Test
    void serialize_single_row() {
        JsonValue element = JsonParser.parse("{\"name\":\"Alice\",\"age\":30}");
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(element, interner);

        String result = StreamingSerializer.serialize(
                Stream.of(row),
                List.of(SelectColumnDef.asterisk()),
                REGISTRY,
                Integer.MAX_VALUE
        );
        assertEquals("[{\"name\":\"Alice\",\"age\":30}]", result);
    }

    @Test
    void serialize_multiple_rows() {
        FieldKey.Interner interner = new FieldKey.Interner();
        JsonValue e1 = JsonParser.parse("{\"id\":1}");
        JsonValue e2 = JsonParser.parse("{\"id\":2}");
        Row r1 = Row.lazy(e1, interner);
        Row r2 = Row.lazy(e2, interner);

        String result = StreamingSerializer.serialize(
                Stream.of(r1, r2),
                List.of(SelectColumnDef.asterisk()),
                REGISTRY,
                Integer.MAX_VALUE
        );
        assertEquals("[{\"id\":1},{\"id\":2}]", result);
    }

    @Test
    void serialize_matches_unflatten_output() {
        String json = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]";
        List<SelectColumnDef> columns = List.of(SelectColumnDef.asterisk());
        FieldKey.Interner interner = new FieldKey.Interner();

        // Tree path
        List<JsonValue> elements = JsonParser.parse(json).asArray().orElseThrow();
        List<RowAccessor> rows = elements.stream()
                .map(e -> (RowAccessor) Row.lazy(e, interner))
                .toList();
        String treeResult = JsonSerializer.serialize(
                JsonUnflattener.unflatten(rows, columns, REGISTRY));

        // Streaming path
        FieldKey.Interner interner2 = new FieldKey.Interner();
        Stream<RowAccessor> rowStream = elements.stream()
                .map(e -> (RowAccessor) Row.lazy(e, interner2));
        String streamResult = StreamingSerializer.serialize(rowStream, columns, REGISTRY, Integer.MAX_VALUE);

        assertEquals(treeResult, streamResult);
    }

    @Test
    void serialize_with_selected_columns() {
        JsonValue element = JsonParser.parse("{\"name\":\"Alice\",\"age\":30}");
        FieldKey.Interner interner = new FieldKey.Interner();
        Row row = Row.lazy(element, interner);

        List<SelectColumnDef> columns = List.of(SelectColumnDef.column("name"));
        String result = StreamingSerializer.serialize(
                Stream.of(row), columns, REGISTRY, Integer.MAX_VALUE);
        assertEquals("[{\"name\":\"Alice\"}]", result);
    }
}
