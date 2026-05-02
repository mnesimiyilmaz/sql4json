// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.mnesimiyilmaz.sql4json.json.JsonParser;
import io.github.mnesimiyilmaz.sql4json.parser.OrderByColumnDef;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class QueryPipelineTest {

    @Test
    void where_stage_filters_rows() {
        var row1 = Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(30)));
        var row2 = Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(20)));
        var row3 = Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(35)));

        // WHERE age > 25
        var query = new QueryDefinition(
                List.of(SelectColumnDef.asterisk()),
                "$r",
                null,
                row -> {
                    var age = row.get(FieldKey.of("age"));
                    return age instanceof SqlNumber n && n.doubleValue() > 25;
                },
                null,
                null,
                null,
                Set.of(),
                false,
                null,
                null,
                false,
                null,
                null,
                null,
                null,
                0,
                Set.of(),
                0,
                List.of());

        var result = QueryPipeline.build(query, FunctionRegistry.createDefault(), Sql4jsonSettings.defaults())
                .execute(Stream.of(row1, row2, row3));

        assertEquals(2, result.size());
    }

    @Test
    void order_by_sorts_result() {
        var row1 = Row.eager(Map.of(FieldKey.of("name"), new SqlString("Charlie")));
        var row2 = Row.eager(Map.of(FieldKey.of("name"), new SqlString("Alice")));

        var query = new QueryDefinition(
                List.of(SelectColumnDef.asterisk()),
                "$r",
                null,
                null,
                null,
                null,
                List.of(OrderByColumnDef.of("name", "ASC")),
                Set.of(),
                false,
                null,
                null,
                false,
                null,
                null,
                null,
                null,
                0,
                Set.of(),
                0,
                List.of());

        var result = QueryPipeline.build(query, FunctionRegistry.createDefault(), Sql4jsonSettings.defaults())
                .execute(Stream.of(row1, row2));

        assertEquals("Alice", ((SqlString) result.getFirst().get(FieldKey.of("name"))).value());
    }

    // ── Stream execution ────────────────────────────────────────────────

    @Test
    void executeAsStream_returns_same_results_as_execute() {
        String json = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]";
        JsonValue data = JsonParser.parse(json);
        QueryDefinition query = QueryParser.parse("SELECT * FROM $r WHERE age > 25");
        FunctionRegistry registry = FunctionRegistry.getDefault();
        QueryPipeline pipeline = QueryPipeline.build(query, registry, Sql4jsonSettings.defaults());

        FieldKey.Interner interner1 = new FieldKey.Interner();
        Stream<RowAccessor> input1 =
                data.asArray().orElseThrow().stream().map(e -> (RowAccessor) Row.lazy(e, interner1));
        List<RowAccessor> listResult = pipeline.execute(input1);

        // Need to rebuild pipeline — stream is consumed
        QueryPipeline pipeline2 = QueryPipeline.build(query, registry, Sql4jsonSettings.defaults());
        FieldKey.Interner interner2 = new FieldKey.Interner();
        Stream<RowAccessor> input2 =
                data.asArray().orElseThrow().stream().map(e -> (RowAccessor) Row.lazy(e, interner2));
        List<RowAccessor> streamResult = pipeline2.executeAsStream(input2).toList();

        assertEquals(listResult.size(), streamResult.size());
    }

    @Test
    void executeAsStream_supports_limit_short_circuit() {
        String json = "[{\"id\":1},{\"id\":2},{\"id\":3},{\"id\":4},{\"id\":5}]";
        JsonValue data = JsonParser.parse(json);
        QueryDefinition query = QueryParser.parse("SELECT * FROM $r LIMIT 2");
        FunctionRegistry registry = FunctionRegistry.getDefault();
        QueryPipeline pipeline = QueryPipeline.build(query, registry, Sql4jsonSettings.defaults());

        FieldKey.Interner interner = new FieldKey.Interner();
        Stream<RowAccessor> input = data.asArray().orElseThrow().stream().map(e -> (RowAccessor) Row.lazy(e, interner));
        List<RowAccessor> result = pipeline.executeAsStream(input).toList();

        assertEquals(2, result.size());
    }
}
