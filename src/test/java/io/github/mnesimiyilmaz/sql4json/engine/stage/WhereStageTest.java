package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WhereStageTest {

    @Test
    void rows_matching_criteria_pass_through() {
        var row1 = Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(30)));
        var row2 = Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(20)));
        var row3 = Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(35)));

        var stage = new WhereStage(row -> {
            var age = row.get(FieldKey.of("age"));
            return age instanceof SqlNumber n && n.doubleValue() > 25;
        });

        List<Row> result = stage.apply(Stream.of(row1, row2, row3)).toList();
        assertEquals(2, result.size()); // row1 and row3
    }

    @Test
    void where_stage_is_lazy() {
        assertTrue(new WhereStage(row -> true).isLazy());
    }

    @Test
    void all_rows_filtered_returns_empty() {
        var row = Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(5)));
        List<Row> result = new WhereStage(r -> false).apply(Stream.of(row)).toList();
        assertTrue(result.isEmpty());
    }
}
