package io.github.mnesimiyilmaz.sql4json.settings;

import io.github.mnesimiyilmaz.sql4json.engine.QueryExecutor;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.json.JsonParser;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class Sql4jsonSettingsMultiSourceTests {

    @Test
    void unknownTable_redacted_by_default_omits_table_name_and_available_keys() {
        // default settings have redactErrorDetails=true
        QueryExecutor executor = new QueryExecutor();
        String sql = "SELECT u.name FROM users u JOIN orders o ON u.id = o.userId";
        QueryDefinition qd = QueryParser.parse(sql);
        Map<String, JsonValue> sources = Map.of(
                "users", JsonParser.parse("[]")
                // orders intentionally missing
        );

        var ex = assertThrows(SQL4JsonException.class,
                () -> executor.execute(qd, sources, Sql4jsonSettings.defaults()));
        assertEquals("Unknown table", ex.getMessage(),
                "redacted message must not leak the missing table name or available keys");
    }

    @Test
    void unknownTable_unredacted_includes_table_name_and_available_keys() {
        QueryExecutor executor = new QueryExecutor();
        String sql = "SELECT u.name FROM users u JOIN orders o ON u.id = o.userId";
        QueryDefinition qd = QueryParser.parse(sql);
        Map<String, JsonValue> sources = Map.of(
                "users", JsonParser.parse("[]")
        );
        var settings = Sql4jsonSettings.builder()
                .security(s -> s.redactErrorDetails(false))
                .build();

        var ex = assertThrows(SQL4JsonException.class,
                () -> executor.execute(qd, sources, settings));
        assertTrue(ex.getMessage().contains("orders"),
                "unredacted message should include the missing table name — actual: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("users"),
                "unredacted message should include available keys — actual: " + ex.getMessage());
    }
}
