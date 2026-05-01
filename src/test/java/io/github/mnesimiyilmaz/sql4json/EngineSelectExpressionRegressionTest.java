package io.github.mnesimiyilmaz.sql4json;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EngineSelectExpressionRegressionTest {

    private static final String JSON =
            "[{\"id\":\"014a0ff1-cc32-4bef-887b-4b84bc584805\"},{\"id\":\"22222222\"}]";
    private static final String SQL  =
            "SELECT 'my-fiction-value' as myCol, lpad(id,2,'0') FROM $r limit 10";

    @Test
    void engineBuilderPath_evaluatesSelectExpressions() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        String result = engine.query(SQL);
        assertTrue(
                result.contains("\"myCol\":\"my-fiction-value\""),
                "engine path must evaluate string literal in SELECT, got: " + result);
        assertTrue(
                result.contains("\"id\":\"01\""),
                "engine path must evaluate lpad(id,2,'0') to '01', got: " + result);
    }

    @Test
    void staticPath_evaluatesSelectExpressions() {
        String result = SQL4Json.query(SQL, JSON);
        assertEquals(
                "[{\"myCol\":\"my-fiction-value\",\"id\":\"01\"},"
                        + "{\"myCol\":\"my-fiction-value\",\"id\":\"22\"}]",
                result);
    }

    @Test
    void joinPath_evaluatesSelectExpressionsAndLiterals() {
        String users = "[{\"id\":1,\"name\":\"Bo\"}]";
        String orders = "[{\"userId\":1,\"total\":99}]";
        String sql = "SELECT 'X' as tag, lpad(u.name, 4, '*') as padded, o.total "
                + "FROM users u JOIN orders o ON u.id = o.userId";
        String result = SQL4Json.query(sql,
                java.util.Map.of("users", users, "orders", orders));
        assertTrue(result.contains("\"tag\":\"X\""),
                "JOIN+SELECT must evaluate string literals, got: " + result);
        assertTrue(result.contains("\"padded\":\"**Bo\""),
                "JOIN+SELECT must evaluate scalar fn, got: " + result);
        assertTrue(result.contains("\"o.total\":99") || result.contains("\"total\":99"),
                "JOIN column ref must still resolve, got: " + result);
    }
}
