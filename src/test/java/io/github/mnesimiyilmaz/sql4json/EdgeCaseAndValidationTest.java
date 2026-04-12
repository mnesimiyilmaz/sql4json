package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EdgeCaseAndValidationTest {

    @Nested
    class EdgeCases {

        private List<JsonValue> arr(JsonValue v) {
            return v.asArray().orElseThrow();
        }

        private Map<String, JsonValue> obj(JsonValue v) {
            return v.asObject().orElseThrow();
        }

        private String str(JsonValue v) {
            return v.asString().orElseThrow();
        }

        private int num(JsonValue v) {
            return v.asNumber().orElseThrow().intValue();
        }

        private JsonValue execute(String sql, String data) {
            return SQL4Json.queryAsJsonValue(sql, data);
        }

        @Test
        void emptyArrayInput_returnsEmptyResult() {
            JsonValue result = execute("SELECT * FROM $r", "[]");
            assertEquals(0, arr(result).size());
        }

        @Test
        void singleItemArray_returnsOneResult() {
            String data = """
                    [{"name":"Alice"}]""";
            JsonValue result = execute("SELECT * FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(obj(arr(result).getFirst()).get("name")));
        }

        @Test
        void nullFieldInWhere_returnsNoResults() {
            String data = """
                    [{"name":"Alice"}]""";
            JsonValue result = execute("SELECT * FROM $r WHERE nonexistent_field = 'x'", data);
            assertEquals(0, arr(result).size());
        }

        @Test
        void booleanValuesInWhere() {
            String data = """
                    [{"active":true,"name":"Alice"},{"active":false,"name":"Bob"}]""";

            JsonValue result = execute("SELECT * FROM $r WHERE active = true", data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(obj(arr(result).getFirst()).get("name")));
        }

        @Test
        void mixedTypesWithNullInWhere() {
            String data = """
                    [{"age":1},{"age":null}]""";

            JsonValue result = execute("SELECT * FROM $r WHERE age > 0", data);
            assertEquals(1, arr(result).size());
            assertEquals(1, num(obj(arr(result).getFirst()).get("age")));
        }

        @Test
        void nestedQueryWithSubqueryInFrom() {
            String data = """
                    {"name":"Alice","age":30}""";

            String sql = "SELECT renamed FROM (SELECT name AS renamed, age FROM $r)";
            JsonValue result = execute(sql, data);
            assertEquals(1, arr(result).size());
            assertEquals("Alice", str(obj(arr(result).getFirst()).get("renamed")));
        }

        @Test
        void veryLongFieldNames() {
            String longName = "a" + String.join("", Collections.nCopies(50, "bcdefghij"));
            String data = "[{\"" + longName + "\":\"value\"}]";

            JsonValue result = execute("SELECT * FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals("value", str(obj(arr(result).getFirst()).get(longName)));
        }

        @Test
        void unicodeValuesInStrings() {
            String data = """
                    [{"name":"\u6771\u4EAC"},{"name":"\u0645\u0631\u062D\u0628\u0627"}]""";

            JsonValue result = execute("SELECT * FROM $r", data);
            assertEquals(2, arr(result).size());
            assertEquals("\u6771\u4EAC", str(obj(arr(result).get(0)).get("name")));
            assertEquals("\u0645\u0631\u062D\u0628\u0627", str(obj(arr(result).get(1)).get("name")));
        }

        @Test
        void selectWithAliasOnNestedField() {
            String data = """
                    {"name":"Alice","address":{"city":"Istanbul","zip":"34000"}}""";

            JsonValue result = execute("SELECT address.city AS location FROM $r", data);
            assertEquals(1, arr(result).size());
            assertEquals("Istanbul", str(obj(arr(result).getFirst()).get("location")));
        }
    }

    @Nested
    class InputValidation {

        @Test
        void nullSql_throwsException() {
            assertThrows(SQL4JsonException.class,
                    () -> SQL4Json.query(null, "[]"));
        }

        @Test
        void nullInputData_throwsException() {
            assertThrows(SQL4JsonException.class,
                    () -> SQL4Json.query("SELECT * FROM $r", (String) null));
        }

        @Test
        void malformedJsonInput_throwsExecutionException() {
            assertThrows(SQL4JsonException.class,
                    () -> SQL4Json.query("SELECT * FROM $r", "{broken json"));
        }
    }
}
