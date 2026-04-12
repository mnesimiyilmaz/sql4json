package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mnesimiyilmaz
 */
class SQL4JsonQueryTests {

    /**
     * Helper: get array elements from a JsonValue.
     */
    private static List<JsonValue> arr(JsonValue v) {
        return v.asArray().orElseThrow();
    }

    /**
     * Helper: get object field map from a JsonValue.
     */
    private static Map<String, JsonValue> obj(JsonValue v) {
        return v.asObject().orElseThrow();
    }

    /**
     * Helper: get string value.
     */
    private static String str(JsonValue v) {
        return v.asString().orElseThrow();
    }

    /**
     * Helper: get int value.
     */
    private static int num(JsonValue v) {
        return v.asNumber().orElseThrow().intValue();
    }

    @Test
    void when_select_asterisk_then_return_all() {
        String data = """
                {"name":"M\u00fccahit","surname":"YILMAZ","age":26,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}""";

        String jql = "SELECT * FROM $r";
        JsonValue result = arr(SQL4Json.queryAsJsonValue(jql, data)).getFirst();

        assertAll(
                () -> assertEquals("M\u00fccahit", str(obj(result).get("name"))),
                () -> assertEquals("YILMAZ", str(obj(result).get("surname"))),
                () -> assertEquals(26, num(obj(result).get("age")))
        );
    }

    @Test
    void when_select_asterisk_from_nested_data_then_return_all() {
        String data = """
                {"nested":{"data":{"name":"M\u00fccahit","surname":"YILMAZ","age":26,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}}}""";

        String jql = "SELECT * FROM $r.nested.data";
        JsonValue result = arr(SQL4Json.queryAsJsonValue(jql, data)).getFirst();

        assertAll(
                () -> assertEquals("M\u00fccahit", str(obj(result).get("name"))),
                () -> assertEquals("YILMAZ", str(obj(result).get("surname"))),
                () -> assertEquals(26, num(obj(result).get("age")))
        );
    }

    @Test
    void when_select_with_alias_on_basic_column_then_expect_no_error() {
        String data = """
                {"name":"M\u00fccahit","surname":"YILMAZ","age":26,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}""";

        String jql = "SELECT " +
                "name AS user.name, " +
                "surname AS user.surname, " +
                "age AS user.age " +
                "FROM $r";
        JsonValue result = arr(SQL4Json.queryAsJsonValue(jql, data)).getFirst();

        Map<String, JsonValue> user = obj(obj(result).get("user"));
        assertAll(
                () -> assertEquals("M\u00fccahit", str(user.get("name"))),
                () -> assertEquals("YILMAZ", str(user.get("surname"))),
                () -> assertEquals(26, num(user.get("age")))
        );
    }

    @Test
    void when_select_with_alias_on_object_column_then_expect_no_error() {
        String data = """
                {"name":"M\u00fccahit","surname":"YILMAZ","age":26,"account":{"username":"test","nicknames":["nick","name"],"active":true,"loginHistoryList":null},"lastLoginDateTime":null,"dateOfBirth":null}""";

        String jql = "SELECT " +
                "name AS xyz.name, " +
                "account as user.acc, " +
                "account.username AS accUsername " +
                "FROM $r";
        JsonValue result = arr(SQL4Json.queryAsJsonValue(jql, data)).getFirst();

        assertAll(
                () -> assertEquals("M\u00fccahit", str(obj(obj(result).get("xyz")).get("name"))),
                () -> assertEquals("test", str(obj(obj(obj(result).get("user")).get("acc")).get("username"))),
                () -> assertEquals("test", str(obj(result).get("accUsername")))
        );
    }

    static Stream<Arguments> scalarFunctionCases() {
        return Stream.of(
                Arguments.of("LOWER(name,'tr-TR')", "M\u00fccahit", "m\u00fccahit"),
                Arguments.of("UPPER(name,'tr-TR')", "M\u00fccahit", "M\u00dcCAH\u0130T"),
                Arguments.of("COALESCE(name,'Nesimi')", null, "Nesimi")
        );
    }

    @ParameterizedTest(name = "scalar function {0} on single person -> {2}")
    @MethodSource("scalarFunctionCases")
    void when_use_scalar_func_in_select_then_expect_correct_value(String funcExpr, String inputName,
                                                                  String expected) {
        String nameValue = inputName == null ? "null" : "\"" + inputName + "\"";
        String data = "{\"name\":" + nameValue + ",\"surname\":null,\"age\":null,\"account\":null,\"lastLoginDateTime\":null,\"dateOfBirth\":null}";

        String jql = "SELECT " + funcExpr + " AS name FROM $r";
        JsonValue result = arr(SQL4Json.queryAsJsonValue(jql, data)).getFirst();

        assertEquals(expected, str(obj(result).get("name")));
    }

    @Test
    void when_use_lower_func_in_select_with_null_values_then_expect_no_error() {
        String data = """
                [{"name":"M\u00fccahit","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":null,"surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}]""";

        String jql = "SELECT " +
                "LOWER(name,'tr-TR') AS name " +
                "FROM $r";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertAll(
                () -> assertEquals("m\u00fccahit", str(obj(arr(result).get(0)).get("name"))),
                () -> assertTrue(obj(arr(result).get(1)).get("name").isNull())
        );
    }

    @Test
    void when_use_upper_func_in_select_with_null_values_then_expect_no_error() {
        String data = """
                [{"name":"M\u00fccahit","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":null,"surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}]""";

        String jql = "SELECT " +
                "UPPER(name,'tr-TR') AS name " +
                "FROM $r";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertAll(
                () -> assertEquals("M\u00dcCAH\u0130T", str(obj(arr(result).get(0)).get("name"))),
                () -> assertTrue(obj(arr(result).get(1)).get("name").isNull())
        );
    }

    @Test
    void when_use_nested_query_then_expect_no_error() {
        String data = """
                {"data":{"name":"M\u00fccahit","surname":"YILMAZ","age":26,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}}""";

        String jql = "SELECT " +
                "user.name AS username " +
                "FROM (SELECT name AS user.name, surname AS user.surname, age AS user.age FROM $r.data)";
        JsonValue result = arr(SQL4Json.queryAsJsonValue(jql, data)).getFirst();

        assertAll(
                () -> assertEquals("M\u00fccahit", str(obj(result).get("username")))
        );
    }

    @Test
    void when_use_lower_func_in_any_possible_place_then_expect_no_error() {
        String data = """
                [{"name":"M\u00fccahit","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":"M\u00fccahit","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":"M\u00fccahit","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":"Nesimi","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":"Nesimi","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":null,"surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}]""";

        String jql = "SELECT " +
                "LOWER(name,'tr-TR') AS name, " +
                "COUNT(*) AS cnt " +
                "FROM $r " +
                "WHERE LOWER(name,'tr-TR') = 'm\u00fccahit' OR LOWER(name,'tr-TR') = 'nesimi' " +
                "GROUP BY LOWER(name,'tr-TR') " +
                "HAVING cnt > 1 " +
                "ORDER BY cnt DESC, LOWER(name,'tr-TR') ASC";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertAll(
                () -> assertEquals(3, num(obj(arr(result).get(0)).get("cnt"))),
                () -> assertEquals(2, num(obj(arr(result).get(1)).get("cnt")))
        );
    }

    @Test
    void when_use_to_date_function_in_where_condition_then_expect_no_error() {
        String data = """
                {"name":"M\u00fccahit","surname":null,"age":null,"account":null,"lastLoginDateTime":"2023-10-23T21:00:00","dateOfBirth":"1997-06-01"}""";

        String jql = "SELECT " +
                "name " +
                "FROM $r " +
                "WHERE TO_DATE(dateOfBirth) = TO_DATE('1997-06-01') " +
                "AND TO_DATE(lastLoginDateTime) > TO_DATE('2023-10-23 20:00:00','yyyy-MM-dd HH:mm:ss')";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertEquals("M\u00fccahit", str(obj(arr(result).getFirst()).get("name")));
    }

    @Test
    void when_use_now_function_in_where_condition_then_expect_no_error() {
        String data = """
                {"name":"M\u00fccahit","surname":null,"age":null,"account":null,"lastLoginDateTime":"2099-01-01T00:00:00","dateOfBirth":null}""";

        String jql = "SELECT " +
                "name " +
                "FROM $r " +
                "WHERE TO_DATE(lastLoginDateTime) > NOW()";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertEquals("M\u00fccahit", str(obj(arr(result).getFirst()).get("name")));
    }

    @Test
    void when_use_isnull_and_isnotnull_in_where_condition_then_expect_no_error() {
        String data = """
                {"name":"M\u00fccahit","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}""";

        String jql = "SELECT " +
                "name " +
                "FROM $r " +
                "WHERE name IS NOT NULL " +
                "AND surname IS NULL";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertEquals("M\u00fccahit", str(obj(arr(result).getFirst()).get("name")));
    }

    @Test
    void when_use_like_in_where_condition_then_expect_no_error() {
        String data = """
                [{"name":"M\u00fccahit","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":"Nesimi","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":"Y\u0131lmaz","surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}]""";

        String jql = "SELECT " +
                "name " +
                "FROM $r " +
                "WHERE name LIKE '%cahit%' " +
                "OR name LIKE 'Ne%' " +
                "OR name LIKE '%maz'";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertAll(
                () -> assertEquals("M\u00fccahit", str(obj(arr(result).get(0)).get("name"))),
                () -> assertEquals("Nesimi", str(obj(arr(result).get(1)).get("name"))),
                () -> assertEquals("Y\u0131lmaz", str(obj(arr(result).get(2)).get("name")))
        );
    }

    @Test
    void when_use_boolean_value_in_where_condition_then_expect_no_error() {
        String data = """
                [{"name":null,"surname":null,"age":null,"account":{"username":null,"nicknames":null,"active":true,"loginHistoryList":null},"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":null,"surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}]""";

        String jql = "SELECT * " +
                "FROM $r " +
                "WHERE account.active = true";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertEquals(1, arr(result).size());
    }

    @Test
    void when_use_numeric_value_in_where_condition_then_expect_no_error() {
        String data = """
                [{"name":null,"surname":null,"age":25,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":null,"surname":null,"age":null,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}]""";

        String jql = "SELECT * " +
                "FROM $r " +
                "WHERE age > 20";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertEquals(1, arr(result).size());
    }

    @Test
    void when_alias_for_object_field_in_select_then_expect_no_error() {
        String data = """
                {"name":null,"surname":null,"age":null,"account":{"username":"muc","nicknames":null,"active":true,"loginHistoryList":null},"lastLoginDateTime":null,"dateOfBirth":null}""";

        String jql = "SELECT account as acc FROM $r";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertEquals("muc", str(obj(obj(arr(result).getFirst()).get("acc")).get("username")));
    }

    @Test
    void when_use_group_by_multiple_fields_then_expect_no_error() {
        String data = """
                [{"field1":"a","field2":"a","field3":"a","value":1},\
                {"field1":"a","field2":"a","field3":"b","value":2},\
                {"field1":"a","field2":"a","field3":"c","value":3}]""";

        String jql = "SELECT " +
                "field1, " +
                "field2, " +
                "SUM(value) as total, " +
                "COUNT(value) as cnt, " +
                "MAX(value) as max_val, " +
                "MIN(value) as min_val, " +
                "AVG(value) as avg_val " +
                "FROM $r " +
                "GROUP BY field1, field2";
        JsonValue result = arr(SQL4Json.queryAsJsonValue(jql, data)).getFirst();

        assertAll(
                () -> assertEquals("a", str(obj(result).get("field1"))),
                () -> assertEquals("a", str(obj(result).get("field2"))),
                () -> assertEquals(6, num(obj(result).get("total"))),
                () -> assertEquals(3, num(obj(result).get("max_val"))),
                () -> assertEquals(1, num(obj(result).get("min_val"))),
                () -> assertEquals(2, num(obj(result).get("avg_val"))),
                () -> assertEquals(3, num(obj(result).get("cnt")))
        );
    }

    @Test
    void when_use_proper_nested_query_then_expect_no_error() {
        String data = """
                {"data":{"name":"M\u00fccahit","surname":"YILMAZ","age":26,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}}""";

        String jql = "SELECT " +
                "user.name AS username " +
                "FROM (" +
                "SELECT " +
                "name AS user.name, " +
                "surname AS user.surname, " +
                "age AS user.age " +
                "FROM $r.data)";
        JsonValue result = arr(SQL4Json.queryAsJsonValue(jql, data)).getFirst();

        assertEquals("M\u00fccahit", str(obj(result).get("username")));
    }

    @Test
    void when_use_proper_nested_query_withBothInnerAndOuterWhereClause_then_expect_no_error() {
        String data = """
                [{"name":"M\u00fccahit","surname":"YILMAZ","age":26,"account":null,"lastLoginDateTime":null,"dateOfBirth":null},\
                {"name":"Hayanesh","surname":"Kamalan","age":18,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}]""";

        String jql = "SELECT " +
                "user.name AS username," +
                "user.age AS age " +
                "FROM (" +
                "SELECT " +
                "name AS user.name, " +
                "surname AS user.surname, " +
                "age AS user.age " +
                "FROM $r WHERE age > 10) WHERE user.age > 16";
        JsonValue result = SQL4Json.queryAsJsonValue(jql, data);

        assertEquals("M\u00fccahit", str(obj(arr(result).getFirst()).get("username")));
    }

    @Test
    void when_try_to_execute_incorrect_query_then_expect_error() {
        String data = """
                {"name":"M\u00fccahit","surname":"YILMAZ","age":26,"account":null,"lastLoginDateTime":null,"dateOfBirth":null}""";

        assertThrows(SQL4JsonParseException.class,
                () -> SQL4Json.queryAsJsonValue("select from $r", data));
    }

}
