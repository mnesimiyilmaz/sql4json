package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SQL4JsonQueryAsTest {

    record Person(String name, int age) {
    }

    private static final String JSON = """
            [
              {"name":"Alice","age":30},
              {"name":"Bob","age":40}
            ]
            """;

    @Test
    void when_queryAsList_then_each_row_mapped() {
        List<Person> people = SQL4Json.queryAsList(
                "SELECT name, age FROM $r", JSON, Person.class);
        assertEquals(List.of(new Person("Alice", 30), new Person("Bob", 40)), people);
    }

    @Test
    void when_queryAs_with_single_row_result_then_unwrap() {
        Person p = SQL4Json.queryAs(
                "SELECT name, age FROM $r WHERE age = 30", JSON, Person.class);
        assertEquals(new Person("Alice", 30), p);
    }

    @Test
    void when_queryAs_with_zero_rows_then_exception() {
        assertThrows(SQL4JsonMappingException.class, () -> SQL4Json.queryAs(
                "SELECT name, age FROM $r WHERE age = 999", JSON, Person.class));
    }

    @Test
    void when_queryAs_with_multiple_rows_then_exception() {
        SQL4JsonMappingException e = assertThrows(SQL4JsonMappingException.class,
                () -> SQL4Json.queryAs("SELECT name, age FROM $r", JSON, Person.class));
        assertTrue(e.getMessage().toLowerCase().contains("queryaslist"));
    }

    record UserOrder(String userName, int orderAmount) {
    }

    @Test
    void when_join_query_as_list_then_mapped() {
        String users = """
                [{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]
                """;
        String orders = """
                [{"userId":1,"amount":100},{"userId":2,"amount":200}]
                """;
        List<UserOrder> result = SQL4Json.queryAsList(
                "SELECT u.name AS userName, o.amount AS orderAmount "
                        + "FROM users u INNER JOIN orders o ON u.id = o.userId",
                Map.of("users", users, "orders", orders),
                UserOrder.class);
        // Hash-join may not preserve build-side order — use set equality.
        assertEquals(
                java.util.Set.of(new UserOrder("Alice", 100), new UserOrder("Bob", 200)),
                java.util.Set.copyOf(result));
        assertEquals(2, result.size());
    }

    @Test
    void when_prepared_query_execute_as_list_then_mapped() {
        PreparedQuery q = SQL4Json.prepare("SELECT name, age FROM $r WHERE age > 25");
        List<Person> out = q.executeAsList(JSON, Person.class);
        assertEquals(List.of(new Person("Alice", 30), new Person("Bob", 40)), out);
    }

    @Test
    void when_prepared_query_execute_as_single_row_then_mapped() {
        PreparedQuery q = SQL4Json.prepare("SELECT name, age FROM $r WHERE age = 30");
        Person p = q.executeAs(JSON, Person.class);
        assertEquals(new Person("Alice", 30), p);
    }

    @Test
    void when_engine_query_as_list_then_mapped() {
        SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        List<Person> out = engine.queryAsList("SELECT name, age FROM $r", Person.class);
        assertEquals(List.of(new Person("Alice", 30), new Person("Bob", 40)), out);
    }
}
