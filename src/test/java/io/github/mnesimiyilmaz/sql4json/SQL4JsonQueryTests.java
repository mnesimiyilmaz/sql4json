package io.github.mnesimiyilmaz.sql4json;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.mnesimiyilmaz.sql4json.dataclasses.Account;
import io.github.mnesimiyilmaz.sql4json.dataclasses.Person;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mnesimiyilmaz
 */
class SQL4JsonQueryTests {

    @Test
    void when_select_asterisk_then_return_all() {
        Person person = new Person();
        person.setName("Mücahit");
        person.setSurname("YILMAZ");
        person.setAge(26);

        String jql = "SELECT * FROM $r";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, person);
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult().get(0);

        assertAll(
                () -> assertEquals("Mücahit", result.get("name").asText()),
                () -> assertEquals("YILMAZ", result.get("surname").asText()),
                () -> assertEquals(26, result.get("age").asInt())
        );
    }

    @Test
    void when_select_asterisk_from_nested_data_then_return_all() {
        Person person = new Person();
        person.setName("Mücahit");
        person.setSurname("YILMAZ");
        person.setAge(26);

        String jql = "SELECT * FROM $r.nested.data";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, Collections.singletonMap("nested", Collections.singletonMap("data", person)));
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult().get(0);

        assertAll(
                () -> assertEquals("Mücahit", result.get("name").asText()),
                () -> assertEquals("YILMAZ", result.get("surname").asText()),
                () -> assertEquals(26, result.get("age").asInt())
        );
    }

    @Test
    void when_select_with_alias_on_basic_column_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");
        person.setSurname("YILMAZ");
        person.setAge(26);

        String jql = "SELECT " +
                "name AS user.name, " +
                "surname AS user.surname, " +
                "age AS user.age " +
                "FROM $r";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, person);
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult().get(0);

        assertAll(
                () -> assertEquals("Mücahit", result.get("user").get("name").asText()),
                () -> assertEquals("YILMAZ", result.get("user").get("surname").asText()),
                () -> assertEquals(26, result.get("user").get("age").asInt())
        );
    }

    @Test
    void when_select_with_alias_on_object_column_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");
        person.setSurname("YILMAZ");
        person.setAge(26);
        person.setAccount(new Account("test", new String[]{"nick", "name"}, true, null));

        String jql = "SELECT " +
                "name AS xyz.name, " +
                "account as user.acc, " +
                "account.username AS accUsername " +
                "FROM $r";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, person);
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult().get(0);

        assertAll(
                () -> assertEquals("Mücahit", result.get("xyz").get("name").asText()),
                () -> assertEquals("test", result.get("user").get("acc").get("username").asText()),
                () -> assertEquals("test", result.get("accUsername").asText())
        );
    }

    @Test
    void when_use_lower_func_in_select_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");

        String jql = "SELECT " +
                "LOWER(name,'tr-TR') AS name " +
                "FROM $r";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, person);
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult().get(0);

        assertAll(
                () -> assertEquals("mücahit", result.get("name").asText())
        );
    }

    @Test
    void when_use_lower_func_in_select_with_null_values_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");

        Person person2 = new Person();
        person2.setName(null);

        String jql = "SELECT " +
                "LOWER(name,'tr-TR') AS name " +
                "FROM $r";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, Arrays.asList(person, person2));
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult();

        assertAll(
                () -> assertEquals("mücahit", result.get(0).get("name").asText()),
                () -> assertTrue(result.get(1).get("name").isNull())
        );
    }

    @Test
    void when_use_upper_func_in_select_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");

        String jql = "SELECT " +
                "UPPER(name,'tr-TR') AS name " +
                "FROM $r";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, person);
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult().get(0);

        assertAll(
                () -> assertEquals("MÜCAHİT", result.get("name").asText())
        );
    }

    @Test
    void when_use_upper_func_in_select_with_null_values_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");

        Person person2 = new Person();
        person2.setName(null);

        String jql = "SELECT " +
                "UPPER(name,'tr-TR') AS name " +
                "FROM $r";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, Arrays.asList(person, person2));
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult();

        assertAll(
                () -> assertEquals("MÜCAHİT", result.get(0).get("name").asText()),
                () -> assertTrue(result.get(1).get("name").isNull())
        );
    }

    @Test
    void when_use_coalesce_func_in_select_then_expect_no_error() {
        Person person = new Person();
        person.setName(null);

        String jql = "SELECT " +
                "COALESCE(name,'Nesimi') AS name " +
                "FROM $r";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, person);
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult().get(0);

        assertAll(
                () -> assertEquals("Nesimi", result.get("name").asText())
        );
    }

    @Test
    void when_use_nested_query_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");
        person.setSurname("YILMAZ");
        person.setAge(26);

        String jql = "SELECT " +
                "user.name AS username " +
                "FROM $r " +
                "    >>> " +
                "SELECT " +
                "name AS user.name, " +
                "surname AS user.surname, " +
                "age AS user.age " +
                "FROM $r.data";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, Collections.singletonMap("data", person));
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult().get(0);

        assertAll(
                () -> assertEquals("Mücahit", result.get("username").asText())
        );
    }

    @Test
    void when_use_lower_func_in_any_possible_place_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");
        Person person2 = new Person();
        person2.setName("Mücahit");
        Person person3 = new Person();
        person3.setName("Mücahit");
        Person person4 = new Person();
        person4.setName("Nesimi");
        Person person5 = new Person();
        person5.setName("Nesimi");
        Person person6 = new Person();
        person6.setName(null);

        String jql = "SELECT " +
                "LOWER(name,'tr-TR') AS name, " +
                "COUNT(*) AS cnt " +
                "FROM $r " +
                "WHERE LOWER(name,'tr-TR') = 'mücahit' OR LOWER(name,'tr-TR') = 'nesimi' " +
                "GROUP BY LOWER(name,'tr-TR') " +
                "HAVING cnt > 1 " +
                "ORDER BY cnt DESC, LOWER(name,'tr-TR') ASC";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, Arrays.asList(person, person2, person3, person4, person5, person6));
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult();

        assertAll(
                () -> assertEquals(3, result.get(0).get("cnt").asInt()),
                () -> assertEquals(2, result.get(1).get("cnt").asInt())
        );
    }

    @Test
    void when_use_to_date_function_in_where_condition_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");
        person.setDateOfBirth(LocalDate.of(1997, Month.JUNE, 1));
        person.setLastLoginDateTime(LocalDateTime.of(2023, Month.OCTOBER, 23, 21, 0, 0));

        String jql = "SELECT " +
                "name " +
                "FROM $r " +
                "WHERE TO_DATE(dateOfBirth) = TO_DATE('1997-06-01') " +
                "AND TO_DATE(lastLoginDateTime) > TO_DATE('2023-10-23 20:00:00','yyyy-MM-dd HH:mm:ss')";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, person);
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult();

        assertEquals("Mücahit", result.get(0).get("name").asText());
    }

    @Test
    void when_use_now_function_in_where_condition_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");
        person.setLastLoginDateTime(LocalDateTime.now().plusYears(1));

        String jql = "SELECT " +
                "name " +
                "FROM $r " +
                "WHERE TO_DATE(lastLoginDateTime) > NOW()";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, person);
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult();

        assertEquals("Mücahit", result.get(0).get("name").asText());
    }

    @Test
    void when_use_isnull_and_isnotnull_in_where_condition_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");
        person.setSurname(null);

        String jql = "SELECT " +
                "name " +
                "FROM $r " +
                "WHERE name IS NOT NULL " +
                "AND surname IS NULL";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, person);
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult();

        assertEquals("Mücahit", result.get(0).get("name").asText());
    }

    @Test
    void when_use_like_in_where_condition_then_expect_no_error() {
        Person person = new Person();
        person.setName("Mücahit");
        Person person2 = new Person();
        person2.setName("Nesimi");
        Person person3 = new Person();
        person3.setName("Yılmaz");

        String jql = "SELECT " +
                "name " +
                "FROM $r " +
                "WHERE name LIKE '%cahit%' " +
                "OR name LIKE 'Ne%' " +
                "OR name LIKE '%maz'";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, Arrays.asList(person, person2, person3));
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult();

        assertAll(
                () -> assertEquals("Mücahit", result.get(0).get("name").asText()),
                () -> assertEquals("Nesimi", result.get(1).get("name").asText()),
                () -> assertEquals("Yılmaz", result.get(2).get("name").asText())
        );
    }

    @Test
    void when_use_boolean_value_in_where_condition_then_expect_no_error() {
        Person person = new Person();
        person.setAccount(new Account());
        person.getAccount().setActive(true);
        Person person2 = new Person();

        String jql = "SELECT * " +
                "FROM $r " +
                "WHERE account.active = true";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, Arrays.asList(person, person2));
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult();

        assertEquals(1, result.size());
    }

    @Test
    void when_use_numeric_value_in_where_condition_then_expect_no_error() {
        Person person = new Person();
        person.setAge(25);
        Person person2 = new Person();

        String jql = "SELECT * " +
                "FROM $r " +
                "WHERE age > 20";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, Arrays.asList(person, person2));
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult();

        assertEquals(1, result.size());
    }

    @Test
    void when_alias_for_object_field_in_select_then_expect_no_error() {
        Person person = new Person();
        person.setAccount(new Account());
        person.getAccount().setActive(true);
        person.getAccount().setUsername("muc");

        String jql = "SELECT account as acc FROM $r";
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, person);
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult();

        assertEquals("muc", result.get(0).get("acc").get("username").asText());
    }


    @Test
    void when_use_group_by_multiple_fields_then_expect_no_error() {
        Map<String, Object> val = new HashMap<>();
        val.put("field1", "a");
        val.put("field2", "a");
        val.put("field3", "a");
        val.put("value", 1);

        Map<String, Object> val2 = new HashMap<>();
        val2.put("field1", "a");
        val2.put("field2", "a");
        val2.put("field3", "b");
        val2.put("value", 2);

        Map<String, Object> val3 = new HashMap<>();
        val3.put("field1", "a");
        val3.put("field2", "a");
        val3.put("field3", "c");
        val3.put("value", 3);

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
        SQL4JsonInput input = SQL4JsonInput.fromObject(jql, Arrays.asList(val, val2, val3));
        SQL4JsonProcessor processor = new SQL4JsonProcessor(input);
        JsonNode result = processor.getResult().get(0);

        assertAll(
                () -> assertEquals("a", result.get("field1").asText()),
                () -> assertEquals("a", result.get("field2").asText()),
                () -> assertEquals(6, result.get("total").asInt()),
                () -> assertEquals(3, result.get("max_val").asInt()),
                () -> assertEquals(1, result.get("min_val").asInt()),
                () -> assertEquals(2, result.get("avg_val").asInt()),
                () -> assertEquals(3, result.get("cnt").asInt())
        );
    }

}
