// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.mnesimiyilmaz.sql4json.json.JsonLongValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.settings.MappingSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonValueMapperRecordTest {

    private static final MappingSettings S = MappingSettings.defaults();

    record Person(String name, int age, LocalDate birthDate) {}

    record Order(String id, Person customer) {}

    private static JsonValue obj(Map<String, JsonValue> m) {
        return new JsonObjectValue(new LinkedHashMap<>(m));
    }

    @Test
    void when_json_object_mapped_to_flat_record() {
        JsonValue v = obj(Map.of(
                "name", new JsonStringValue("Alice"),
                "age", new JsonLongValue(30L),
                "birthDate", new JsonStringValue("1994-01-15")));
        Person p = JsonValueMapper.INSTANCE.map(v, Person.class, S);
        assertEquals(new Person("Alice", 30, LocalDate.of(1994, 1, 15)), p);
    }

    @Test
    void when_nested_record_then_inner_populated() {
        JsonValue customer = obj(Map.of(
                "name", new JsonStringValue("Bob"),
                "age", new JsonLongValue(40L),
                "birthDate", new JsonStringValue("1984-05-05")));
        JsonValue order = obj(Map.of("id", new JsonStringValue("o1"), "customer", customer));
        Order o = JsonValueMapper.INSTANCE.map(order, Order.class, S);
        assertEquals("o1", o.id());
        assertEquals("Bob", o.customer().name());
        assertEquals(40, o.customer().age());
    }
}
