package io.github.mnesimiyilmaz.sql4json.mapper;

import io.github.mnesimiyilmaz.sql4json.json.JsonNumberValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.settings.MappingSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonValueMapperPojoTest {

    public static class Employee {
        private String name;
        private int    age;

        public void setName(String n) {
            this.name = n;
        }

        public void setAge(int a) {
            this.age = a;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }
    }

    public static class Fluent {
        private String name;

        public Fluent setName(String n) {
            this.name = n;
            return this;
        }

        public String getName() {
            return name;
        }
    }

    public static class Parent {
        private String label;

        public void setLabel(String l) {
            this.label = l;
        }

        public String getLabel() {
            return label;
        }
    }

    public static class Child extends Parent {
        private int count;

        public void setCount(int c) {
            this.count = c;
        }

        public int getCount() {
            return count;
        }
    }

    private static final MappingSettings S = MappingSettings.defaults();

    private static JsonValue obj(Map<String, JsonValue> m) {
        return new JsonObjectValue(new LinkedHashMap<>(m));
    }

    @Test
    void when_pojo_mapped_from_json_object_then_setters_invoked() {
        JsonValue v = obj(Map.of(
                "name", new JsonStringValue("Alice"),
                "age", new JsonNumberValue(30)));
        Employee e = JsonValueMapper.INSTANCE.map(v, Employee.class, S);
        assertEquals("Alice", e.getName());
        assertEquals(30, e.getAge());
    }

    @Test
    void when_pojo_has_fluent_setter_then_return_value_ignored_but_value_stored() {
        JsonValue v = obj(Map.of("name", new JsonStringValue("Bob")));
        Fluent f = JsonValueMapper.INSTANCE.map(v, Fluent.class, S);
        assertEquals("Bob", f.getName());
    }

    @Test
    void when_pojo_inherits_setter_then_both_levels_populated() {
        JsonValue v = obj(Map.of(
                "label", new JsonStringValue("L"),
                "count", new JsonNumberValue(5)));
        Child c = JsonValueMapper.INSTANCE.map(v, Child.class, S);
        assertEquals("L", c.getLabel());
        assertEquals(5, c.getCount());
    }

    @Test
    void when_pojo_has_extra_json_key_then_ignored() {
        JsonValue v = obj(Map.of(
                "name", new JsonStringValue("X"),
                "age", new JsonNumberValue(1),
                "extra", new JsonStringValue("ignored")));
        Employee e = JsonValueMapper.INSTANCE.map(v, Employee.class, S);
        assertEquals("X", e.getName());
    }
}
