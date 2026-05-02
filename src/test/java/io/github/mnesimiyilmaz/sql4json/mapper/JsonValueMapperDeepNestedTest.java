// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.settings.MappingSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JsonValueMapperDeepNestedTest {

    record Country(String iso2, String name) {}

    record City(String name, Country country) {}

    record Address(String street, City city) {}

    record Employee(String name, Address address) {}

    record Team(String name, List<Employee> members) {}

    record Department(String name, List<Team> teams) {}

    record Company(String name, List<Department> departments) {}

    record Product(String sku, Set<String> tags, Map<String, BigDecimal> prices) {}

    private static final MappingSettings S = MappingSettings.defaults();

    private static JsonValue obj(Map<String, JsonValue> m) {
        return new JsonObjectValue(new LinkedHashMap<>(m));
    }

    private static JsonValue arr(JsonValue... elems) {
        return new JsonArrayValue(List.of(elems));
    }

    @Test
    void when_seven_level_record_chain_then_entire_tree_populates() {
        JsonValue tr = obj(Map.of("iso2", new JsonStringValue("TR"), "name", new JsonStringValue("Türkiye")));
        JsonValue ist = obj(Map.of("name", new JsonStringValue("Istanbul"), "country", tr));
        JsonValue addr = obj(Map.of("street", new JsonStringValue("Main 1"), "city", ist));
        JsonValue emp = obj(Map.of("name", new JsonStringValue("Ada"), "address", addr));
        JsonValue team = obj(Map.of("name", new JsonStringValue("A"), "members", arr(emp)));
        JsonValue dept = obj(Map.of("name", new JsonStringValue("Eng"), "teams", arr(team)));
        JsonValue co = obj(Map.of("name", new JsonStringValue("Acme"), "departments", arr(dept)));

        Company c = JsonValueMapper.INSTANCE.map(co, Company.class, S);
        assertEquals(
                "TR",
                c.departments()
                        .get(0)
                        .teams()
                        .get(0)
                        .members()
                        .get(0)
                        .address()
                        .city()
                        .country()
                        .iso2());
    }

    @Test
    void when_product_has_set_and_map_of_bigdecimal_then_mapped() {
        JsonValue v = obj(Map.of(
                "sku", new JsonStringValue("p1"),
                "tags", arr(new JsonStringValue("a"), new JsonStringValue("b")),
                "prices",
                        obj(Map.of(
                                "usd", new JsonStringValue("9.99"),
                                "eur", new JsonStringValue("8.49")))));
        Product p = JsonValueMapper.INSTANCE.map(v, Product.class, S);
        assertEquals(Set.of("a", "b"), p.tags());
        assertEquals(new BigDecimal("9.99"), p.prices().get("usd"));
    }
}
