package io.github.mnesimiyilmaz.sql4json.mapper;

import org.junit.jupiter.api.Test;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TypeIntrospectionTest {

    @SuppressWarnings("unused")
    static class Holder<T> {
        List<String>         list;
        Map<String, Integer> map;
        List<String>[]       genericArray;
    }

    @Test
    void rawType_returns_class_directly() {
        assertEquals(String.class, TypeIntrospection.rawType(String.class));
    }

    @Test
    void rawType_unwraps_parameterized_type() throws Exception {
        Type listType = Holder.class.getDeclaredField("list").getGenericType();
        assertEquals(List.class, TypeIntrospection.rawType(listType));
    }

    @Test
    void rawType_handles_generic_array_type() throws Exception {
        Type genericArray = Holder.class.getDeclaredField("genericArray").getGenericType();
        assertInstanceOf(GenericArrayType.class, genericArray);
        Class<?> raw = TypeIntrospection.rawType(genericArray);
        assertTrue(raw.isArray());
        assertEquals(List.class, raw.getComponentType());
    }

    @Test
    void rawType_throws_for_unsupported_type() {
        Type typeVariable = Holder.class.getTypeParameters()[0];
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> TypeIntrospection.rawType(typeVariable));
        assertTrue(ex.getMessage().contains("Unsupported Type"));
    }

    @Test
    void typeArg_returns_argument_for_parameterized_type() throws Exception {
        Type mapType = Holder.class.getDeclaredField("map").getGenericType();
        assertEquals(String.class, TypeIntrospection.typeArg(mapType, 0));
        assertEquals(Integer.class, TypeIntrospection.typeArg(mapType, 1));
    }

    @Test
    void typeArg_returns_object_when_index_out_of_bounds() throws Exception {
        Type listType = Holder.class.getDeclaredField("list").getGenericType();
        assertEquals(Object.class, TypeIntrospection.typeArg(listType, 5));
    }

    @Test
    void typeArg_returns_object_for_non_parameterized_type() {
        assertEquals(Object.class, TypeIntrospection.typeArg(String.class, 0));
    }
}
