// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.mapper;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Small helpers for unpacking a {@link Type} into its raw class and type arguments. Used by {@link JsonValueMapper} to
 * handle {@code List<T>}, {@code Map<String,V>}, {@code Optional<T>}, {@code T[]}, etc.
 */
final class TypeIntrospection {

    private TypeIntrospection() {}

    static Class<?> rawType(Type t) {
        if (t instanceof Class<?> c) return c;
        if (t instanceof ParameterizedType pt) return (Class<?>) pt.getRawType();
        if (t instanceof GenericArrayType gat) {
            Class<?> componentClass = rawType(gat.getGenericComponentType());
            return java.lang.reflect.Array.newInstance(componentClass, 0).getClass();
        }
        throw new IllegalArgumentException("Unsupported Type: " + t);
    }

    /** @return the i-th type argument of a {@link ParameterizedType}, or {@link Object} if none */
    static Type typeArg(Type t, int i) {
        if (t instanceof ParameterizedType pt) {
            Type[] args = pt.getActualTypeArguments();
            if (i < args.length) return args[i];
        }
        return Object.class;
    }
}
