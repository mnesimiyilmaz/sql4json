package io.github.mnesimiyilmaz.sql4json.mapper;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException;

import java.lang.reflect.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Cached reflection metadata for a mapping target. Either a record (canonical
 * constructor + components) or a POJO (no-arg constructor + setter map).
 *
 * <p>Instances are built once per {@link Class} and stored in
 * {@link JsonValueMapper}'s {@code ConcurrentHashMap} cache.
 */
sealed interface TypeDescriptor permits TypeDescriptor.RecordDescriptor, TypeDescriptor.PojoDescriptor {

    Class<?> type();

    /**
     * @throws SQL4JsonMappingException if {@code type} is neither a record nor a POJO
     *                                  with a public no-arg constructor
     */
    static TypeDescriptor build(Class<?> type) {
        if (type.isRecord()) return RecordDescriptor.build(type);
        return PojoDescriptor.build(type);
    }

    // S6218: array-valued components are fine — RecordDescriptor is keyed by Class in the mapper cache
    //        and never compared for structural equality.
    @SuppressWarnings("java:S6218")
    record RecordDescriptor(Class<?> type,
                            Constructor<?> canonicalConstructor,
                            RecordComponent[] components,
                            Type[] componentGenericTypes) implements TypeDescriptor {

        // S3011: setAccessible is required — user records may live in a module that does not
        //        open to this library, or in a non-public scope.
        @SuppressWarnings("java:S3011")
        static RecordDescriptor build(Class<?> type) {
            RecordComponent[] comps = type.getRecordComponents();
            Class<?>[] paramTypes = Arrays.stream(comps)
                    .map(RecordComponent::getType)
                    .toArray(Class<?>[]::new);
            try {
                Constructor<?> ctor = type.getDeclaredConstructor(paramTypes);
                ctor.setAccessible(true);
                Type[] genericTypes = Arrays.stream(comps)
                        .map(RecordComponent::getGenericType)
                        .toArray(Type[]::new);
                return new RecordDescriptor(type, ctor, comps, genericTypes);
            } catch (NoSuchMethodException e) {
                throw new SQL4JsonMappingException(
                        "Cannot find canonical constructor for record " + type.getName(), e);
            }
        }
    }

    record PojoDescriptor(Class<?> type,
                          Constructor<?> noArgConstructor,
                          Map<String, SetterInfo> settersByProperty) implements TypeDescriptor {

        record SetterInfo(Method method, Type paramType) {
        }

        static PojoDescriptor build(Class<?> type) {
            Constructor<?> ctor;
            try {
                ctor = type.getConstructor();
            } catch (NoSuchMethodException e) {
                throw new SQL4JsonMappingException(
                        "No public no-arg constructor for " + type.getName(), e);
            }

            Method[] allMethods = type.getMethods();
            java.util.List<Method> setterList = new java.util.ArrayList<>();
            for (Method m : allMethods) {
                if (isSetter(m)) setterList.add(m);
            }
            setterList.sort(Comparator.comparing(Method::getName)
                    .thenComparing(m -> m.getParameterTypes()[0].getName()));

            Map<String, SetterInfo> setters = new LinkedHashMap<>();
            for (Method m : setterList) {
                String prop = propertyName(m.getName());
                SetterInfo existing = setters.get(prop);
                if (existing == null
                        || isMoreSpecific(m.getParameterTypes()[0], existing.method.getParameterTypes()[0])) {
                    setters.put(prop, new SetterInfo(m, m.getGenericParameterTypes()[0]));
                }
            }
            return new PojoDescriptor(type, ctor, Map.copyOf(setters));
        }

        private static boolean isSetter(Method m) {
            return m.getName().startsWith("set")
                    && m.getName().length() > 3
                    && m.getParameterCount() == 1
                    && !Modifier.isStatic(m.getModifiers())
                    && m.getDeclaringClass() != Object.class;
        }

        private static String propertyName(String setterName) {
            String rest = setterName.substring(3);
            return Character.toLowerCase(rest.charAt(0)) + rest.substring(1);
        }

        private static boolean isMoreSpecific(Class<?> candidate, Class<?> existing) {
            return existing.isAssignableFrom(candidate) && !candidate.equals(existing);
        }
    }
}
