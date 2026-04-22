package io.github.mnesimiyilmaz.sql4json.mapper;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException;
import io.github.mnesimiyilmaz.sql4json.json.*;
import io.github.mnesimiyilmaz.sql4json.settings.MappingSettings;
import io.github.mnesimiyilmaz.sql4json.settings.MissingFieldPolicy;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps a {@link JsonValue} to a Java target type. Singleton via {@link #INSTANCE}.
 *
 * <p>Entry points:
 * <ul>
 *   <li>{@link #map(JsonValue, Class, MappingSettings)} — public, type-safe</li>
 * </ul>
 *
 * <p>Thread-safe: reflection metadata cached in a {@link ConcurrentHashMap}. No
 * per-call mutable state outside a stack-scoped {@link VisitedStack} and
 * {@link MappingPath}.
 */
@SuppressWarnings("java:S6548") // Intentional singleton — stateless mapper with a shared reflection cache.
public final class JsonValueMapper {

    /**
     * Shared singleton.
     */
    public static final JsonValueMapper INSTANCE = new JsonValueMapper();

    private final ConcurrentHashMap<Class<?>, TypeDescriptor> descriptorCache = new ConcurrentHashMap<>();

    private JsonValueMapper() {
    }

    /**
     * Map {@code value} to an instance of {@code type}.
     *
     * @param value    JSON value to map
     * @param type     target class
     * @param settings mapping settings (missing-field policy, etc.)
     * @param <T>      target type
     * @return mapped instance of {@code type}
     * @throws SQL4JsonMappingException on any mapping failure (null-to-primitive,
     *                                  unsupported target, enum mismatch, etc.)
     */
    @SuppressWarnings("unchecked")
    public <T> T map(JsonValue value, Class<T> type, MappingSettings settings) {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(settings, "settings");
        Object result = mapInternal(value, type, MappingPath.root(), new VisitedStack(), settings);
        return (T) result;
    }

    Object mapInternal(JsonValue value, Type targetType,
                       MappingPath path, VisitedStack visited, MappingSettings settings) {
        Class<?> rawType = TypeIntrospection.rawType(targetType);

        // JsonValue passthrough — target accepts the raw tree (before null handling so
        // JsonNullValue maps to the JsonNullValue instance, not Java null)
        if (JsonValue.class.isAssignableFrom(rawType) && rawType.isInstance(value)) {
            return value;
        }

        // Null handling first — applies across all raw types
        if (value instanceof JsonNullValue) {
            return handleNull(rawType, path);
        }

        // Object passthrough — recursively convert to natural Java types.
        // Placed after null handling so null + Object.class → Java null via handleNull.
        if (rawType == Object.class) {
            return naturalValue(value, path, visited, settings);
        }

        // Optional unwraps to its element type
        if (rawType == Optional.class) {
            Type inner = TypeIntrospection.typeArg(targetType, 0);
            Object mapped = mapInternal(value, inner, path, visited, settings);
            return Optional.ofNullable(mapped);
        }

        Object scalar = mapScalar(value, rawType, path);
        if (scalar != NOT_SCALAR) return scalar;

        if (value instanceof JsonArrayValue(List<JsonValue> elements)) {
            return mapArrayLike(elements, targetType, rawType, path, visited, settings);
        }

        if (value instanceof JsonObjectValue(Map<String, JsonValue> fields)) {
            return mapObjectLike(fields, targetType, rawType, path, visited, settings);
        }

        throw new SQL4JsonMappingException(
                "Unsupported target " + rawType.getName() + " for value " + value + " at " + path);
    }

    /**
     * Sentinel returned by {@link #mapScalar} when the value is not a scalar JSON type.
     */
    private static final Object NOT_SCALAR = new Object();

    private Object mapScalar(JsonValue value, Class<?> rawType, MappingPath path) {
        if (value instanceof JsonBooleanValue(boolean b)) {
            return mapBoolean(b, rawType, path);
        }
        if (value instanceof JsonNumberValue(Number n)) {
            return mapNumber(n, rawType, path);
        }
        if (value instanceof JsonStringValue(String s)) {
            return mapString(s, rawType, path);
        }
        return NOT_SCALAR;
    }

    private Object mapArrayLike(List<JsonValue> elements, Type targetType, Class<?> rawType,
                                MappingPath path, VisitedStack visited, MappingSettings settings) {
        if (rawType.isArray()) {
            Class<?> componentType = rawType.getComponentType();
            Object array = Array.newInstance(componentType, elements.size());
            for (int i = 0; i < elements.size(); i++) {
                Object elem = mapInternal(elements.get(i), componentType,
                        path.index(i), visited, settings);
                Array.set(array, i, elem);
            }
            return array;
        }
        if (Collection.class.isAssignableFrom(rawType) || rawType == Iterable.class) {
            Type elementType = TypeIntrospection.typeArg(targetType, 0);
            Collection<Object> out;
            if (rawType == Set.class || rawType == LinkedHashSet.class) {
                out = new LinkedHashSet<>();
            } else {
                out = new ArrayList<>(elements.size());  // List, Collection, Iterable
            }
            int i = 0;
            for (JsonValue elem : elements) {
                out.add(mapInternal(elem, elementType, path.index(i), visited, settings));
                i++;
            }
            return out;
        }
        throw new SQL4JsonMappingException(
                "Unsupported target " + rawType.getName() + " for array value at " + path);
    }

    private Object mapObjectLike(Map<String, JsonValue> fields, Type targetType, Class<?> rawType,
                                 MappingPath path, VisitedStack visited, MappingSettings settings) {
        if (Map.class.isAssignableFrom(rawType)) {
            Type keyType = TypeIntrospection.typeArg(targetType, 0);
            if (keyType != String.class && keyType != Object.class) {
                throw new SQL4JsonMappingException(
                        "Map keys must be String, got " + keyType + " at " + path);
            }
            Type valueType = TypeIntrospection.typeArg(targetType, 1);
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<String, JsonValue> entry : fields.entrySet()) {
                out.put(entry.getKey(),
                        mapInternal(entry.getValue(), valueType,
                                path.key(entry.getKey()), visited, settings));
            }
            return out;
        }
        if (rawType.isRecord()) {
            return mapRecord(fields, rawType, path, visited, settings);
        }
        if (!rawType.isInterface() && !Modifier.isAbstract(rawType.getModifiers())) {
            return mapPojo(fields, rawType, path, visited, settings);
        }
        throw new SQL4JsonMappingException(
                "Unsupported target " + rawType.getName() + " for object value at " + path);
    }

    private Object mapNumber(Number n, Class<?> rawType, MappingPath path) {
        Object primitive = mapNumberToPrimitive(n, rawType);
        if (primitive != null) return primitive;
        if (rawType == BigDecimal.class) return new BigDecimal(n.toString());
        if (rawType == BigInteger.class) return numberToBigInteger(n, path);
        if (rawType == Number.class) return n;
        if (rawType == Instant.class) return Instant.ofEpochMilli(n.longValue());
        if (rawType == String.class || rawType == CharSequence.class) return n.toString();
        if (rawType == Object.class) return n;
        throw new SQL4JsonMappingException(
                "Cannot map number to " + rawType.getName() + " at " + path);
    }

    private static Object mapNumberToPrimitive(Number n, Class<?> rawType) {
        if (rawType == byte.class || rawType == Byte.class) return n.byteValue();
        if (rawType == short.class || rawType == Short.class) return n.shortValue();
        if (rawType == int.class || rawType == Integer.class) return n.intValue();
        if (rawType == long.class || rawType == Long.class) return n.longValue();
        if (rawType == float.class || rawType == Float.class) return n.floatValue();
        if (rawType == double.class || rawType == Double.class) return n.doubleValue();
        if (rawType == char.class || rawType == Character.class) return (char) n.intValue();
        return null;
    }

    private static BigInteger numberToBigInteger(Number n, MappingPath path) {
        try {
            return new BigInteger(n.toString());
        } catch (NumberFormatException e) {
            throw new SQL4JsonMappingException(
                    "Cannot map non-integer number '" + n + "' to BigInteger at " + path, e);
        }
    }

    private Object mapString(String s, Class<?> rawType, MappingPath path) {
        if (rawType == String.class || rawType == CharSequence.class || rawType == Object.class) return s;
        if (rawType.isEnum()) return stringToEnum(s, rawType, path);
        Object temporal = stringToTemporal(s, rawType, path);
        if (temporal != null) return temporal;
        if (rawType == BigDecimal.class) return stringToBigDecimal(s, path);
        if (rawType == BigInteger.class) return stringToBigInteger(s, path);
        throw new SQL4JsonMappingException(
                "Cannot map string to " + rawType.getName() + " at " + path);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object stringToEnum(String s, Class<?> rawType, MappingPath path) {
        try {
            return Enum.valueOf((Class) rawType, s);
        } catch (IllegalArgumentException e) {
            throw new SQL4JsonMappingException(
                    "Invalid enum value '" + s + "' for " + rawType.getName()
                            + " at " + path + ". Available: "
                            + Arrays.toString(rawType.getEnumConstants()), e);
        }
    }

    private static Object stringToTemporal(String s, Class<?> rawType, MappingPath path) {
        if (rawType == LocalDate.class) {
            LocalDate d = IsoTemporals.tryParseDate(s);
            if (d == null) throw new SQL4JsonMappingException(
                    "Cannot parse '" + s + "' as ISO date at " + path);
            return d;
        }
        if (rawType == LocalDateTime.class) {
            LocalDateTime dt = IsoTemporals.tryParseDateTime(s);
            if (dt == null) throw new SQL4JsonMappingException(
                    "Cannot parse '" + s + "' as ISO datetime at " + path);
            return dt;
        }
        if (rawType == Instant.class) {
            Instant i = IsoTemporals.tryParseInstant(s);
            if (i == null) throw new SQL4JsonMappingException(
                    "Cannot parse '" + s + "' as ISO instant at " + path);
            return i;
        }
        return null;
    }

    private static BigDecimal stringToBigDecimal(String s, MappingPath path) {
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            throw new SQL4JsonMappingException(
                    "Cannot parse '" + s + "' as BigDecimal at " + path, e);
        }
    }

    private static BigInteger stringToBigInteger(String s, MappingPath path) {
        try {
            return new BigInteger(s);
        } catch (NumberFormatException e) {
            throw new SQL4JsonMappingException(
                    "Cannot parse '" + s + "' as BigInteger at " + path, e);
        }
    }

    private Object mapBoolean(boolean b, Class<?> rawType, MappingPath path) {
        if (rawType == boolean.class || rawType == Boolean.class) return b;
        if (rawType == String.class || rawType == CharSequence.class) return Boolean.toString(b);
        if (rawType == Object.class) return b;
        throw new SQL4JsonMappingException(
                "Cannot map boolean to " + rawType.getName() + " at " + path);
    }

    private Object mapRecord(Map<String, JsonValue> fields, Class<?> rawType,
                             MappingPath path, VisitedStack visited, MappingSettings settings) {
        TypeDescriptor.RecordDescriptor desc = (TypeDescriptor.RecordDescriptor)
                descriptorCache.computeIfAbsent(rawType, TypeDescriptor::build);
        Object[] args = new Object[desc.components().length];
        for (int i = 0; i < desc.components().length; i++) {
            var comp = desc.components()[i];
            Type compType = desc.componentGenericTypes()[i];
            JsonValue fieldValue = fields.get(comp.getName());
            if (fieldValue == null) {
                args[i] = missingFieldValue(comp.getType(), path.field(comp.getName()), settings);
            } else {
                args[i] = mapInternal(fieldValue, compType,
                        path.field(comp.getName()), visited, settings);
            }
        }
        try {
            return desc.canonicalConstructor().newInstance(args);
        } catch (InvocationTargetException e) {
            throw new SQL4JsonMappingException(
                    "Record constructor threw at " + path + ": " + e.getCause().getMessage(), e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new SQL4JsonMappingException(
                    "Failed to instantiate record " + rawType.getName() + " at " + path, e);
        }
    }

    private Object mapPojo(Map<String, JsonValue> fields, Class<?> rawType,
                           MappingPath path, VisitedStack visited, MappingSettings settings) {
        if (visited.contains(fields, rawType)) {
            throw new SQL4JsonMappingException("Cycle detected at " + path);
        }
        visited.push(fields, rawType);
        try {
            TypeDescriptor.PojoDescriptor desc = (TypeDescriptor.PojoDescriptor)
                    descriptorCache.computeIfAbsent(rawType, TypeDescriptor::build);
            Object instance;
            try {
                instance = desc.noArgConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new SQL4JsonMappingException(
                        "Failed to instantiate " + rawType.getName() + " at " + path, e);
            }

            for (Map.Entry<String, TypeDescriptor.PojoDescriptor.SetterInfo> entry
                    : desc.settersByProperty().entrySet()) {
                String prop = entry.getKey();
                TypeDescriptor.PojoDescriptor.SetterInfo setter = entry.getValue();
                JsonValue fieldValue = fields.get(prop);
                if (fieldValue == null) {
                    if (settings.missingFieldPolicy() == MissingFieldPolicy.FAIL) {
                        throw new SQL4JsonMappingException(
                                "Missing field at " + path.field(prop));
                    }
                    continue;
                }
                Object arg = mapInternal(fieldValue, setter.paramType(),
                        path.field(prop), visited, settings);
                try {
                    setter.method().invoke(instance, arg);
                } catch (InvocationTargetException e) {
                    throw new SQL4JsonMappingException(
                            "Setter " + setter.method().getName() + " threw at " + path.field(prop)
                                    + ": " + e.getCause().getMessage(), e.getCause());
                } catch (ReflectiveOperationException e) {
                    throw new SQL4JsonMappingException(
                            "Failed to invoke setter " + setter.method().getName()
                                    + " at " + path.field(prop), e);
                }
            }
            return instance;
        } finally {
            visited.pop(fields);
        }
    }

    private Object missingFieldValue(Class<?> componentType, MappingPath path, MappingSettings settings) {
        if (settings.missingFieldPolicy() == MissingFieldPolicy.FAIL) {
            throw new SQL4JsonMappingException("Missing field at " + path);
        }
        // IGNORE: defaults
        if (componentType == Optional.class) return Optional.empty();
        if (componentType.isPrimitive()) return primitiveDefault(componentType);
        if (componentType.isArray()) return Array.newInstance(componentType.getComponentType(), 0);
        if (Set.class.isAssignableFrom(componentType)) return new LinkedHashSet<>();
        if (Collection.class.isAssignableFrom(componentType) || componentType == Iterable.class) {
            return new ArrayList<>();
        }
        if (Map.class.isAssignableFrom(componentType)) return new LinkedHashMap<>();
        return null;
    }

    private Object primitiveDefault(Class<?> t) {
        if (t == boolean.class) return false;
        if (t == char.class) return '\0';
        if (t == byte.class) return (byte) 0;
        if (t == short.class) return (short) 0;
        if (t == int.class) return 0;
        if (t == long.class) return 0L;
        if (t == float.class) return 0f;
        if (t == double.class) return 0d;
        throw new IllegalStateException("Unknown primitive " + t);
    }

    private Object naturalValue(JsonValue v, MappingPath path,
                                VisitedStack visited, MappingSettings settings) {
        if (v instanceof JsonNullValue) return null;
        if (v instanceof JsonBooleanValue(boolean b)) return b;
        if (v instanceof JsonNumberValue(Number n)) return n;
        if (v instanceof JsonStringValue(String s)) return s;
        if (v instanceof JsonArrayValue(List<JsonValue> elements)) {
            List<Object> out = new ArrayList<>(elements.size());
            int i = 0;
            for (JsonValue el : elements) {
                out.add(naturalValue(el, path.index(i), visited, settings));
                i++;
            }
            return out;
        }
        JsonObjectValue jo = (JsonObjectValue) v;
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, JsonValue> e : jo.fields().entrySet()) {
            out.put(e.getKey(), naturalValue(e.getValue(), path.key(e.getKey()), visited, settings));
        }
        return out;
    }

    private Object handleNull(Class<?> rawType, MappingPath path) {
        if (rawType == Optional.class) return Optional.empty();
        if (rawType.isPrimitive()) {
            throw new SQL4JsonMappingException(
                    "Cannot map null to primitive " + rawType.getName() + " at " + path);
        }
        return null;
    }
}
