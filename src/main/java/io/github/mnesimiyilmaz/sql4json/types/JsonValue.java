package io.github.mnesimiyilmaz.sql4json.types;

import io.github.mnesimiyilmaz.sql4json.json.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A sealed interface representing a JSON value. This is the library's own JSON abstraction,
 * independent of any JSON library.
 *
 * <p>Use the type-check methods ({@link #isObject()}, {@link #isArray()}, etc.) to determine
 * the runtime type, then call the corresponding accessor ({@link #asObject()}, {@link #asArray()},
 * etc.) to safely extract the underlying value as an {@link Optional}.
 *
 * <p>Implementations are Java records, so they support {@code equals}, {@code hashCode},
 * and {@code toString} out of the box.
 *
 * <h2>Example usage:</h2>
 * <pre>{@code
 * JsonValue result = SQL4Json.queryAsJsonValue("SELECT name FROM $r", json);
 * result.asArray().ifPresent(rows -> {
 *     for (JsonValue row : rows) {
 *         row.asObject().ifPresent(fields -> {
 *             fields.get("name").asString().ifPresent(System.out::println);
 *         });
 *     }
 * });
 * }</pre>
 *
 * @see io.github.mnesimiyilmaz.sql4json.SQL4Json#queryAsJsonValue(String, String)
 */
public sealed interface JsonValue
        permits JsonObjectValue, JsonArrayValue, JsonStringValue,
        JsonNumberValue, JsonBooleanValue, JsonNullValue {

    /**
     * Returns {@code true} if this value is JSON {@code null}.
     *
     * @return {@code true} if null, {@code false} otherwise
     */
    boolean isNull();

    /**
     * Returns {@code true} if this value is a JSON object (key-value map).
     *
     * @return {@code true} if an object, {@code false} otherwise
     */
    boolean isObject();

    /**
     * Returns {@code true} if this value is a JSON array.
     *
     * @return {@code true} if an array, {@code false} otherwise
     */
    boolean isArray();

    /**
     * Returns {@code true} if this value is a JSON number (integer or floating-point).
     *
     * @return {@code true} if a number, {@code false} otherwise
     */
    boolean isNumber();

    /**
     * Returns {@code true} if this value is a JSON string.
     *
     * @return {@code true} if a string, {@code false} otherwise
     */
    boolean isString();

    /**
     * Returns {@code true} if this value is a JSON boolean.
     *
     * @return {@code true} if a boolean, {@code false} otherwise
     */
    boolean isBoolean();

    /**
     * Returns the value as a JSON object (map of field names to values).
     *
     * @return the fields if this is an object, or {@link Optional#empty()} otherwise
     */
    Optional<Map<String, JsonValue>> asObject();

    /**
     * Returns the value as a JSON array (ordered list of values).
     *
     * @return the elements if this is an array, or {@link Optional#empty()} otherwise
     */
    Optional<List<JsonValue>> asArray();

    /**
     * Returns the value as a {@link Number}.
     *
     * @return the number if this is a numeric value, or {@link Optional#empty()} otherwise
     */
    Optional<Number> asNumber();

    /**
     * Returns the value as a {@link String}.
     *
     * @return the string if this is a string value, or {@link Optional#empty()} otherwise
     */
    Optional<String> asString();

    /**
     * Returns the value as a {@link Boolean}.
     *
     * @return the boolean if this is a boolean value, or {@link Optional#empty()} otherwise
     */
    Optional<Boolean> asBoolean();
}
