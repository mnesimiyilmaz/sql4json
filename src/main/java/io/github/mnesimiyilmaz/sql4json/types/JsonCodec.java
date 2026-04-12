package io.github.mnesimiyilmaz.sql4json.types;

/**
 * Strategy interface for JSON parsing and serialization.
 *
 * <p>The library ships with a built-in implementation; users may provide their own
 * to integrate a different JSON library (Jackson, Gson, etc.).</p>
 *
 * <pre>{@code
 * JsonCodec myCodec = new MyGsonCodec();
 * String result = SQL4Json.query(sql, json, myCodec);
 * }</pre>
 *
 * @see io.github.mnesimiyilmaz.sql4json.SQL4Json
 */
public interface JsonCodec {

    /**
     * Parse a JSON string into a {@link JsonValue}.
     *
     * @param json valid JSON string
     * @return parsed JsonValue
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException on malformed input
     */
    JsonValue parse(String json);

    /**
     * Serialize a {@link JsonValue} to a compact JSON string.
     *
     * @param value the value to serialize
     * @return JSON string
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException on serialization failure
     */
    String serialize(JsonValue value);
}
