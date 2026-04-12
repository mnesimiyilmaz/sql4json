package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.types.*;

/**
 * Converts between {@link JsonValue} and {@link SqlValue} representations.
 */
public final class JsonToSqlConverter {

    private JsonToSqlConverter() {
    }

    /**
     * Converts a {@link JsonValue} to its corresponding {@link SqlValue}.
     * Throws for objects and arrays that have not been flattened.
     *
     * @param json the JSON value to convert
     * @return the corresponding SQL value
     */
    public static SqlValue toSqlValue(JsonValue json) {
        return switch (json) {
            case JsonStringValue(var value) -> new SqlString(value);
            case JsonNumberValue(var value) -> SqlNumber.of(value);
            case JsonBooleanValue(var value) -> SqlBoolean.of(value);
            case JsonNullValue ignored -> SqlNull.INSTANCE;
            case JsonObjectValue ignored ->
                    throw new SQL4JsonExecutionException("Cannot convert JSON object to SqlValue — flatten first");
            case JsonArrayValue ignored ->
                    throw new SQL4JsonExecutionException("Cannot convert JSON array to SqlValue — flatten first");
        };
    }

    /**
     * Safe variant: returns SqlNull for objects and arrays instead of throwing.
     * Used when the caller will fall back to original JsonValue navigation (e.g. cherryPick).
     *
     * @param json the JSON value to convert
     * @return the corresponding SQL value, or {@link SqlNull} for objects and arrays
     */
    public static SqlValue toSqlValueSafe(JsonValue json) {
        return switch (json) {
            case JsonStringValue(var value) -> new SqlString(value);
            case JsonNumberValue(var value) -> SqlNumber.of(value);
            case JsonBooleanValue(var value) -> SqlBoolean.of(value);
            case JsonNullValue ignored -> SqlNull.INSTANCE;
            case JsonObjectValue ignored -> SqlNull.INSTANCE;
            case JsonArrayValue ignored -> SqlNull.INSTANCE;
        };
    }

    /**
     * Converts a {@link SqlValue} back to its corresponding {@link JsonValue}.
     *
     * @param sql the SQL value to convert
     * @return the corresponding JSON value
     */
    public static JsonValue toJsonValue(SqlValue sql) {
        return switch (sql) {
            case SqlString(var value) -> new JsonStringValue(value);
            case SqlNumber(var value) -> new JsonNumberValue(value);
            case SqlBoolean(var value) -> value ? JsonBooleanValue.TRUE : JsonBooleanValue.FALSE;
            case SqlDate(var value) -> new JsonStringValue(value.toString());
            case SqlDateTime(var value) -> new JsonStringValue(value.toString());
            case SqlNull ignored -> JsonNullValue.INSTANCE;
        };
    }
}
