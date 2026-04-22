package io.github.mnesimiyilmaz.sql4json.exception;

/**
 * Thrown when {@code JsonValue → Java} object mapping fails. Categories include:
 * <ul>
 *   <li>Null mapped to a primitive target</li>
 *   <li>Unsupported target type (interface / abstract class / missing constructor)</li>
 *   <li>Enum constant not matched</li>
 *   <li>Setter threw (wrapped with cause)</li>
 *   <li>Record single-row unwrap violated (0 or >1 rows for scalar/record target)</li>
 *   <li>Missing required field with {@link io.github.mnesimiyilmaz.sql4json.settings.MissingFieldPolicy#FAIL}</li>
 *   <li>POJO cycle detected</li>
 * </ul>
 *
 * <p>Messages include a JSON path suffix (e.g. {@code $.orders[2].amount}) where meaningful.
 *
 * @see SQL4JsonException
 * @since 1.1.0
 */
public final class SQL4JsonMappingException extends SQL4JsonException {

    /**
     * Creates a mapping exception with the given message.
     *
     * @param message detail message
     */
    public SQL4JsonMappingException(String message) {
        super(message);
    }

    /**
     * Creates a mapping exception with the given message and cause.
     *
     * @param message detail message
     * @param cause   underlying cause (e.g. setter-thrown exception)
     */
    public SQL4JsonMappingException(String message, Throwable cause) {
        super(message, cause);
    }
}
