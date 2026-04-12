package io.github.mnesimiyilmaz.sql4json.exception;

/**
 * Thrown when the SQL query cannot be parsed. Provides the line and character position of the error.
 */
public final class SQL4JsonParseException extends SQL4JsonException {

    /**
     * 1-based line number where the error occurred.
     */
    private final int line;
    /**
     * 0-based character offset within the line.
     */
    private final int charPosition;

    /**
     * Creates a parse exception with the given message and error position.
     *
     * @param message      detail message
     * @param line         1-based line number where the error occurred
     * @param charPosition 0-based character offset within the line
     */
    public SQL4JsonParseException(String message, int line, int charPosition) {
        super(message);
        this.line = line;
        this.charPosition = charPosition;
    }

    /**
     * Creates a parse exception with the given message, error position, and cause.
     *
     * @param message      detail message
     * @param line         1-based line number where the error occurred
     * @param charPosition 0-based character offset within the line
     * @param cause        underlying cause
     */
    public SQL4JsonParseException(String message, int line, int charPosition, Throwable cause) {
        super(message, cause);
        this.line = line;
        this.charPosition = charPosition;
    }

    /**
     * Returns the 1-based line number where the parse error occurred.
     * Returns 0 if the position could not be determined.
     *
     * @return the error line number (1-based), or 0
     */
    public int getLine() {
        return line;
    }

    /**
     * Returns the 0-based character offset within the line where the parse error occurred.
     * Returns 0 if the position could not be determined.
     *
     * @return the character position (0-based), or 0
     */
    public int getCharPosition() {
        return charPosition;
    }

}
