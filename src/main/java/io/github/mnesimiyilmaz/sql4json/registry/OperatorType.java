package io.github.mnesimiyilmaz.sql4json.registry;

/**
 * Classifies comparison operators by their arity and position.
 */
public enum OperatorType {
    /**
     * A binary operator taking a left-hand and right-hand operand (e.g. {@code =}, {@code >}).
     */
    BINARY,
    /**
     * A unary postfix operator applied after the operand (e.g. IS NULL).
     */
    UNARY_POSTFIX,
    /**
     * A range operator taking a value and two bounds (e.g. BETWEEN).
     */
    RANGE
}
