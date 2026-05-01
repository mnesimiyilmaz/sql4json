package io.github.mnesimiyilmaz.sql4json.parser;

/**
 * Describes the syntactic position of a parameter placeholder. Used by the
 * bind-time validator to check the bound value's type against the position's
 * expectations.
 *
 * @since 1.2.0
 */
public enum ParameterPositionKind {
    /**
     * Regular scalar slot — comparison RHS, function argument, LIMIT/OFFSET, etc.
     */
    REGULAR_SCALAR,
    /**
     * Inside an {@code IN(...)} list — collection bind expands; scalar bind is one value.
     */
    IN_LIST,
    /**
     * Inside {@code ARRAY[?]} — must bind to a scalar (no expansion).
     */
    ARRAY_ELEMENT,
    /**
     * Bare RHS of {@code @>} / {@code <@} / {@code &&} or array equality — must bind to a Collection.
     */
    BARE_ARRAY_RHS
}
