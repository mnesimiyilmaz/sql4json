// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.sorting.SqlValueComparator;
import io.github.mnesimiyilmaz.sql4json.types.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * Registry of comparison operators (e.g. {@code =}, {@code !=}, {@code >}). Holds a map of operator symbols to their
 * predicate implementations. The default instance is frozen (immutable) after creation.
 */
public final class OperatorRegistry {

    private Map<String, ComparisonOperatorDef> operators = new HashMap<>();

    private static final OperatorRegistry DEFAULT = createDefault();

    /** Creates a new empty {@code OperatorRegistry}. */
    public OperatorRegistry() {
        // Operator map is initialized eagerly; callers populate it via register().
    }

    /**
     * Returns the shared default operator registry containing the standard SQL comparison operators.
     *
     * @return the default OperatorRegistry
     */
    public static OperatorRegistry getDefault() {
        return DEFAULT;
    }

    /** Freezes this registry, making the operator map unmodifiable. */
    public void freeze() {
        operators = Collections.unmodifiableMap(operators);
    }

    /**
     * Registers a comparison operator definition.
     *
     * @param op the operator definition to register
     */
    public void register(ComparisonOperatorDef op) {
        operators.put(op.symbol(), op);
    }

    /**
     * Returns the predicate for the given operator symbol.
     *
     * @param symbol the operator symbol (e.g. {@code "="}, {@code "!="})
     * @return the comparison predicate
     * @throws SQL4JsonExecutionException if the symbol is not registered
     */
    public BiPredicate<SqlValue, SqlValue> getPredicate(String symbol) {
        ComparisonOperatorDef op = operators.get(symbol);
        if (op == null) throw new SQL4JsonExecutionException("Unknown operator: " + symbol);
        return op.predicate();
    }

    /**
     * Creates a new default operator registry pre-populated with standard SQL comparison operators.
     *
     * @return a frozen OperatorRegistry with standard operators
     */
    public static OperatorRegistry createDefault() {
        var r = new OperatorRegistry();

        r.register(new ComparisonOperatorDef("=", OperatorType.BINARY, (a, b) -> switch (a) {
            case SqlNull ignored -> false;
            case SqlNumber lhs ->
                b instanceof SqlNumber rhs && Double.compare(lhs.doubleValue(), rhs.doubleValue()) == 0;
            case SqlString(var value) -> b instanceof SqlString(var other) && value.equals(other);
            case SqlBoolean(var value) -> b instanceof SqlBoolean(var other) && value == other;
            case SqlDate(var value) -> b instanceof SqlDate(var other) && value.equals(other);
            case SqlDateTime(var value) -> b instanceof SqlDateTime(var other) && value.equals(other);
        }));

        // NOTE: The != predicate captures `r` and calls r.getPredicate("=") at evaluation
        // time. This is safe because the registry is fully populated before any predicate
        // is invoked, and getPredicate() only reads from the (frozen) map.
        r.register(new ComparisonOperatorDef(
                "!=", OperatorType.BINARY, (a, b) -> !r.getPredicate("=").test(a, b)));

        r.register(new ComparisonOperatorDef(">", OperatorType.BINARY, (a, b) -> SqlValueComparator.compare(a, b) > 0));

        r.register(new ComparisonOperatorDef("<", OperatorType.BINARY, (a, b) -> SqlValueComparator.compare(a, b) < 0));

        r.register(
                new ComparisonOperatorDef(">=", OperatorType.BINARY, (a, b) -> SqlValueComparator.compare(a, b) >= 0));

        r.register(
                new ComparisonOperatorDef("<=", OperatorType.BINARY, (a, b) -> SqlValueComparator.compare(a, b) <= 0));

        r.freeze();
        return r;
    }
}
