package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Row;

/**
 * Leaf of the WHERE / HAVING criteria tree. Wraps a {@link ConditionContext} alongside its
 * resolved {@link CriteriaNode} so that {@code ParameterSubstitutor} can later rewrite
 * placeholder values inside the context and re-resolve a fresh CriteriaNode. Non-parameterised
 * queries behave exactly as before — the wrapper just delegates {@link #test(Row)} to
 * {@code resolved}.
 *
 * @param cc       the condition context from the listener (may carry unresolved
 *                 {@code ParameterRef}s before substitution)
 * @param resolved the {@link CriteriaNode} produced by the handler registry for {@code cc}
 */
public record SingleConditionNode(ConditionContext cc, CriteriaNode resolved) implements CriteriaNode {
    @Override
    public boolean test(Row row) {
        return resolved.test(row);
    }
}
