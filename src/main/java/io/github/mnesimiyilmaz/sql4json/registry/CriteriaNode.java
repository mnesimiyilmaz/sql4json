package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Row;

/**
 * CriteriaNode — evaluates a condition against a lazy Row.
 * <p>
 * This is the new (Phase 3+) version. It is distinct from the existing
 * {@code condition.CriteriaNode} which takes {@code Map<FieldKey, Object>}.
 * Phase 5 replaces the old sealed hierarchy with this interface.
 * <p>
 * Not yet sealed — sealed + permits clause added in Phase 5.
 */
@FunctionalInterface
public interface CriteriaNode {
    /**
     * Tests whether the given row satisfies this condition.
     *
     * @param row the row to test
     * @return {@code true} if the condition is met
     */
    boolean test(Row row);
}
