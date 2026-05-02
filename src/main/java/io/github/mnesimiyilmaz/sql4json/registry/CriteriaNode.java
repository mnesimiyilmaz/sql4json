// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;

/**
 * CriteriaNode — evaluates a condition against a {@link RowAccessor}.
 *
 * <p>Both lazy {@link io.github.mnesimiyilmaz.sql4json.engine.Row} and materialised
 * {@link io.github.mnesimiyilmaz.sql4json.engine.FlatRow} implement the interface, so handlers and stages do not need
 * to distinguish between them.
 *
 * @since 1.2.0
 */
@FunctionalInterface
public interface CriteriaNode {
    /**
     * Tests whether the given row satisfies this condition.
     *
     * @param row the row to test
     * @return {@code true} if the condition is met
     */
    boolean test(RowAccessor row);
}
