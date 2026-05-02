// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;

/**
 * A criteria node representing a logical OR of two child criteria nodes.
 *
 * @param left the left-hand criteria node
 * @param right the right-hand criteria node
 */
public record OrNode(CriteriaNode left, CriteriaNode right) implements CriteriaNode {

    @Override
    public boolean test(RowAccessor row) {
        return left.test(row) || right.test(row);
    }
}
