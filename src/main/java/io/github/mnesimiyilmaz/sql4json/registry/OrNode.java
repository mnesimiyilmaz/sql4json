package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Row;

/**
 * A criteria node representing a logical OR of two child criteria nodes.
 *
 * @param left  the left-hand criteria node
 * @param right the right-hand criteria node
 */
public record OrNode(CriteriaNode left, CriteriaNode right) implements CriteriaNode {

    @Override
    public boolean test(Row row) {
        return left.test(row) || right.test(row);
    }
}
