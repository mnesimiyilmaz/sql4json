package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Row;

/**
 * Logical AND of two criteria nodes. Evaluates to {@code true} only when both children match.
 *
 * @param left  left-hand condition
 * @param right right-hand condition
 */
public record AndNode(CriteriaNode left, CriteriaNode right) implements CriteriaNode {

    @Override
    public boolean test(Row row) {
        return left.test(row) && right.test(row);
    }
}
