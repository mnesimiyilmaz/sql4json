// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.parser;

import java.util.List;

/**
 * Definition of a JOIN clause including table, alias, type, and ON conditions.
 *
 * @param tableName the name of the table to join
 * @param alias the alias for the joined table
 * @param joinType the type of join (INNER, LEFT, RIGHT)
 * @param onConditions the list of equality conditions in the ON clause
 */
public record JoinDef(String tableName, String alias, JoinType joinType, List<JoinEquality> onConditions) {
    /** Compact constructor that defensively copies the ON conditions list. */
    public JoinDef {
        onConditions = List.copyOf(onConditions);
    }
}
