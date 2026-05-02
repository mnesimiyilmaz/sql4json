// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.grouping;

import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.List;

/**
 * Immutable GROUP BY grouping key. Record-based for correct {@code equals}/{@code hashCode} semantics when used as a
 * map key in group aggregation.
 *
 * @param values the evaluated GROUP BY expression values for one row
 */
public record GroupKey(List<SqlValue> values) {
    /** Creates a defensive copy of the values list. */
    public GroupKey {
        values = List.copyOf(values);
    }
}
