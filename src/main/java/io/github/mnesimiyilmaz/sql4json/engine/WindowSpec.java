// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.parser.OrderByColumnDef;
import java.util.List;

/**
 * OVER clause specification: PARTITION BY expressions + ORDER BY columns. Used by {@link Expression.WindowFnCall} in
 * the AST.
 *
 * @param partitionBy expressions to partition the window by
 * @param orderBy ordering within each partition
 */
public record WindowSpec(List<Expression> partitionBy, List<OrderByColumnDef> orderBy) {
    /** Creates a defensive copy of both lists. */
    public WindowSpec {
        partitionBy = List.copyOf(partitionBy);
        orderBy = List.copyOf(orderBy);
    }
}
