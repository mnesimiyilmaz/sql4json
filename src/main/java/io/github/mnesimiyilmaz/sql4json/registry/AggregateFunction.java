// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.List;
import java.util.function.Function;

/**
 * An aggregate SQL function (COUNT, SUM, AVG, MIN, MAX) that reduces a list of values to one.
 *
 * @param name function name (case-insensitive)
 * @param apply reduction logic: takes a list of per-row values, returns the aggregate result
 */
public record AggregateFunction(String name, Function<List<SqlValue>, SqlValue> apply) implements SqlFunction {}
