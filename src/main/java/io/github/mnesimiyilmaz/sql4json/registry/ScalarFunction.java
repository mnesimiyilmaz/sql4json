// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.List;
import java.util.function.BiFunction;

/**
 * A scalar function definition pairing a function name with its evaluation logic.
 *
 * @param name the function name (e.g. {@code "lower"}, {@code "abs"})
 * @param apply the function that evaluates a primary input value and additional arguments
 */
public record ScalarFunction(String name, BiFunction<SqlValue, List<SqlValue>, SqlValue> apply)
        implements SqlFunction {}
