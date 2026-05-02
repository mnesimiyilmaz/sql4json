// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.parser;

/**
 * A single equality condition in a JOIN ON clause (left path = right path).
 *
 * @param leftPath the left-hand side column path
 * @param rightPath the right-hand side column path
 */
public record JoinEquality(String leftPath, String rightPath) {}
