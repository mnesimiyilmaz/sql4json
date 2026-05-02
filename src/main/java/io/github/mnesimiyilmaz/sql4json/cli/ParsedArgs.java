// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.cli;

import java.util.Map;

/**
 * Parsed command-line arguments for the SQL4Json CLI.
 *
 * <p>Produced by {@link ArgParser#parse(String[])}; consumed by {@link CliRunner#run}.
 *
 * @param querySource raw {@code -q/--query} value (literal SQL or {@code @path}); may be {@code null} when
 *     {@link #help} or {@link #version} short-circuited
 * @param filePath {@code -f/--file} path; {@code null} = read from stdin
 * @param outputPath {@code -o/--output} path; {@code null} = write to stdout
 * @param dataSources ordered map {@code name → path} from {@code --data}; empty if none. Mutually exclusive with
 *     {@link #filePath}
 * @param namedParams ordered map {@code name → bound Java value} from {@code -p/--param}; empty if none
 * @param pretty {@code true} when {@code --pretty} was supplied
 * @param help {@code true} when {@code -h/--help} was supplied (short-circuits every other flag including missing
 *     {@code -q})
 * @param version {@code true} when {@code -v/--version} was supplied (same short-circuit)
 * @since 1.2.0
 */
record ParsedArgs(
        String querySource,
        String filePath,
        String outputPath,
        Map<String, String> dataSources,
        Map<String, Object> namedParams,
        boolean pretty,
        boolean help,
        boolean version) {}
