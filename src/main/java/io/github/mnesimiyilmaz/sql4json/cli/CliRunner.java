// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.cli;

import io.github.mnesimiyilmaz.sql4json.BoundParameters;
import io.github.mnesimiyilmaz.sql4json.SQL4Json;
import io.github.mnesimiyilmaz.sql4json.SQL4JsonEngine;
import io.github.mnesimiyilmaz.sql4json.SQL4JsonEngineBuilder;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.json.JsonSerializer;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The unit of CLI behavior. Takes streams as parameters so behavior can be unit-tested with in-memory
 * {@link java.io.ByteArrayInputStream} / {@link java.io.ByteArrayOutputStream} without spawning a child process.
 *
 * <p>{@link Main#main(String[])} is a 3-line delegator that calls {@link #run} and forwards the integer return code to
 * {@link System#exit(int)}.
 *
 * <p>Exit codes:
 *
 * <ul>
 *   <li>{@code 0} — query succeeded, or {@code --help} / {@code --version} short-circuited.
 *   <li>{@code 1} — runtime failure (SQL4Json error, IO error).
 *   <li>{@code 2} — usage error (bad flags, missing required option).
 * </ul>
 *
 * <p>The environment variable {@code SQL4JSON_DEBUG=1} causes full stack traces to accompany failure messages on
 * stderr; otherwise only the message is printed.
 *
 * @since 1.2.0
 */
public final class CliRunner {

    private static final String USAGE = """
            Usage: java -jar sql4json-cli.jar [OPTIONS]

              -q, --query <sql|@path>     SQL to run; @path reads from a file. Required.
              -f, --file <path>           JSON input file. Omit to read from stdin.
              -o, --output <path>         Write result here. Omit for stdout.
                  --data <name>=<path>    Repeatable. Named source for JOIN queries.
                                          Cannot be combined with -f.
              -p, --param <name>=<json>   Repeatable. Bind :name parameters. Value is
                                          parsed as a JSON literal: 25, "alice", [1,2,3], null.
                  --pretty                Pretty-print the JSON output (default: compact).
              -h, --help                  Print this help and exit 0.
              -v, --version               Print library version and exit 0.
            """;

    private CliRunner() {
        // utility class — no instances
    }

    /**
     * Runs the CLI with the supplied arguments and streams.
     *
     * @param args input arguments
     * @param in stdin (used when {@code -f} is omitted)
     * @param out stdout (default sink for query results, {@code --help}, {@code --version})
     * @param err stderr (sink for failure messages and usage hints)
     * @return process exit code (0, 1, or 2)
     */
    public static int run(String[] args, InputStream in, PrintStream out, PrintStream err) {
        ParsedArgs parsed;
        try {
            parsed = ArgParser.parse(args);
        } catch (UsageException e) {
            err.println(e.getMessage());
            err.println();
            err.print(USAGE);
            return 2;
        }

        if (parsed.help()) {
            out.print(USAGE);
            return 0;
        }
        if (parsed.version()) {
            out.println("sql4json " + libraryVersion());
            return 0;
        }

        if (parsed.querySource() == null) {
            err.println("Missing required option: -q/--query");
            err.println();
            err.print(USAGE);
            return 2;
        }

        try {
            String sql = resolveSql(parsed.querySource());
            JsonValue result = executeQuery(parsed, sql, in);
            String body = parsed.pretty() ? JsonSerializer.prettySerialize(result) : JsonSerializer.serialize(result);
            writeOutput(parsed.outputPath(), body, out);
            return 0;
        } catch (UsageException e) {
            err.println(e.getMessage());
            err.println();
            err.print(USAGE);
            return 2;
        } catch (SQL4JsonException e) {
            reportError(err, e, e.getMessage());
            return 1;
        } catch (IOException e) {
            reportError(err, e, "I/O error: " + e.getMessage());
            return 1;
        }
    }

    private static String resolveSql(String source) throws IOException {
        if (source.startsWith("@")) {
            String pathStr = source.substring(1);
            if (pathStr.isEmpty()) {
                throw new UsageException("@-path SQL source has empty path");
            }
            return Files.readString(Path.of(pathStr), StandardCharsets.UTF_8);
        }
        return source;
    }

    private static JsonValue executeQuery(ParsedArgs parsed, String sql, InputStream in) throws IOException {
        if (!parsed.dataSources().isEmpty()) {
            return executeMultiSource(parsed, sql);
        }
        return executeSingleSource(parsed, sql, in);
    }

    private static JsonValue executeSingleSource(ParsedArgs parsed, String sql, InputStream in) throws IOException {
        String json = readInput(parsed.filePath(), in);
        if (parsed.namedParams().isEmpty()) {
            return SQL4Json.queryAsJsonValue(sql, json);
        }
        JsonValue data = Sql4jsonSettings.defaults().codec().parse(json);
        return SQL4Json.prepare(sql).execute(data, buildParams(parsed.namedParams()));
    }

    private static JsonValue executeMultiSource(ParsedArgs parsed, String sql) throws IOException {
        Map<String, String> sources = new LinkedHashMap<>();
        for (var entry : parsed.dataSources().entrySet()) {
            sources.put(entry.getKey(), Files.readString(Path.of(entry.getValue()), StandardCharsets.UTF_8));
        }
        if (parsed.namedParams().isEmpty()) {
            return SQL4Json.queryAsJsonValue(sql, sources);
        }
        SQL4JsonEngineBuilder builder = SQL4Json.engine();
        sources.forEach(builder::data);
        SQL4JsonEngine engine = builder.build();
        return engine.queryAsJsonValue(sql, buildParams(parsed.namedParams()));
    }

    private static BoundParameters buildParams(Map<String, Object> named) {
        BoundParameters p = BoundParameters.named();
        for (var entry : named.entrySet()) {
            p = p.bind(entry.getKey(), entry.getValue());
        }
        return p;
    }

    private static String readInput(String filePath, InputStream stdin) throws IOException {
        if (filePath != null) {
            return Files.readString(Path.of(filePath), StandardCharsets.UTF_8);
        }
        return new String(stdin.readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void writeOutput(String outputPath, String body, PrintStream stdout) throws IOException {
        if (outputPath != null) {
            Files.writeString(Path.of(outputPath), body, StandardCharsets.UTF_8);
            return;
        }
        stdout.println(body);
    }

    private static void reportError(PrintStream err, Throwable t, String message) {
        err.println(message != null ? message : t.getClass().getSimpleName());
        if ("1".equals(System.getenv("SQL4JSON_DEBUG"))) {
            t.printStackTrace(err);
        }
    }

    private static String libraryVersion() {
        String v = CliRunner.class.getPackage().getImplementationVersion();
        return v != null ? v : "dev";
    }
}
