package io.github.mnesimiyilmaz.sql4json.cli;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Hand-rolled state-machine parser for CLI arguments.
 *
 * <p>Walks {@code args} once and produces a {@link ParsedArgs}. Has no third-party
 * dependencies. Validation is shallow at this layer: structural problems and mutual
 * exclusions are surfaced; semantic checks (file exists, query non-empty, source
 * count vs. SQL shape) belong to {@link CliRunner}.</p>
 *
 * @since 1.2.0
 */
final class ArgParser {

    private ArgParser() {
        // utility class — no instances
    }

    /**
     * Parses the supplied argument array.
     *
     * @param args raw command-line arguments
     * @return populated {@link ParsedArgs}
     * @throws UsageException for missing values, unknown flags, duplicate single-value
     *                        flags, malformed {@code --data} / {@code -p} arguments,
     *                        and the {@code -f}/{@code --data} mutual exclusion
     */
    static ParsedArgs parse(String[] args) {
        State state = new State();
        int i = 0;
        while (i < args.length) {
            i = state.consume(args, i);
        }
        if (state.filePath != null && !state.dataSources.isEmpty()) {
            throw new UsageException("-f/--file and --data are mutually exclusive");
        }
        return state.toParsedArgs();
    }

    /**
     * Mutable parser state. Each {@link #consume(String[], int)} call dispatches one
     * argument and returns the next index — splitting the per-flag work out of
     * {@link ArgParser#parse(String[])} keeps that method's cognitive complexity flat.
     */
    private static final class State {
        String              querySource;
        String              filePath;
        String              outputPath;
        final Map<String, String> dataSources = new LinkedHashMap<>();
        final Map<String, Object> namedParams = new LinkedHashMap<>();
        boolean             pretty;
        boolean             help;
        boolean             version;

        int consume(String[] args, int i) {
            String arg = args[i];
            return switch (arg) {
                case "-h", "--help" -> {
                    help = true;
                    yield i + 1;
                }
                case "-v", "--version" -> {
                    version = true;
                    yield i + 1;
                }
                case "-q", "--query" -> {
                    querySource = requireUniqueValue(args, i, querySource, "-q", "-q/--query");
                    yield i + 2;
                }
                case "-f", "--file" -> {
                    filePath = requireUniqueValue(args, i, filePath, "-f", "-f/--file");
                    yield i + 2;
                }
                case "-o", "--output" -> {
                    outputPath = requireUniqueValue(args, i, outputPath, "-o", "-o/--output");
                    yield i + 2;
                }
                case "--pretty" -> {
                    pretty = setUniqueFlag(pretty, "--pretty");
                    yield i + 1;
                }
                case "--data" -> {
                    addDataSource(dataSources, requireValue(args, i, "--data"));
                    yield i + 2;
                }
                case "-p", "--param" -> {
                    addNamedParam(namedParams, requireValue(args, i, "-p"));
                    yield i + 2;
                }
                default -> throw unexpectedArgument(arg);
            };
        }

        ParsedArgs toParsedArgs() {
            return new ParsedArgs(querySource, filePath, outputPath, dataSources, namedParams,
                pretty, help, version);
        }

        private static String requireUniqueValue(String[] args, int idx, String existing, String flag, String label) {
            if (existing != null) {
                throw new UsageException(label + " may only be specified once");
            }
            return requireValue(args, idx, flag);
        }

        @SuppressWarnings({"SameParameterValue", "SameReturnValue"})
        private static boolean setUniqueFlag(boolean current, String label) {
            if (current) {
                throw new UsageException(label + " may only be specified once");
            }
            return true;
        }

        private static UsageException unexpectedArgument(String arg) {
            return arg.startsWith("-")
                ? new UsageException("Unknown flag: " + arg)
                : new UsageException("Unexpected positional argument: " + arg);
        }

        private static void addDataSource(Map<String, String> sources, String raw) {
            int eq = raw.indexOf('=');
            if (eq < 0) {
                throw new UsageException("--data expects name=path, got '" + raw + "'");
            }
            String name = raw.substring(0, eq);
            String path = raw.substring(eq + 1);
            if (name.isEmpty()) {
                throw new UsageException("--data name must not be empty");
            }
            if (path.isEmpty()) {
                throw new UsageException("--data '" + name + "' has empty path");
            }
            if (sources.containsKey(name)) {
                throw new UsageException("--data name '" + name + "' specified more than once");
            }
            sources.put(name, path);
        }

        private static void addNamedParam(Map<String, Object> params, String raw) {
            Map.Entry<String, Object> entry = ParamValueParser.parse(raw);
            if (params.containsKey(entry.getKey())) {
                throw new UsageException("--param name '" + entry.getKey() + "' specified more than once");
            }
            params.put(entry.getKey(), entry.getValue());
        }

        private static String requireValue(String[] args, int idx, String flag) {
            if (idx + 1 >= args.length) {
                throw new UsageException(flag + " requires a value");
            }
            return args[idx + 1];
        }
    }

}
