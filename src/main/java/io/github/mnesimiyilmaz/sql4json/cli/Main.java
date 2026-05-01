package io.github.mnesimiyilmaz.sql4json.cli;

/**
 * Process entry point for the SQL4Json CLI.
 *
 * <p>This class exists solely to host the {@code Main-Class} target referenced by the
 * shaded {@code cli} jar's manifest. All behavior lives in {@link CliRunner}; this is
 * a 3-line delegator that forwards to {@link CliRunner#run} and exits with the
 * returned status.</p>
 *
 * @since 1.2.0
 */
public final class Main {

    private Main() {
        // utility class — not instantiated
    }

    /**
     * CLI process entry point. Forwards {@code args} and the JVM standard streams to
     * {@link CliRunner#run} and exits with the integer status it returns.
     *
     * @param args command-line arguments
     */
    // System.in/out/err are the documented contract for this CLI entry point —
    // a logging framework would be the wrong abstraction here.
    @SuppressWarnings("java:S106")
    public static void main(String[] args) {
        System.exit(CliRunner.run(args, System.in, System.out, System.err));
    }
}
