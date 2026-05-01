package io.github.mnesimiyilmaz.sql4json.cli;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CliRunnerTest {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private final PrintStream outStream = new PrintStream(out, true, StandardCharsets.UTF_8);
    private final PrintStream errStream = new PrintStream(err, true, StandardCharsets.UTF_8);
    private final InputStream stdin = new ByteArrayInputStream(new byte[0]);

    private String stdout() { return out.toString(StandardCharsets.UTF_8); }
    private String stderr() { return err.toString(StandardCharsets.UTF_8); }

    @Test
    void run_help_printsUsageAndReturnsZero() {
        int code = CliRunner.run(new String[]{"--help"}, stdin, outStream, errStream);
        assertEquals(0, code);
        String out = stdout();
        assertTrue(out.contains("--query"));
        assertTrue(out.contains("--file"));
        assertTrue(out.contains("--data"));
        assertTrue(out.contains("--param"));
        assertTrue(out.contains("--pretty"));
        assertTrue(stderr().isEmpty());
    }

    @Test
    void run_dashH_printsUsageAndReturnsZero() {
        int code = CliRunner.run(new String[]{"-h"}, stdin, outStream, errStream);
        assertEquals(0, code);
        assertFalse(stdout().isEmpty());
    }

    @Test
    void run_version_printsVersionLineAndReturnsZero() {
        int code = CliRunner.run(new String[]{"--version"}, stdin, outStream, errStream);
        assertEquals(0, code);
        String out = stdout().trim();
        assertTrue(out.startsWith("sql4json"), "Expected version line to start with 'sql4json' but got: " + out);
        assertTrue(stderr().isEmpty());
    }

    @Test
    void run_dashV_printsVersionAndReturnsZero() {
        int code = CliRunner.run(new String[]{"-v"}, stdin, outStream, errStream);
        assertEquals(0, code);
        assertTrue(stdout().trim().startsWith("sql4json"));
    }

    @Test
    void run_helpAndVersion_helpWins() {
        int code = CliRunner.run(new String[]{"--version", "--help"}, stdin, outStream, errStream);
        assertEquals(0, code);
        assertTrue(stdout().contains("--query"));
    }

    @Test
    void run_missingQuery_returnsExit2_andPrintsUsageHint() {
        int code = CliRunner.run(new String[0], stdin, outStream, errStream);
        assertEquals(2, code);
        assertTrue(stderr().contains("-q") || stderr().contains("--query"));
    }

    @Test
    void run_unknownFlag_returnsExit2() {
        int code = CliRunner.run(new String[]{"--bogus"}, stdin, outStream, errStream);
        assertEquals(2, code);
        assertTrue(stderr().contains("--bogus"));
    }

    @Nested
    class SingleSource {

        @TempDir
        Path tmp;

        private Path writeFile(String name, String content) throws IOException {
            Path p = tmp.resolve(name);
            Files.writeString(p, content, StandardCharsets.UTF_8);
            return p;
        }

        @Test
        void run_queryFromLiteralAgainstFile_writesResultToStdout() throws Exception {
            Path data = writeFile("data.json",
                    "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]");
            int code = CliRunner.run(
                    new String[]{"-q", "SELECT name FROM $r WHERE age > 25", "-f", data.toString()},
                    stdin, outStream, errStream);
            assertEquals(0, code);
            String result = stdout().trim();
            assertTrue(result.contains("Alice"));
            assertFalse(result.contains("Bob"));
            assertTrue(stderr().isEmpty());
        }

        @Test
        void run_queryFromAtPath_readsSqlFromFile() throws Exception {
            Path data = writeFile("data.json", "[{\"x\":1},{\"x\":2}]");
            Path sql = writeFile("q.sql", "SELECT * FROM $r WHERE x > 1");
            int code = CliRunner.run(
                    new String[]{"-q", "@" + sql, "-f", data.toString()},
                    stdin, outStream, errStream);
            assertEquals(0, code);
            assertTrue(stdout().contains("\"x\":2"));
            assertFalse(stdout().contains("\"x\":1"));
        }

        @Test
        void run_queryAtPath_missingFile_returnsExit1() {
            int code = CliRunner.run(
                    new String[]{"-q", "@no-such.sql", "-f", "irrelevant"},
                    stdin, outStream, errStream);
            assertEquals(1, code);
            assertFalse(stderr().isEmpty());
        }

        @Test
        void run_inputFromStdin_isUsedWhenNoFile() {
            String json = "[{\"v\":1},{\"v\":2}]";
            InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
            int code = CliRunner.run(
                    new String[]{"-q", "SELECT * FROM $r WHERE v = 2"},
                    in, outStream, errStream);
            assertEquals(0, code);
            assertTrue(stdout().contains("\"v\":2"));
        }

        @Test
        void run_outputToFile_writesResultThereAndNotToStdout() throws Exception {
            Path data = writeFile("d.json", "[{\"a\":1}]");
            Path outPath = tmp.resolve("out.json");
            int code = CliRunner.run(
                    new String[]{"-q", "SELECT * FROM $r", "-f", data.toString(),
                            "-o", outPath.toString()},
                    stdin, outStream, errStream);
            assertEquals(0, code);
            assertTrue(stdout().isEmpty());
            String fileContent = Files.readString(outPath, StandardCharsets.UTF_8);
            assertTrue(fileContent.contains("\"a\":1"));
        }

        @Test
        void run_compactOutput_isDefault() throws Exception {
            Path data = writeFile("d.json", "[{\"a\":1}]");
            int code = CliRunner.run(
                    new String[]{"-q", "SELECT * FROM $r", "-f", data.toString()},
                    stdin, outStream, errStream);
            assertEquals(0, code);
            String result = stdout().trim();
            assertEquals("[{\"a\":1}]", result);
        }

        @Test
        void run_inputFileMissing_returnsExit1() {
            int code = CliRunner.run(
                    new String[]{"-q", "SELECT 1", "-f", "no-such.json"},
                    stdin, outStream, errStream);
            assertEquals(1, code);
        }
    }

    @Nested
    class MultiSource {

        @TempDir
        Path tmp;

        @Test
        void run_joinAcrossTwoSources_returnsJoinedRows() throws Exception {
            Path users = tmp.resolve("u.json");
            Path orders = tmp.resolve("o.json");
            Files.writeString(users,
                    "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]",
                    StandardCharsets.UTF_8);
            Files.writeString(orders,
                    "[{\"user_id\":1,\"amount\":100},{\"user_id\":2,\"amount\":50}]",
                    StandardCharsets.UTF_8);
            int code = CliRunner.run(new String[]{
                    "-q", "SELECT u.name AS name, o.amount AS amount " +
                            "FROM users u JOIN orders o ON u.id = o.user_id ORDER BY name",
                    "--data", "users=" + users,
                    "--data", "orders=" + orders
            }, stdin, outStream, errStream);
            assertEquals(0, code);
            assertTrue(stdout().contains("Alice"));
            assertTrue(stdout().contains("Bob"));
        }

        @Test
        void run_dataFileMissing_returnsExit1() {
            int code = CliRunner.run(new String[]{
                    "-q", "SELECT * FROM users",
                    "--data", "users=no-such.json"
            }, stdin, outStream, errStream);
            assertEquals(1, code);
        }
    }

    @Nested
    class PrettyAndErrors {

        @TempDir
        Path tmp;

        @Test
        void run_pretty_indentsOutput() throws Exception {
            Path data = tmp.resolve("d.json");
            Files.writeString(data, "[{\"a\":1}]", StandardCharsets.UTF_8);
            int code = CliRunner.run(new String[]{
                    "-q", "SELECT * FROM $r",
                    "-f", data.toString(),
                    "--pretty"
            }, stdin, outStream, errStream);
            assertEquals(0, code);
            String result = stdout();
            assertTrue(result.contains("\n  "), "Expected indentation in pretty output: " + result);
        }

        @Test
        void run_invalidSql_returnsExit1WithMessage() throws Exception {
            Path data = tmp.resolve("d.json");
            Files.writeString(data, "[]", StandardCharsets.UTF_8);
            int code = CliRunner.run(new String[]{
                    "-q", "NOT VALID SQL",
                    "-f", data.toString()
            }, stdin, outStream, errStream);
            assertEquals(1, code);
            assertFalse(stderr().isEmpty());
        }

        @Test
        void run_outputPathInMissingDirectory_returnsExit1() throws Exception {
            Path data = tmp.resolve("d.json");
            Files.writeString(data, "[]", StandardCharsets.UTF_8);
            int code = CliRunner.run(new String[]{
                    "-q", "SELECT * FROM $r",
                    "-f", data.toString(),
                    "-o", tmp.resolve("nope/out.json").toString()
            }, stdin, outStream, errStream);
            assertEquals(1, code);
        }

        @Test
        void run_emptyAtPath_returnsExit2() {
            int code = CliRunner.run(new String[]{"-q", "@"}, stdin, outStream, errStream);
            assertEquals(2, code);
        }
    }

    @Nested
    class Parameters {

        @TempDir
        Path tmp;

        @Test
        void run_singleSourceWithNamedParam_filtersResult() throws Exception {
            Path data = tmp.resolve("d.json");
            Files.writeString(data,
                    "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]",
                    StandardCharsets.UTF_8);
            int code = CliRunner.run(new String[]{
                    "-q", "SELECT name FROM $r WHERE age > :min",
                    "-f", data.toString(),
                    "-p", "min=25"
            }, stdin, outStream, errStream);
            assertEquals(0, code);
            assertTrue(stdout().contains("Alice"));
            assertFalse(stdout().contains("Bob"));
        }

        @Test
        void run_multiSourceWithNamedParam_filtersJoinResult() throws Exception {
            Path users = tmp.resolve("u.json");
            Path orders = tmp.resolve("o.json");
            Files.writeString(users,
                    "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]",
                    StandardCharsets.UTF_8);
            Files.writeString(orders,
                    "[{\"user_id\":1,\"amount\":100},{\"user_id\":2,\"amount\":50}]",
                    StandardCharsets.UTF_8);
            int code = CliRunner.run(new String[]{
                    "-q", "SELECT u.name AS name, o.amount AS amount " +
                            "FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > :min",
                    "--data", "users=" + users,
                    "--data", "orders=" + orders,
                    "-p", "min=75"
            }, stdin, outStream, errStream);
            assertEquals(0, code);
            assertTrue(stdout().contains("Alice"));
            assertFalse(stdout().contains("Bob"));
        }

        @Test
        void run_multipleNamedParams_areAllBound() throws Exception {
            Path data = tmp.resolve("d.json");
            Files.writeString(data,
                    "[{\"name\":\"Alice\",\"dept\":\"eng\"}," +
                            "{\"name\":\"Bob\",\"dept\":\"eng\"}," +
                            "{\"name\":\"Carol\",\"dept\":\"hr\"}]",
                    StandardCharsets.UTF_8);
            int code = CliRunner.run(new String[]{
                    "-q", "SELECT name FROM $r WHERE dept = :d AND name LIKE :pattern",
                    "-f", data.toString(),
                    "-p", "d=\"eng\"",
                    "-p", "pattern=\"A%\""
            }, stdin, outStream, errStream);
            assertEquals(0, code);
            String s = stdout();
            assertTrue(s.contains("Alice"));
            assertFalse(s.contains("Bob"));
            assertFalse(s.contains("Carol"));
        }
    }
}
