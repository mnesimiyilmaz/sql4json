// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.cli;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ArgParserTest {

    @Nested
    class HelpAndVersion {

        @Test
        void parse_dashH_setsHelp() {
            ParsedArgs a = ArgParser.parse(new String[] {"-h"});
            assertTrue(a.help());
            assertFalse(a.version());
        }

        @Test
        void parse_longHelp_setsHelp() {
            assertTrue(ArgParser.parse(new String[] {"--help"}).help());
        }

        @Test
        void parse_dashV_setsVersion() {
            ParsedArgs a = ArgParser.parse(new String[] {"-v"});
            assertTrue(a.version());
            assertFalse(a.help());
        }

        @Test
        void parse_longVersion_setsVersion() {
            assertTrue(ArgParser.parse(new String[] {"--version"}).version());
        }

        @Test
        void parse_helpAlone_doesNotRequireQuery() {
            ParsedArgs a = ArgParser.parse(new String[] {"--help"});
            assertNull(a.querySource());
        }
    }

    @Nested
    class QueryFlag {

        @Test
        void parse_dashQ_setsLiteralQuery() {
            ParsedArgs a = ArgParser.parse(new String[] {"-q", "SELECT * FROM $r"});
            assertEquals("SELECT * FROM $r", a.querySource());
        }

        @Test
        void parse_longQuery_setsLiteralQuery() {
            ParsedArgs a = ArgParser.parse(new String[] {"--query", "SELECT 1"});
            assertEquals("SELECT 1", a.querySource());
        }

        @Test
        void parse_queryAtPath_keepsAtPrefix() {
            ParsedArgs a = ArgParser.parse(new String[] {"-q", "@queries/q.sql"});
            assertEquals("@queries/q.sql", a.querySource());
        }

        @Test
        void parse_queryWithoutValue_throwsUsage() {
            UsageException ex = assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q"}));
            assertTrue(ex.getMessage().contains("-q"));
        }

        @Test
        void parse_repeatedQuery_throwsUsage() {
            assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q", "a", "-q", "b"}));
        }
    }

    @Nested
    class FileAndOutput {

        @Test
        void parse_dashF_setsFilePath() {
            ParsedArgs a = ArgParser.parse(new String[] {"-q", "x", "-f", "data.json"});
            assertEquals("data.json", a.filePath());
        }

        @Test
        void parse_longFile_setsFilePath() {
            ParsedArgs a = ArgParser.parse(new String[] {"-q", "x", "--file", "d.json"});
            assertEquals("d.json", a.filePath());
        }

        @Test
        void parse_dashO_setsOutputPath() {
            ParsedArgs a = ArgParser.parse(new String[] {"-q", "x", "-o", "out.json"});
            assertEquals("out.json", a.outputPath());
        }

        @Test
        void parse_longOutput_setsOutputPath() {
            ParsedArgs a = ArgParser.parse(new String[] {"-q", "x", "--output", "out.json"});
            assertEquals("out.json", a.outputPath());
        }

        @Test
        void parse_fileWithoutValue_throwsUsage() {
            assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q", "x", "-f"}));
        }

        @Test
        void parse_outputWithoutValue_throwsUsage() {
            assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q", "x", "-o"}));
        }
    }

    @Nested
    class DataSources {

        @Test
        void parse_singleData_addsOneSource() {
            ParsedArgs a = ArgParser.parse(new String[] {"-q", "x", "--data", "users=u.json"});
            assertEquals(1, a.dataSources().size());
            assertEquals("u.json", a.dataSources().get("users"));
        }

        @Test
        void parse_multipleData_preservesOrder() {
            ParsedArgs a = ArgParser.parse(new String[] {
                "-q", "x",
                "--data", "users=u.json",
                "--data", "orders=o.json"
            });
            assertEquals(2, a.dataSources().size());
            assertEquals("u.json", a.dataSources().get("users"));
            assertEquals("o.json", a.dataSources().get("orders"));
            // LinkedHashMap iteration order
            var iter = a.dataSources().keySet().iterator();
            assertEquals("users", iter.next());
            assertEquals("orders", iter.next());
        }

        @Test
        void parse_dataMissingEquals_throwsUsage() {
            UsageException ex = assertThrows(
                    UsageException.class, () -> ArgParser.parse(new String[] {"-q", "x", "--data", "noequals"}));
            assertTrue(ex.getMessage().contains("name=path"));
        }

        @Test
        void parse_dataEmptyName_throwsUsage() {
            assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q", "x", "--data", "=p.json"}));
        }

        @Test
        void parse_dataEmptyPath_throwsUsage() {
            assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q", "x", "--data", "name="}));
        }

        @Test
        void parse_duplicateDataName_throwsUsage() {
            assertThrows(
                    UsageException.class,
                    () -> ArgParser.parse(new String[] {
                        "-q", "x",
                        "--data", "users=a.json",
                        "--data", "users=b.json"
                    }));
        }

        @Test
        void parse_dataAndFile_throwsUsage() {
            UsageException ex = assertThrows(
                    UsageException.class,
                    () -> ArgParser.parse(new String[] {
                        "-q", "x",
                        "-f", "d.json",
                        "--data", "users=u.json"
                    }));
            assertTrue(ex.getMessage().contains("--data"));
            assertTrue(ex.getMessage().contains("-f"));
        }

        @Test
        void parse_fileAndData_throwsUsage_regardlessOfOrder() {
            assertThrows(
                    UsageException.class,
                    () -> ArgParser.parse(new String[] {
                        "-q", "x",
                        "--data", "users=u.json",
                        "-f", "d.json"
                    }));
        }
    }

    @Nested
    class NamedParameters {

        @Test
        void parse_dashP_addsNamedParam() {
            ParsedArgs a = ArgParser.parse(new String[] {"-q", "x", "-p", "id=42"});
            assertEquals(1, a.namedParams().size());
            assertEquals(42, ((Number) a.namedParams().get("id")).intValue());
        }

        @Test
        void parse_longParam_addsNamedParam() {
            ParsedArgs a = ArgParser.parse(new String[] {"-q", "x", "--param", "name=\"alice\""});
            assertEquals("alice", a.namedParams().get("name"));
        }

        @Test
        void parse_multipleParams_preservesOrder() {
            ParsedArgs a = ArgParser.parse(new String[] {
                "-q", "x",
                "-p", "min=10",
                "-p", "max=99"
            });
            var iter = a.namedParams().keySet().iterator();
            assertEquals("min", iter.next());
            assertEquals("max", iter.next());
        }

        @Test
        void parse_paramArrayLiteral_storesList() {
            ParsedArgs a = ArgParser.parse(new String[] {"-q", "x", "-p", "ids=[1,2,3]"});
            assertTrue(a.namedParams().get("ids") instanceof java.util.List);
        }

        @Test
        void parse_duplicateParamName_throwsUsage() {
            assertThrows(
                    UsageException.class,
                    () -> ArgParser.parse(new String[] {
                        "-q", "x",
                        "-p", "id=1",
                        "-p", "id=2"
                    }));
        }

        @Test
        void parse_paramMalformedJson_throwsUsage() {
            assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q", "x", "-p", "x=not-json"}));
        }

        @Test
        void parse_paramWithoutValue_throwsUsage() {
            assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q", "x", "-p"}));
        }
    }

    @Nested
    class PrettyAndUnknown {

        @Test
        void parse_pretty_setsFlag() {
            assertTrue(ArgParser.parse(new String[] {"-q", "x", "--pretty"}).pretty());
        }

        @Test
        void parse_noPretty_leavesFalse() {
            assertFalse(ArgParser.parse(new String[] {"-q", "x"}).pretty());
        }

        @Test
        void parse_repeatedPretty_throwsUsage() {
            assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q", "x", "--pretty", "--pretty"}));
        }

        @Test
        void parse_unknownFlag_throwsUsage() {
            UsageException ex =
                    assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q", "x", "--bogus"}));
            assertTrue(ex.getMessage().contains("--bogus"));
        }

        @Test
        void parse_trailingPositionalArg_throwsUsage() {
            assertThrows(UsageException.class, () -> ArgParser.parse(new String[] {"-q", "x", "stray"}));
        }

        @Test
        void parse_emptyArgs_returnsBlankParsedArgs() {
            ParsedArgs a = ArgParser.parse(new String[0]);
            assertNull(a.querySource());
            assertNull(a.filePath());
            assertNull(a.outputPath());
            assertTrue(a.dataSources().isEmpty());
            assertTrue(a.namedParams().isEmpty());
            assertFalse(a.pretty());
            assertFalse(a.help());
            assertFalse(a.version());
        }
    }

    @Nested
    class CompoundCases {

        @Test
        void parse_fullSingleSourceCommand_setsAllFields() {
            ParsedArgs a = ArgParser.parse(new String[] {
                "-q", "SELECT * FROM $r",
                "-f", "data.json",
                "-o", "out.json",
                "-p", "min=18",
                "--pretty"
            });
            assertEquals("SELECT * FROM $r", a.querySource());
            assertEquals("data.json", a.filePath());
            assertEquals("out.json", a.outputPath());
            assertEquals(18, ((Number) a.namedParams().get("min")).intValue());
            assertTrue(a.pretty());
            assertFalse(a.help());
        }

        @Test
        void parse_multiSourceJoinCommand_setsAllFields() {
            ParsedArgs a = ArgParser.parse(new String[] {
                "-q", "@join.sql",
                "--data", "users=u.json",
                "--data", "orders=o.json",
                "-p", "minAge=21"
            });
            assertEquals("@join.sql", a.querySource());
            assertNull(a.filePath());
            assertEquals(2, a.dataSources().size());
            assertEquals(1, a.namedParams().size());
        }
    }
}
