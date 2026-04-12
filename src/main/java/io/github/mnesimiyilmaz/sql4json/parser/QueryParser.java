package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonLexer;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;
import io.github.mnesimiyilmaz.sql4json.registry.ConditionHandlerRegistry;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.Objects;

/**
 * Parses a SQL string into a QueryDefinition using ANTLR.
 * Thread-safe: each call creates its own lexer/parser/listener instances.
 * <p>
 * Usage:
 * QueryDefinition qd = QueryParser.parse("SELECT * FROM $r WHERE age > 25");
 */
public final class QueryParser {

    private QueryParser() {
    }

    /**
     * Parse a SQL string and return an immutable QueryDefinition.
     *
     * @param sql the SQL query string to parse
     * @return the parsed query definition
     * @throws SQL4JsonParseException if the SQL is syntactically invalid
     */
    public static QueryDefinition parse(String sql) {
        return parse(sql, Sql4jsonSettings.defaults());
    }

    /**
     * Settings-aware variant of {@link #parse(String)}: parses {@code sql} into a
     * {@link QueryDefinition} with settings-driven limits applied.
     *
     * @param sql      the SQL query string to parse
     * @param settings the settings controlling parse limits
     * @return the parsed query definition
     * @throws SQL4JsonParseException if the SQL is blank, exceeds the configured length limit,
     *                                or is syntactically invalid
     * @throws NullPointerException   if {@code settings} is null
     */
    public static QueryDefinition parse(String sql, Sql4jsonSettings settings) {
        Objects.requireNonNull(settings, "settings");
        if (sql == null || sql.isBlank()) {
            throw new SQL4JsonParseException("SQL query must not be blank", 0, 0);
        }
        final int maxSqlLength = settings.limits().maxSqlLength();
        if (sql.length() > maxSqlLength) {
            throw new SQL4JsonParseException(
                    "SQL query length exceeds configured maximum ("
                            + maxSqlLength + ")", 0, 0);
        }

        var lexer = new SQL4JsonLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new AntlrSyntaxErrorListener());

        var tokens = new CommonTokenStream(lexer);
        var parser = new SQL4JsonParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new AntlrSyntaxErrorListener());

        SQL4JsonParser.Sql4jsonContext tree;
        try {
            tree = parser.sql4json();
            if (parser.getNumberOfSyntaxErrors() > 0) {
                throw new SQL4JsonParseException("SQL contains syntax errors", 0, 0);
            }
        } catch (SQL4JsonParseException e) {
            throw e;
        } catch (Exception e) {
            throw new SQL4JsonParseException("Parse error: " + e.getMessage(), 0, 0);
        }

        var conditionRegistry = ConditionHandlerRegistry.forSettings(settings);
        var functionRegistry = FunctionRegistry.getDefault();

        var listener = new SQL4JsonParserListener(tokens, conditionRegistry, functionRegistry, settings);
        ParseTreeWalker.DEFAULT.walk(listener, tree);

        return listener.buildQueryDefinition();
    }
}
