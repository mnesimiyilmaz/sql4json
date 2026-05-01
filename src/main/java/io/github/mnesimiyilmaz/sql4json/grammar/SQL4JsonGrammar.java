package io.github.mnesimiyilmaz.sql4json.grammar;

import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonLexer;
import org.antlr.v4.runtime.BufferedTokenStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Lexer;

import java.util.*;

/**
 * Grammar introspection API for IDE and tooling consumers.
 *
 * <p>Exposes three static views of the SQL4Json grammar:
 * <ul>
 *   <li>{@link #keywords()} — reserved keywords</li>
 *   <li>{@link #functions()} — built-in function catalog with categories, arities, and signatures</li>
 *   <li>{@link #tokenize(String)} — flat tokenisation with no ANTLR leak</li>
 * </ul>
 *
 * <p>This class has no state and is thread-safe. All methods return unmodifiable views.
 *
 * @since 1.2.0
 */
public final class SQL4JsonGrammar {

    private SQL4JsonGrammar() {
        // Utility class — no instances.
    }

    private static final List<String> KEYWORDS = List.of(
            "AND", "ARRAY", "AS", "ASC",
            "AVG",
            "BETWEEN", "BOOLEAN", "BY",
            "CASE", "CAST", "CONTAINS", "COUNT",
            "DATE", "DATETIME", "DECIMAL", "DENSE_RANK", "DESC", "DISTINCT",
            "ELSE", "END",
            "FALSE", "FROM",
            "GROUP",
            "HAVING",
            "IN", "INNER", "INTEGER", "IS",
            "JOIN",
            "LAG", "LEAD", "LEFT", "LIKE", "LIMIT",
            "MAX", "MIN",
            "NOT", "NTILE", "NULL", "NUMBER",
            "OFFSET", "ON", "OR", "ORDER", "OVER",
            "PARTITION",
            "RANK", "RIGHT", "ROW_NUMBER",
            "SELECT", "STRING", "SUM",
            "THEN", "TRUE",
            "WHEN", "WHERE"
    );

    /**
     * Returns the reserved keywords of the SQL4Json grammar, upper-case, sorted,
     * and deduplicated.
     *
     * <p>The list is derived from the ANTLR grammar's literal lexer rules and
     * validated by a drift test against {@code SQL4JsonLexer.VOCABULARY}.
     *
     * @return an unmodifiable list of reserved keywords
     * @since 1.2.0
     */
    public static List<String> keywords() {
        return KEYWORDS;
    }

    private static final List<FunctionInfo> FUNCTIONS = buildFunctions();

    /**
     * Returns the catalog of built-in functions with category, arity, signature,
     * and one-line description.
     *
     * <p>The catalog is a hand-maintained static table, validated against
     * {@code FunctionRegistry.getDefault()} by a drift test. Window functions are
     * grammar-literal (they are not registered in {@code FunctionRegistry}); they
     * appear here with {@link Category#WINDOW}.
     *
     * @return an unmodifiable list of function metadata, ordered by category then name
     * @since 1.2.0
     */
    public static List<FunctionInfo> functions() {
        return FUNCTIONS;
    }

    private static final Map<Integer, TokenKind> TOKEN_KIND_BY_TYPE = buildTokenKindMap();

    /**
     * Tokenises {@code sql} into a flat list of {@link Token}s with absolute offsets.
     *
     * <p>Does not throw on malformed input: unrecognised characters surface as
     * {@link TokenKind#BAD_TOKEN} entries and tokenisation continues. Whitespace is
     * surfaced as {@link TokenKind#WHITESPACE}. {@code EOF} is not emitted.
     *
     * @param sql the SQL text to tokenise (may be empty or malformed; must not be {@code null})
     * @return unmodifiable list of tokens in source order
     * @throws NullPointerException if {@code sql} is {@code null}
     * @since 1.2.0
     */
    public static List<Token> tokenize(String sql) {
        Objects.requireNonNull(sql, "sql");
        if (sql.isEmpty()) return List.of();

        Lexer lexer = new SQL4JsonLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners(); // silence — we reconstruct bad spans from offset gaps

        BufferedTokenStream stream = new BufferedTokenStream(lexer);
        stream.fill(); // drain all tokens on all channels

        List<Token> out = new ArrayList<>();
        int cursor = 0;
        for (org.antlr.v4.runtime.Token t : stream.getTokens()) {
            if (t.getType() == org.antlr.v4.runtime.Token.EOF) continue;
            int start = t.getStartIndex();
            int end = t.getStopIndex() + 1; // ANTLR stopIndex is inclusive; our convention is exclusive
            if (start > cursor) {
                out.add(new Token(TokenKind.BAD_TOKEN, cursor, start));
            }
            out.add(new Token(kindFor(t.getType()), start, end));
            cursor = end;
        }
        if (cursor < sql.length()) {
            out.add(new Token(TokenKind.BAD_TOKEN, cursor, sql.length()));
        }
        return List.copyOf(out);
    }

    private static TokenKind kindFor(int grammarType) {
        TokenKind k = TOKEN_KIND_BY_TYPE.get(grammarType);
        return k != null ? k : TokenKind.BAD_TOKEN;
    }

    private static Map<Integer, TokenKind> buildTokenKindMap() {
        var m = new HashMap<Integer, TokenKind>();
        // Keywords — every single-literal reserved-word lexer rule
        int[] kw = {
                SQL4JsonLexer.SELECT, SQL4JsonLexer.FROM, SQL4JsonLexer.WHERE, SQL4JsonLexer.AS,
                SQL4JsonLexer.GROUP, SQL4JsonLexer.BY, SQL4JsonLexer.ORDER, SQL4JsonLexer.HAVING,
                SQL4JsonLexer.LIKE, SQL4JsonLexer.IS, SQL4JsonLexer.NOT, SQL4JsonLexer.AND, SQL4JsonLexer.OR,
                SQL4JsonLexer.NULL, SQL4JsonLexer.ORDER_DIRECTION, SQL4JsonLexer.AGG_FUNCTION,
                SQL4JsonLexer.BOOLEAN, SQL4JsonLexer.DISTINCT, SQL4JsonLexer.LIMIT, SQL4JsonLexer.OFFSET,
                SQL4JsonLexer.IN, SQL4JsonLexer.BETWEEN, SQL4JsonLexer.CAST,
                SQL4JsonLexer.STRING_TYPE, SQL4JsonLexer.NUMBER_TYPE, SQL4JsonLexer.INTEGER_TYPE,
                SQL4JsonLexer.DECIMAL_TYPE, SQL4JsonLexer.BOOLEAN_TYPE, SQL4JsonLexer.DATE_TYPE,
                SQL4JsonLexer.DATETIME_TYPE,
                SQL4JsonLexer.INNER, SQL4JsonLexer.JOIN, SQL4JsonLexer.LEFT, SQL4JsonLexer.RIGHT, SQL4JsonLexer.ON,
                SQL4JsonLexer.OVER, SQL4JsonLexer.PARTITION,
                SQL4JsonLexer.ROW_NUMBER, SQL4JsonLexer.RANK, SQL4JsonLexer.DENSE_RANK, SQL4JsonLexer.NTILE,
                SQL4JsonLexer.LAG, SQL4JsonLexer.LEAD,
                SQL4JsonLexer.CASE, SQL4JsonLexer.WHEN, SQL4JsonLexer.THEN, SQL4JsonLexer.ELSE, SQL4JsonLexer.END,
                SQL4JsonLexer.CONTAINS, SQL4JsonLexer.ARRAY
        };
        for (int t : kw) m.put(t, TokenKind.KEYWORD);
        m.put(SQL4JsonLexer.IDENTIFIER, TokenKind.IDENTIFIER);
        m.put(SQL4JsonLexer.STRING, TokenKind.STRING_LITERAL);
        m.put(SQL4JsonLexer.NUMBER, TokenKind.NUMBER_LITERAL);
        m.put(SQL4JsonLexer.COMPARISON_OPERATOR, TokenKind.OPERATOR);
        m.put(SQL4JsonLexer.ASTERISK, TokenKind.OPERATOR);
        m.put(SQL4JsonLexer.ARRAY_CONTAINS_OP, TokenKind.OPERATOR);
        m.put(SQL4JsonLexer.ARRAY_CONTAINED_BY_OP, TokenKind.OPERATOR);
        m.put(SQL4JsonLexer.ARRAY_OVERLAP_OP, TokenKind.OPERATOR);
        m.put(SQL4JsonLexer.SEMI_COLON, TokenKind.PUNCTUATION);
        m.put(SQL4JsonLexer.COMMA, TokenKind.PUNCTUATION);
        m.put(SQL4JsonLexer.DOT, TokenKind.PUNCTUATION);
        m.put(SQL4JsonLexer.LPAREN, TokenKind.PUNCTUATION);
        m.put(SQL4JsonLexer.RPAREN, TokenKind.PUNCTUATION);
        m.put(SQL4JsonLexer.LBRACKET, TokenKind.PUNCTUATION);
        m.put(SQL4JsonLexer.RBRACKET, TokenKind.PUNCTUATION);
        m.put(SQL4JsonLexer.ROOT, TokenKind.ROOT_REF);
        m.put(SQL4JsonLexer.POSITIONAL_PARAM, TokenKind.PARAM_POSITIONAL);
        m.put(SQL4JsonLexer.NAMED_PARAM, TokenKind.PARAM_NAMED);
        m.put(SQL4JsonLexer.ESC, TokenKind.WHITESPACE);
        return Map.copyOf(m);
    }

    private static List<FunctionInfo> buildFunctions() {
        return List.of(
                // ── STRING ──
                new FunctionInfo("concat", Category.STRING, 1, -1,
                        "CONCAT(s, ...)", "Concatenates string values"),
                new FunctionInfo("left", Category.STRING, 2, 2,
                        "LEFT(s, n)", "Leftmost n characters of s"),
                new FunctionInfo("length", Category.STRING, 1, 1,
                        "LENGTH(s)", "Character length of s"),
                new FunctionInfo("lower", Category.STRING, 1, 2,
                        "LOWER(s, locale?)", "Lower-cases s, optionally per locale tag"),
                new FunctionInfo("lpad", Category.STRING, 3, 3,
                        "LPAD(s, length, pad)", "Pads s on the left to reach length using pad"),
                new FunctionInfo("position", Category.STRING, 2, 2,
                        "POSITION(substring, s)", "1-based index of substring in s, 0 if missing"),
                new FunctionInfo("replace", Category.STRING, 3, 3,
                        "REPLACE(s, search, replacement)", "Replaces all occurrences of search in s"),
                new FunctionInfo("reverse", Category.STRING, 1, 1,
                        "REVERSE(s)", "Reverses the characters in s"),
                new FunctionInfo("right", Category.STRING, 2, 2,
                        "RIGHT(s, n)", "Rightmost n characters of s"),
                new FunctionInfo("rpad", Category.STRING, 3, 3,
                        "RPAD(s, length, pad)", "Pads s on the right to reach length using pad"),
                new FunctionInfo("substring", Category.STRING, 2, 3,
                        "SUBSTRING(s, start, length?)", "Extracts a substring (1-based)"),
                new FunctionInfo("trim", Category.STRING, 1, 1,
                        "TRIM(s)", "Removes leading and trailing whitespace from s"),
                new FunctionInfo("upper", Category.STRING, 1, 2,
                        "UPPER(s, locale?)", "Upper-cases s, optionally per locale tag"),

                // ── MATH ──
                new FunctionInfo("abs", Category.MATH, 1, 1,
                        "ABS(n)", "Absolute value of n"),
                new FunctionInfo("ceil", Category.MATH, 1, 1,
                        "CEIL(n)", "Smallest integer >= n"),
                new FunctionInfo("floor", Category.MATH, 1, 1,
                        "FLOOR(n)", "Largest integer <= n"),
                new FunctionInfo("mod", Category.MATH, 2, 2,
                        "MOD(n, divisor)", "Remainder of n divided by divisor"),
                new FunctionInfo("power", Category.MATH, 2, 2,
                        "POWER(base, exp)", "base raised to exp"),
                new FunctionInfo("round", Category.MATH, 1, 2,
                        "ROUND(n, decimals?)", "Rounds n to decimals (default 0)"),
                new FunctionInfo("sign", Category.MATH, 1, 1,
                        "SIGN(n)", "Returns -1, 0, or 1 for the sign of n"),
                new FunctionInfo("sqrt", Category.MATH, 1, 1,
                        "SQRT(n)", "Square root of n"),

                // ── DATE_TIME ──
                new FunctionInfo("date_add", Category.DATE_TIME, 3, 3,
                        "DATE_ADD(d, amount, unit)", "Adds amount of unit (YEAR/MONTH/DAY/HOUR/MINUTE/SECOND)"),
                new FunctionInfo("date_diff", Category.DATE_TIME, 3, 3,
                        "DATE_DIFF(d1, d2, unit)", "d1 - d2 expressed in unit"),
                new FunctionInfo("day", Category.DATE_TIME, 1, 1,
                        "DAY(d)", "Day of month for d"),
                new FunctionInfo("hour", Category.DATE_TIME, 1, 1,
                        "HOUR(d)", "Hour-of-day for d"),
                new FunctionInfo("minute", Category.DATE_TIME, 1, 1,
                        "MINUTE(d)", "Minute-of-hour for d"),
                new FunctionInfo("month", Category.DATE_TIME, 1, 1,
                        "MONTH(d)", "Month of year for d"),
                new FunctionInfo("now", Category.DATE_TIME, 0, 0,
                        "NOW()", "Current date-time at evaluation"),
                new FunctionInfo("second", Category.DATE_TIME, 1, 1,
                        "SECOND(d)", "Second-of-minute for d"),
                new FunctionInfo("to_date", Category.DATE_TIME, 1, 2,
                        "TO_DATE(s, format?)", "Parses s as a date; format is optional ISO default"),
                new FunctionInfo("year", Category.DATE_TIME, 1, 1,
                        "YEAR(d)", "Calendar year for d"),

                // ── CONVERSION ──
                new FunctionInfo("cast", Category.CONVERSION, 2, 2,
                        "CAST(expr AS type)", "Converts expr to STRING/NUMBER/INTEGER/DECIMAL/BOOLEAN/DATE/DATETIME"),
                new FunctionInfo("coalesce", Category.CONVERSION, 1, -1,
                        "COALESCE(expr, ...)", "Returns the first non-null argument"),
                new FunctionInfo("nullif", Category.CONVERSION, 2, 2,
                        "NULLIF(a, b)", "Returns NULL when a equals b, else a"),

                // ── AGGREGATE ──
                new FunctionInfo("avg", Category.AGGREGATE, 1, 1,
                        "AVG(expr)", "Arithmetic mean of numeric values"),
                new FunctionInfo("count", Category.AGGREGATE, 1, 1,
                        "COUNT(expr | *)", "Number of rows (or non-null values of expr)"),
                new FunctionInfo("max", Category.AGGREGATE, 1, 1,
                        "MAX(expr)", "Maximum value"),
                new FunctionInfo("min", Category.AGGREGATE, 1, 1,
                        "MIN(expr)", "Minimum value"),
                new FunctionInfo("sum", Category.AGGREGATE, 1, 1,
                        "SUM(expr)", "Sum of numeric values"),

                // ── WINDOW (grammar-literal; not in FunctionRegistry) ──
                new FunctionInfo("dense_rank", Category.WINDOW, 0, 0,
                        "DENSE_RANK() OVER (...)", "Dense rank within the partition"),
                new FunctionInfo("lag", Category.WINDOW, 1, 2,
                        "LAG(expr, offset?) OVER (...)", "Value from a row at offset rows before"),
                new FunctionInfo("lead", Category.WINDOW, 1, 2,
                        "LEAD(expr, offset?) OVER (...)", "Value from a row at offset rows after"),
                new FunctionInfo("ntile", Category.WINDOW, 1, 1,
                        "NTILE(buckets) OVER (...)", "Bucket assignment for the current row"),
                new FunctionInfo("rank", Category.WINDOW, 0, 0,
                        "RANK() OVER (...)", "Rank within the partition with gaps"),
                new FunctionInfo("row_number", Category.WINDOW, 0, 0,
                        "ROW_NUMBER() OVER (...)", "Sequential row number within the partition")
        );
    }

    /**
     * Visible-for-testing: returns the internal grammar-type → {@link TokenKind} map so
     * that drift tests can assert every lexer rule has a corresponding entry. The map
     * itself is unmodifiable; this is a read-only escape hatch, not a public API.
     */
    static Map<Integer, TokenKind> tokenKindByTypeForTesting() {
        return TOKEN_KIND_BY_TYPE;
    }
}
