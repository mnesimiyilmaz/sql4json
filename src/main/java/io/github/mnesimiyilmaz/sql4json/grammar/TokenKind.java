// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.grammar;

/**
 * Classification of a lexed token emitted by {@link SQL4JsonGrammar#tokenize(String)}.
 *
 * <p>The mapping from grammar token types to {@code TokenKind} is internal to {@link SQL4JsonGrammar}; see the v1.2.0
 * design spec for per-token decisions.
 *
 * @since 1.2.0
 */
public enum TokenKind {

    /** Reserved keywords (SELECT, FROM, WHERE, AVG, ROW_NUMBER, ...). */
    KEYWORD,

    /** Identifiers — column names, source aliases, scalar/value function names. */
    IDENTIFIER,

    /** Single-quoted string literal; both delimiters are included in the offsets. */
    STRING_LITERAL,

    /** Numeric literal (integer or decimal). */
    NUMBER_LITERAL,

    /**
     * Comparison and arithmetic operators ({@code =}, {@code !=}, {@code <}, {@code >}, {@code <=}, {@code >=},
     * {@code *}).
     */
    OPERATOR,

    /** Structural punctuation ({@code ,}, {@code .}, {@code (}, {@code )}, {@code ;}). */
    PUNCTUATION,

    /** The root reference {@code $R} (case-insensitive). */
    ROOT_REF,

    /** Positional parameter placeholder ({@code ?}). */
    PARAM_POSITIONAL,

    /** Named parameter placeholder ({@code :name}). */
    PARAM_NAMED,

    /** Reserved for line/block comments. No comment rule exists in 1.2.0; never emitted. */
    COMMENT,

    /** Runs of whitespace ({@code [\\t \\r \\n]+}). */
    WHITESPACE,

    /** A character span the lexer could not classify. Recovery token; never causes {@code tokenize} to throw. */
    BAD_TOKEN
}
