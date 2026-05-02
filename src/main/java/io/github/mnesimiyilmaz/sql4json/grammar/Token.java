// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.grammar;

/**
 * A single lexed token emitted by {@link SQL4JsonGrammar#tokenize(String)}.
 *
 * <p>Offsets are absolute into the source text, 0-based, with {@code endOffset} exclusive ({@code [startOffset,
 * endOffset)}) — matching {@link String#substring(int, int)} and IntelliJ's {@code LexerBase} contract.
 *
 * @param kind the classification of this token
 * @param startOffset 0-based inclusive start offset in the source text
 * @param endOffset 0-based exclusive end offset in the source text
 * @since 1.2.0
 */
public record Token(TokenKind kind, int startOffset, int endOffset) {}
