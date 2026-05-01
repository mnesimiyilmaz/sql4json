/**
 * Grammar introspection API for IDE and tooling consumers.
 *
 * <p>Provides:
 * <ul>
 *   <li>{@link io.github.mnesimiyilmaz.sql4json.grammar.SQL4JsonGrammar#keywords()}
 *       — reserved keywords for completion and highlighting</li>
 *   <li>{@link io.github.mnesimiyilmaz.sql4json.grammar.SQL4JsonGrammar#functions()}
 *       — catalog of built-in functions with category, arity, signature, and description</li>
 *   <li>{@link io.github.mnesimiyilmaz.sql4json.grammar.SQL4JsonGrammar#tokenize(String)}
 *       — flat tokenisation in plugin-friendly record types with no ANTLR leak</li>
 * </ul>
 *
 * <p>The types in this package are read-only views: {@link io.github.mnesimiyilmaz.sql4json.grammar.Token},
 * {@link io.github.mnesimiyilmaz.sql4json.grammar.TokenKind},
 * {@link io.github.mnesimiyilmaz.sql4json.grammar.FunctionInfo}, and
 * {@link io.github.mnesimiyilmaz.sql4json.grammar.Category}.
 * They do not participate in query execution and carry no behaviour.
 *
 * @since 1.2.0
 */
package io.github.mnesimiyilmaz.sql4json.grammar;
