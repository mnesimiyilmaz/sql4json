package io.github.mnesimiyilmaz.sql4json.grammar;

/**
 * Metadata for a built-in function, for IDE completion popups and documentation.
 *
 * <p>Intended for read-only consumption by tooling. The catalog that populates
 * these records is {@link SQL4JsonGrammar#functions()}. Records do not participate
 * in execution — the library evaluates functions via {@code FunctionRegistry},
 * which is independent of this catalog.
 *
 * @param name        lowercase canonical name (e.g. {@code "lower"})
 * @param category    domain classification for IDE grouping
 * @param minArity    minimum argument count (inclusive of the primary operand), zero for value functions
 * @param maxArity    maximum argument count, or {@code -1} for vararg
 * @param signature   human-readable signature (e.g. {@code "LOWER(s, locale?)"})
 * @param description one-line description suitable for a completion popup
 * @since 1.2.0
 */
public record FunctionInfo(
        String name,
        Category category,
        int minArity,
        int maxArity,
        String signature,
        String description
) {
}
