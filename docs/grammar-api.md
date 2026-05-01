# Grammar Introspection API (IDE Tooling)

Build editor tooling — syntax highlighting, completion popups, lightweight
static analysis — against the published SQL4Json grammar API.

The library ships with a small read-only catalog
(`io.github.mnesimiyilmaz.sql4json.grammar`) that lets tooling consumers reason
about SQL4Json text without taking a dependency on ANTLR. **This page is for
plugin / extension authors.** If you only want to query JSON with SQL, the main
[README](../README.md) covers everything you need.

## Table of Contents

- [What's exposed](#whats-exposed)
- [Three views of the grammar](#three-views-of-the-grammar)
    - [1. Reserved keywords](#1-reserved-keywords)
    - [2. Built-in function catalog](#2-built-in-function-catalog)
    - [3. Tokenisation](#3-tokenisation)
- [Token kinds](#token-kinds)
- [Function categories](#function-categories)
- [Stability](#stability)
- [Building a syntax highlighter](#building-a-syntax-highlighter)
- [Public API vs. internals](#public-api-vs-internals)

## What's exposed

The package surface is intentionally tiny. No ANTLR types appear in any
signature — by design, so plugin classpaths don't have to inherit the runtime
dependency.

| Type              | Kind   | Purpose                                                              |
|-------------------|--------|----------------------------------------------------------------------|
| `SQL4JsonGrammar` | class  | Static entry point — `keywords()`, `functions()`, `tokenize(...)`    |
| `Token`           | record | One lexed token: `kind`, `startOffset`, `endOffset`                  |
| `TokenKind`       | enum   | Token classification (12 values; see below)                          |
| `FunctionInfo`    | record | Function metadata: `name`, `category`, arity, signature, description |
| `Category`        | enum   | Function classification (7 values; see below)                        |

## Three views of the grammar

### 1. Reserved keywords

```java
List<String> keywords = SQL4JsonGrammar.keywords();
// [AND, AS, ASC, AVG, BETWEEN, BOOLEAN, BY, CASE, CAST, COUNT, ...]
```

Sorted, deduplicated, derived from the lexer vocabulary. Use this to populate a
token-color file or an autocomplete list of reserved words.

### 2. Built-in function catalog

```java
for (FunctionInfo f : SQL4JsonGrammar.functions()) {
    System.out.printf("%-12s [%-10s] %s%n  %s%n",
            f.name(), f.category(), f.signature(), f.description());
}
// concat       [STRING    ] CONCAT(s, ...)
//   Concatenates string values
// round        [MATH      ] ROUND(n, decimals?)
//   Rounds n to decimals (default 0)
// row_number   [WINDOW    ] ROW_NUMBER() OVER (...)
//   Sequential row number within the partition
```

Each `FunctionInfo` carries enough to render a completion popup: name,
category, signature, and a one-line description. `minArity` / `maxArity` allow
tooling to validate argument counts at edit time (`maxArity == -1` indicates
vararg). Window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `LAG`,
`LEAD`) appear with `Category.WINDOW`.

### 3. Tokenisation

```java
List<Token> tokens = SQL4JsonGrammar.tokenize("SELECT age FROM $r WHERE age > 18");
// Token[kind=KEYWORD,        startOffset=0,  endOffset=6]   "SELECT"
// Token[kind=WHITESPACE,     startOffset=6,  endOffset=7]
// Token[kind=IDENTIFIER,     startOffset=7,  endOffset=10]  "age"
// Token[kind=KEYWORD,        startOffset=11, endOffset=15]  "FROM"
// Token[kind=ROOT_REF,       startOffset=16, endOffset=18]  "$r"
// Token[kind=KEYWORD,        startOffset=19, endOffset=24]  "WHERE"
// Token[kind=IDENTIFIER,     startOffset=25, endOffset=28]  "age"
// Token[kind=OPERATOR,       startOffset=29, endOffset=30]  ">"
// Token[kind=NUMBER_LITERAL, startOffset=31, endOffset=33]  "18"
```

Token offsets are absolute, 0-based, with `endOffset` exclusive — matching
`String.substring(int, int)` and IntelliJ's `LexerBase` contract. The slice
for a token is `sql.substring(t.startOffset(), t.endOffset())`.

**Recovery semantics:** unrecognised characters surface as
`TokenKind.BAD_TOKEN`; `tokenize` never throws on malformed input. EOF is not
emitted. Whitespace runs surface as `WHITESPACE` tokens (parser-side they're
on the `HIDDEN` channel, but tokenisation surfaces them so highlighters can
reason about layout).

## Token kinds

| Token Kind         | Description                                                                |
|--------------------|----------------------------------------------------------------------------|
| `KEYWORD`          | Reserved keywords (`SELECT`, `WHERE`, `AVG`, `ROW_NUMBER`, ...)            |
| `IDENTIFIER`       | Column names, source aliases, scalar / value function names                |
| `STRING_LITERAL`   | Single-quoted string literal; both delimiters included in the offsets      |
| `NUMBER_LITERAL`   | Numeric literal (integer or decimal)                                       |
| `OPERATOR`         | Comparison and arithmetic operators (`=`, `!=`, `<`, `>`, `<=`, `>=`, `*`) |
| `PUNCTUATION`      | Structural punctuation (`,`, `.`, `(`, `)`, `;`)                           |
| `ROOT_REF`         | The root reference `$R` (case-insensitive)                                 |
| `PARAM_POSITIONAL` | Positional parameter placeholder (`?`)                                     |
| `PARAM_NAMED`      | Named parameter placeholder (`:name`)                                      |
| `COMMENT`          | Reserved for future line / block comments — not yet emitted                |
| `WHITESPACE`       | Runs of whitespace (`[\t \r\n]+`)                                          |
| `BAD_TOKEN`        | Recovery span for characters the lexer could not classify                  |

## Function categories

`STRING`, `MATH`, `DATE_TIME`, `CONVERSION`, `AGGREGATE`, `WINDOW`, `VALUE`.

`VALUE` is reserved for future zero-argument value functions that don't fit a
domain category; it isn't used in 1.2.0 (`NOW()` is categorised as
`DATE_TIME`).

## Stability

The grammar API follows the library's
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) contract —
additions in minor releases, breaking changes only in major bumps.

Drift tests in `SQL4JsonGrammarDriftTest` guard against silent drift
between the catalog and the underlying grammar / `FunctionRegistry`:

- `keywordsMatchLexerVocabularyLiterals` — adding or removing a keyword in
  the grammar without updating `keywords()` fails CI
- `keywordsCatalog_includes_array_and_contains` — array-predicate keywords
  (`ARRAY`, `CONTAINS`) must stay in the public catalog
- `scalarRegistryEntriesMatchCatalogScalarCategories` /
  `aggregateRegistryEntriesMatchCatalogAggregateCategory` — registering a
  new built-in function without a `FunctionInfo` (or vice versa) fails CI
- `windowFunctionCatalogMatchesGrammarList` — keeps the WINDOW-category
  entries in lock-step with the grammar's window-function rule
- `tokenKindMapCoversEveryLexerType` — adding a new lexer rule without
  mapping it to a `TokenKind` fails CI
- `tokenize_covers_array_operators_with_no_BAD_TOKEN` — `@>`, `<@`, `&&`,
  `[`, `]` must classify cleanly via `tokenize(...)`

In practical terms: if a future minor release adds a keyword or function,
`keywords()` / `functions()` / `tokenize()` will see it on the day of release.
You won't need to ship a new tooling release just to keep up with grammar
growth.

## Building a syntax highlighter

The shape of `tokenize` is deliberately close to IntelliJ's `LexerBase`
contract:

- 0-based, exclusive-end offsets — same convention as `String.substring`
- non-overlapping, contiguous spans (whitespace and bad-token spans included)
- recovery instead of throw — robust on partial / malformed input mid-edit

A minimal IntelliJ `Lexer` implementation can hold a `List<Token>` and expose
`getTokenStart()` / `getTokenEnd()` / `getTokenType()` directly from the
records. TextMate / TM4E grammars can be generated by walking `keywords()` and
`functions()` once at build time.

For per-token type colors, map `TokenKind` to your IDE's standard text
attribute keys: `KEYWORD` → keyword, `STRING_LITERAL` → string,
`NUMBER_LITERAL` → number, `IDENTIFIER` → identifier, `OPERATOR` /
`PUNCTUATION` → operator/braces, `BAD_TOKEN` → bad-character. `PARAM_NAMED` /
`PARAM_POSITIONAL` are typically rendered the same as parameters or template
variables.

## Public API vs. internals

Anything in `io.github.mnesimiyilmaz.sql4json.grammar` is **public API** and
follows the library's semver contract.

Everything else — the ANTLR-generated `SQL4JsonLexer` / `SQL4JsonParser`
types under `io.github.mnesimiyilmaz.sql4json.generated`, the `parser`
package, the `registry` package, the `engine` package — is **internal** and
subject to change at any release. If your tooling needs something the
grammar API doesn't expose yet,
[open an issue](https://github.com/mnesimiyilmaz/sql4json/issues) rather
than reaching into internals.
