package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.settings.DefaultJsonCodecSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Streams top-level array elements from a JSON string one at a time.
 * Each element is parsed via {@link JsonParser#parseRegion} — only one
 * element lives in memory at a time.
 */
public final class StreamingJsonParser {

    private StreamingJsonParser() {
    }

    /**
     * Stream elements of a JSON array one at a time.
     *
     * @param json the raw JSON string
     * @return lazy stream of JsonValue elements
     */
    public static Stream<JsonValue> streamArray(String json) {
        return streamArray(json, null, DefaultJsonCodecSettings.defaults());
    }

    /**
     * Navigate to a path then stream the array at that location.
     *
     * @param json     the raw JSON string
     * @param rootPath path like "$r.data.items", "$r", or null (root)
     * @return lazy stream of JsonValue elements
     */
    public static Stream<JsonValue> streamArray(String json, String rootPath) {
        return streamArray(json, rootPath, DefaultJsonCodecSettings.defaults());
    }

    /**
     * Stream elements of a JSON array one at a time using the given settings.
     *
     * @param json     the raw JSON string
     * @param settings codec/parser settings (security limits, etc.)
     * @return lazy stream of JsonValue elements
     */
    public static Stream<JsonValue> streamArray(String json, DefaultJsonCodecSettings settings) {
        return streamArray(json, null, settings);
    }

    /**
     * Navigate to a path then stream the array at that location using the given settings.
     *
     * @param json     the raw JSON string
     * @param rootPath path like "$r.data.items", "$r", or null (root)
     * @param settings codec/parser settings (security limits, etc.)
     * @return lazy stream of JsonValue elements
     */
    public static Stream<JsonValue> streamArray(String json, String rootPath, DefaultJsonCodecSettings settings) {
        Objects.requireNonNull(settings, "settings");
        if (json == null || json.isBlank()) {
            throw new SQL4JsonExecutionException("Failed to parse JSON: input is null or blank");
        }
        if (json.length() > settings.maxInputLength()) {
            throw new SQL4JsonExecutionException(
                    "Failed to parse JSON: input length exceeds configured maximum ("
                            + settings.maxInputLength() + ")");
        }

        // If a non-root path is specified, parse the full structure to locate
        // the target array. This builds the full tree for the outer JSON, but the
        // outer structure is typically small (one wrapping object with a few keys).
        // A future optimization could partially parse to find the target array's
        // position without building the full tree.
        if (rootPath != null && !rootPath.equals("$r")) {
            JsonValue root = JsonParser.parse(json, settings);
            JsonValue target = JsonFlattener.navigateToPath(root, rootPath);
            return streamFromJsonValue(target);
        }

        int pos = skipWhitespace(json, 0);
        if (pos >= json.length()) {
            throw new SQL4JsonExecutionException(
                    "Failed to parse JSON at position " + pos + ": Unexpected end of input");
        }

        char first = json.charAt(pos);

        // Non-array root: object → single-element stream; primitive/null → empty
        if (first == '{') {
            return Stream.of(JsonParser.parse(json, settings));
        }
        if (first != '[') {
            return Stream.empty();
        }

        return streamArrayElements(json, pos, settings);
    }

    private static Stream<JsonValue> streamFromJsonValue(JsonValue target) {
        return switch (target) {
            case JsonArrayValue(var elements) -> elements.stream();
            case JsonObjectValue ignored -> Stream.of(target);
            default -> Stream.empty();
        };
    }

    private static Stream<JsonValue> streamArrayElements(String json, int arrayStart, DefaultJsonCodecSettings settings) {
        Iterator<JsonValue> iterator = new ArrayElementIterator(json, arrayStart, settings);
        Spliterator<JsonValue> spliterator = Spliterators.spliteratorUnknownSize(
                iterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false);
    }

    /**
     * Iterator that yields one parsed JsonValue per array element.
     * Uses position-skipping to find element boundaries without parsing.
     */
    private static final class ArrayElementIterator implements Iterator<JsonValue> {

        private final String                   json;
        private final DefaultJsonCodecSettings settings;
        private       int                      pos;
        private       boolean                  done;

        ArrayElementIterator(String json, int arrayStart, DefaultJsonCodecSettings settings) {
            this.json = json;
            this.settings = Objects.requireNonNull(settings, "settings");
            this.pos = arrayStart + 1; // skip '['
            this.done = false;
            advancePastWhitespace();
            if (pos < json.length() && json.charAt(pos) == ']') {
                done = true;
            }
        }

        @Override
        public boolean hasNext() {
            return !done;
        }

        @Override
        public JsonValue next() {
            if (done) throw new NoSuchElementException();

            advancePastWhitespace();
            int start = pos;
            int end = findElementEnd();
            JsonValue element = JsonParser.parseRegion(json, start, end, settings);

            // Advance past the element, skip whitespace, check comma or ']'
            pos = end;
            advancePastWhitespace();
            if (pos >= json.length()) {
                throw new SQL4JsonExecutionException(
                        "Failed to parse JSON at position " + pos
                                + ": Unexpected end of input in array");
            }
            char c = json.charAt(pos);
            if (c == ']') {
                done = true;
            } else if (c == ',') {
                pos++; // consume comma
                advancePastWhitespace();
                if (pos < json.length() && json.charAt(pos) == ']') {
                    throw new SQL4JsonExecutionException(
                            "Failed to parse JSON at position " + pos
                                    + ": Trailing comma in array");
                }
            } else {
                throw new SQL4JsonExecutionException(
                        "Failed to parse JSON at position " + pos
                                + ": Expected ',' or ']' after array element");
            }

            return element;
        }

        /**
         * Find the end position of the current element starting at {@code pos}.
         */
        private int findElementEnd() {
            int p = pos;
            if (p >= json.length()) {
                throw new SQL4JsonExecutionException(
                        "Failed to parse JSON at position " + p
                                + ": Unexpected end of input in array");
            }

            char c = json.charAt(p);

            if (c == '{' || c == '[') {
                return skipComposite(p);
            }
            if (c == '"') {
                return skipString(p);
            }
            return skipBareValue(p);
        }

        /**
         * Skip a composite value (object or array) by tracking nesting depth.
         */
        private int skipComposite(int start) {
            int p = start;
            int depth = 0;
            while (p < json.length()) {
                char c = json.charAt(p);
                if (c == '"') {
                    p = skipStringContents(p);
                    continue;
                }
                if (c == '{' || c == '[') {
                    depth++;
                } else if (c == '}' || c == ']') {
                    depth--;
                    if (depth == 0) {
                        return p + 1;
                    }
                }
                p++;
            }
            throw new SQL4JsonExecutionException(
                    "Failed to parse JSON at position " + p
                            + ": Unterminated object or array");
        }

        /**
         * Skip a string value including its quotes.
         */
        private int skipString(int start) {
            return skipStringContents(start);
        }

        /**
         * Skip string contents: advance past opening quote, handle escapes,
         * return position after closing quote.
         */
        private int skipStringContents(int start) {
            int p = start + 1; // skip opening '"'
            while (p < json.length()) {
                char c = json.charAt(p);
                if (c == '"') {
                    return p + 1;
                }
                if (c == '\\') {
                    p++; // skip backslash
                    if (p >= json.length()) break;
                    if (json.charAt(p) == 'u') {
                        p += 4; // skip unicode escape (the 'u' + 4 hex digits)
                    }
                }
                p++;
            }
            throw new SQL4JsonExecutionException(
                    "Failed to parse JSON at position " + p + ": Unterminated string");
        }

        /**
         * Skip a bare value (number, boolean, null).
         */
        private int skipBareValue(int start) {
            int p = start;
            while (p < json.length()) {
                char c = json.charAt(p);
                if (c == ',' || c == ']' || c == '}' || c == ' '
                        || c == '\t' || c == '\n' || c == '\r') {
                    return p;
                }
                p++;
            }
            throw new SQL4JsonExecutionException(
                    "Failed to parse JSON at position " + p
                            + ": Unexpected end of input in array");
        }

        private void advancePastWhitespace() {
            pos = skipWhitespace(json, pos);
        }
    }

    private static int skipWhitespace(String json, int pos) {
        while (pos < json.length()) {
            char c = json.charAt(pos);
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') break;
            pos++;
        }
        return pos;
    }
}
