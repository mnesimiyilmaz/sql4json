package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.settings.DefaultJsonCodecSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.math.BigDecimal;
import java.util.*;

/**
 * Hand-written recursive descent JSON parser (RFC 8259).
 * Produces {@link JsonValue} instances directly — no intermediate tree.
 *
 * <p>Enforces security limits configured via {@link DefaultJsonCodecSettings} to prevent
 * denial-of-service from malicious input: maximum input length, nesting depth, number token
 * length, string value length, property name length, and array element count.</p>
 */
public final class JsonParser {

    private static final JsonNumberValue[] SMALL_INTS;

    static {
        SMALL_INTS = new JsonNumberValue[256];
        for (int i = 0; i < 256; i++) {
            SMALL_INTS[i] = new JsonNumberValue(i);
        }
    }

    private final String                   input;
    private final int                      endPos;
    private       int                      pos;
    private       int                      depth;
    private final HashMap<String, String>  keyPool = new HashMap<>();
    private final DefaultJsonCodecSettings settings;

    private JsonParser(String input, DefaultJsonCodecSettings settings) {
        this(input, 0, input.length(), settings);
    }

    JsonParser(String input, int startPos, int endPos, DefaultJsonCodecSettings settings) {
        this.input = input;
        this.pos = startPos;
        this.endPos = endPos;
        this.settings = settings;
    }

    /**
     * Parses a JSON string into a {@link JsonValue} tree using the given codec settings.
     *
     * @param json     the JSON string to parse
     * @param settings the codec settings controlling parsing limits
     * @return the parsed JSON value tree
     */
    public static JsonValue parse(String json, DefaultJsonCodecSettings settings) {
        Objects.requireNonNull(settings, "settings");
        if (json == null || json.isBlank()) {
            throw new SQL4JsonExecutionException("Failed to parse JSON: input is null or blank");
        }
        if (json.length() > settings.maxInputLength()) {
            throw new SQL4JsonExecutionException(
                    "Failed to parse JSON: input length exceeds configured maximum ("
                            + settings.maxInputLength() + ")");
        }
        JsonParser parser = new JsonParser(json, settings);
        JsonValue result = parser.parseValue();
        parser.skipWhitespace();
        if (parser.pos < parser.endPos) {
            throw parser.error("Unexpected content after JSON value");
        }
        return result;
    }

    /**
     * Convenience overload that parses {@code json} using {@link DefaultJsonCodecSettings#defaults()}.
     *
     * @param json the JSON string to parse
     * @return the parsed JSON value tree
     */
    public static JsonValue parse(String json) {
        return parse(json, DefaultJsonCodecSettings.defaults());
    }

    static JsonValue parseRegion(String input, int start, int end, DefaultJsonCodecSettings settings) {
        Objects.requireNonNull(settings, "settings");
        JsonParser parser = new JsonParser(input, start, end, settings);
        JsonValue result = parser.parseValue();
        parser.skipWhitespace();
        if (parser.pos < parser.endPos) {
            throw parser.error("Unexpected content after JSON value");
        }
        return result;
    }

    /**
     * Convenience overload that parses the region using {@link DefaultJsonCodecSettings#defaults()}.
     */
    static JsonValue parseRegion(String input, int start, int end) {
        return parseRegion(input, start, end, DefaultJsonCodecSettings.defaults());
    }

    private JsonValue parseValue() {
        skipWhitespace();
        if (pos >= endPos) {
            throw error("Unexpected end of input");
        }
        return switch (peek()) {
            case '{' -> parseObject();
            case '[' -> parseArray();
            case '"' -> parseString();
            case 't', 'f' -> parseBoolean();
            case 'n' -> parseNull();
            default -> {
                char c = peek();
                if (c == '-' || (c >= '0' && c <= '9')) {
                    yield parseNumber();
                }
                throw error("Unexpected character: '" + c + "'");
            }
        };
    }

    private JsonValue parseObject() {
        enterNested();
        advance(); // consume '{'
        skipWhitespace();

        if (pos < endPos && peek() == '}') {
            advance();
            leaveNested();
            return new JsonObjectValue(Collections.emptyMap());
        }

        var fields = new LinkedHashMap<String, JsonValue>();
        while (true) {
            skipWhitespace();
            if (pos >= endPos || peek() != '"') {
                throw error("Expected string key in object");
            }
            String key = internKey(parseRawString(settings.maxPropertyNameLength()));
            skipWhitespace();
            expect(':');
            JsonValue value = parseValue();
            putField(fields, key, value);
            skipWhitespace();
            if (pos >= endPos) {
                throw error("Unexpected end of input in object");
            }
            if (peek() == '}') {
                advance();
                leaveNested();
                return new JsonObjectValue(new CompactStringMap<>(fields));
            }
            expect(',');
            // Check for trailing comma
            skipWhitespace();
            if (pos < endPos && peek() == '}') {
                throw error("Trailing comma in object");
            }
        }
    }

    private void putField(LinkedHashMap<String, JsonValue> fields, String key, JsonValue value) {
        JsonValue existing = fields.putIfAbsent(key, value);
        if (existing != null) {
            switch (settings.duplicateKeyPolicy()) {
                case REJECT -> throw error("Duplicate key '" + key + "' in object");
                case LAST_WINS -> fields.put(key, value);
                case FIRST_WINS -> { /* already kept first via putIfAbsent */ }
            }
        }
    }

    private JsonValue parseArray() {
        enterNested();
        advance(); // consume '['
        skipWhitespace();

        if (pos < endPos && peek() == ']') {
            advance();
            leaveNested();
            return new JsonArrayValue(Collections.emptyList());
        }

        var elements = new ArrayList<JsonValue>();
        while (true) {
            elements.add(parseValue());
            if (elements.size() > settings.maxArrayElements()) {
                throw error("Array element count exceeds configured maximum ("
                        + settings.maxArrayElements() + ")");
            }
            skipWhitespace();
            if (pos >= endPos) {
                throw error("Unexpected end of input in array");
            }
            if (peek() == ']') {
                advance();
                leaveNested();
                return new JsonArrayValue(Collections.unmodifiableList(elements));
            }
            expect(',');
            // Check for trailing comma
            skipWhitespace();
            if (pos < endPos && peek() == ']') {
                throw error("Trailing comma in array");
            }
        }
    }

    private JsonValue parseString() {
        return new JsonStringValue(parseRawString());
    }

    private String parseRawString() {
        return parseRawString(settings.maxStringLength());
    }

    private String parseRawString(int maxLength) {
        advance(); // consume opening '"'
        StringBuilder sb = new StringBuilder();
        while (pos < endPos) {
            char c = advance();
            if (c == '"') {
                return sb.toString();
            }
            if (sb.length() >= maxLength) {
                throw error("String value exceeds configured maximum length (" + maxLength + ")");
            }
            if (c == '\\') {
                appendEscapedChar(sb);
            } else if (c < 0x20) {
                throw error("Unescaped control character in string: 0x" + Integer.toHexString(c));
            } else {
                sb.append(c);
            }
        }
        throw error("Unterminated string");
    }

    private void appendEscapedChar(StringBuilder sb) {
        if (pos >= endPos) {
            throw error("Unexpected end of input in string escape");
        }
        char esc = advance();
        switch (esc) {
            case '"' -> sb.append('"');
            case '\\' -> sb.append('\\');
            case '/' -> sb.append('/');
            case 'b' -> sb.append('\b');
            case 'f' -> sb.append('\f');
            case 'n' -> sb.append('\n');
            case 'r' -> sb.append('\r');
            case 't' -> sb.append('\t');
            case 'u' -> appendUnicodeChar(sb);
            default -> throw error("Invalid escape sequence: \\" + esc);
        }
    }

    private void appendUnicodeChar(StringBuilder sb) {
        int cp = parseUnicodeEscape();
        if (Character.isHighSurrogate((char) cp)
                && pos + 1 < endPos
                && input.charAt(pos) == '\\'
                && input.charAt(pos + 1) == 'u') {
            pos += 2; // skip backslash-u
            int low = parseUnicodeEscape();
            sb.appendCodePoint(Character.toCodePoint((char) cp, (char) low));
        } else {
            sb.append((char) cp);
        }
    }

    private int parseUnicodeEscape() {
        if (pos + 4 > endPos) {
            throw error("Incomplete unicode escape");
        }
        String hex = input.substring(pos, pos + 4);
        pos += 4;
        try {
            return Integer.parseInt(hex, 16);
        } catch (NumberFormatException e) {
            throw error("Invalid unicode escape: \\u" + hex);
        }
    }

    private JsonValue parseNumber() {
        int start = pos;
        consumeOptionalMinus();
        consumeIntegerPart();
        boolean hasFraction = consumeFractionPart();
        boolean hasExponent = consumeExponentPart();

        String raw = input.substring(start, pos);
        validateNumberToken(raw, hasExponent);

        if (hasFraction || hasExponent) {
            return new JsonNumberValue(Double.parseDouble(raw));
        }
        return parseIntegerValue(raw);
    }

    private void consumeOptionalMinus() {
        if (pos < endPos && peek() == '-') pos++;
    }

    private void consumeIntegerPart() {
        if (pos >= endPos) throw error("Unexpected end of input in number");
        if (peek() == '0') {
            pos++;
        } else if (peek() >= '1' && peek() <= '9') {
            do pos++;
            while (pos < endPos && peek() >= '0' && peek() <= '9');
        } else {
            throw error("Invalid number");
        }
    }

    private boolean consumeFractionPart() {
        if (pos >= endPos || peek() != '.') return false;
        pos++;
        if (pos >= endPos || peek() < '0' || peek() > '9') {
            throw error("Expected digit after decimal point");
        }
        while (pos < endPos && peek() >= '0' && peek() <= '9') pos++;
        return true;
    }

    private boolean consumeExponentPart() {
        if (pos >= endPos || (peek() != 'e' && peek() != 'E')) return false;
        pos++;
        if (pos < endPos && (peek() == '+' || peek() == '-')) pos++;
        if (pos >= endPos || peek() < '0' || peek() > '9') {
            throw error("Expected digit in exponent");
        }
        while (pos < endPos && peek() >= '0' && peek() <= '9') pos++;
        return true;
    }

    private void validateNumberToken(String raw, boolean hasExponent) {
        if (raw.length() > settings.maxNumberLength()) {
            throw error("Number token exceeds configured maximum length (" + settings.maxNumberLength() + ")");
        }
        if (hasExponent) {
            int eIdx = raw.indexOf('e');
            if (eIdx < 0) eIdx = raw.indexOf('E');
            String expStr = raw.substring(eIdx + 1);
            if (expStr.startsWith("+") || expStr.startsWith("-")) expStr = expStr.substring(1);
            if (expStr.length() > 4) { // exponent > 9999 is pathological for a data library
                throw error("Number exponent magnitude too large");
            }
        }
    }

    private JsonNumberValue parseIntegerValue(String raw) {
        // Integer range fits: use Integer
        // Long range fits: use Long
        // Otherwise: BigDecimal
        int len = raw.length();
        int effectiveLen = raw.startsWith("-") ? len - 1 : len;
        if (effectiveLen < 10) {
            int val = Integer.parseInt(raw);
            if (val >= 0 && val < SMALL_INTS.length) {
                return SMALL_INTS[val];
            }
            return new JsonNumberValue(val);
        }
        if (effectiveLen < 19) {
            long l = Long.parseLong(raw);
            if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                return new JsonNumberValue((int) l);
            }
            return new JsonNumberValue(l);
        }
        if (effectiveLen == 19) {
            // Could be Long or overflow to BigDecimal — try Long
            try {
                return new JsonNumberValue(Long.parseLong(raw));
            } catch (NumberFormatException e) {
                return new JsonNumberValue(new BigDecimal(raw));
            }
        }
        return new JsonNumberValue(new BigDecimal(raw));
    }

    private JsonValue parseBoolean() {
        if (pos + 4 <= endPos && input.startsWith("true", pos)) {
            pos += 4;
            return JsonBooleanValue.TRUE;
        }
        if (pos + 5 <= endPos && input.startsWith("false", pos)) {
            pos += 5;
            return JsonBooleanValue.FALSE;
        }
        throw error("Expected 'true' or 'false'");
    }

    private JsonValue parseNull() {
        if (pos + 4 <= endPos && input.startsWith("null", pos)) {
            pos += 4;
            return JsonNullValue.INSTANCE;
        }
        throw error("Expected 'null'");
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private String internKey(String key) {
        return keyPool.computeIfAbsent(key, k -> k);
    }

    private void skipWhitespace() {
        while (pos < endPos) {
            char c = input.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                pos++;
            } else {
                break;
            }
        }
    }

    private char peek() {
        return input.charAt(pos);
    }

    private char advance() {
        return input.charAt(pos++);
    }

    private void expect(char expected) {
        if (pos >= endPos || peek() != expected) {
            throw error("Expected '" + expected + "'");
        }
        pos++;
    }

    private void enterNested() {
        if (++depth > settings.maxNestingDepth()) {
            throw error("Nesting depth exceeds configured maximum (" + settings.maxNestingDepth() + ")");
        }
    }

    private void leaveNested() {
        depth--;
    }

    private SQL4JsonExecutionException error(String message) {
        return new SQL4JsonExecutionException("Failed to parse JSON at position " + pos + ": " + message);
    }
}
