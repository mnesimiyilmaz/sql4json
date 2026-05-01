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

    private static final JsonLongValue[] SMALL_LONGS;

    /**
     * Maximum number of distinct object shapes the per-parser shape registry will intern
     * before bypassing further candidates. Bounds worst-case heterogeneous-JSON memory.
     */
    private static final int MAX_SHAPES = 256;

    static {
        SMALL_LONGS = new JsonLongValue[256];
        for (int i = 0; i < 256; i++) {
            SMALL_LONGS[i] = new JsonLongValue(i);
        }
    }

    private final String                   input;
    private final int                      endPos;
    private       int                      pos;
    private       int                      depth;
    private final HashMap<String, String>  keyPool       = new HashMap<>();
    private final HashMap<Long, String[]>  shapeRegistry = new HashMap<>();
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

        // Build the keys/values arrays directly — skips the per-object
        // LinkedHashMap + Entry chain that backed the legacy "build then convert"
        // shape. Pre-sized to 8 (good fit for nested inner objects, which are
        // usually small) and doubled on overflow for wider top-level rows.
        String[]    keys   = new String[8];
        JsonValue[] values = new JsonValue[8];
        int         size   = 0;
        while (true) {
            String key = readObjectKey();
            JsonValue value = parseValue();
            int existingIdx = findKey(keys, size, key);
            if (existingIdx >= 0) {
                applyDuplicatePolicy(existingIdx, key, value, values);
            } else {
                if (size == keys.length) {
                    int newCap = keys.length * 2;
                    keys = Arrays.copyOf(keys, newCap);
                    values = Arrays.copyOf(values, newCap);
                }
                keys[size] = key;
                values[size] = value;
                size++;
            }
            if (consumeObjectSeparator()) {
                leaveNested();
                return finalizeObject(keys, values, size);
            }
        }
    }

    private String readObjectKey() {
        skipWhitespace();
        if (pos >= endPos || peek() != '"') {
            throw error("Expected string key in object");
        }
        String key = internKey(parseRawString(settings.maxPropertyNameLength()));
        skipWhitespace();
        expect(':');
        return key;
    }

    private static int findKey(String[] keys, int size, String key) {
        for (int i = 0; i < size; i++) {
            if (keys[i].equals(key)) return i;
        }
        return -1;
    }

    private void applyDuplicatePolicy(int existingIdx, String key, JsonValue value, JsonValue[] values) {
        switch (settings.duplicateKeyPolicy()) {
            case REJECT -> throw error("Duplicate key '" + key + "' in object");
            case LAST_WINS -> values[existingIdx] = value;
            case FIRST_WINS -> {
                // already kept first; ignore the new value
            }
        }
    }

    // Returns true when the object's closing '}' has been consumed; false when a ','
    // was consumed and another member follows. Throws on trailing comma / EOF / missing comma.
    private boolean consumeObjectSeparator() {
        skipWhitespace();
        if (pos >= endPos) {
            throw error("Unexpected end of input in object");
        }
        if (peek() == '}') {
            advance();
            return true;
        }
        expect(',');
        skipWhitespace();
        if (pos < endPos && peek() == '}') {
            throw error("Trailing comma in object");
        }
        return false;
    }

    // Always trim values to exact size for consistency with the interned key array.
    // internShape replaces the prior slack-based opportunistic trim — every accepted
    // shape is exact-fit on return.
    private JsonValue finalizeObject(String[] keys, JsonValue[] values, int size) {
        JsonValue[] trimmed = (values.length != size) ? Arrays.copyOf(values, size) : values;
        String[] internedKeys = internShape(keys, size);
        return new JsonObjectValue(new CompactStringMap<>(internedKeys, trimmed, size));
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
        // Fast path: most JSON strings have no escape sequences. Scan ahead for
        // the closing '"' while checking for '\\' or control chars. If none,
        // return a single substring — skips the StringBuilder + per-char append
        // churn that dominated allocations on parser-heavy workloads.
        int start = pos;
        for (int i = start; i < endPos; i++) {
            char c = input.charAt(i);
            if (c == '"') {
                int len = i - start;
                if (len > maxLength) {
                    throw error("String value exceeds configured maximum length (" + maxLength + ")");
                }
                pos = i + 1;
                return input.substring(start, i);
            }
            if (c == '\\') {
                // Slow path: escape sequence — drop into StringBuilder, prefilling
                // the literal prefix already scanned.
                StringBuilder sb = new StringBuilder(Math.max(16, (i - start) + 16));
                sb.append(input, start, i);
                pos = i;
                return parseRawStringWithEscapes(sb, maxLength);
            }
            if (c < 0x20) {
                throw error("Unescaped control character in string: 0x" + Integer.toHexString(c));
            }
        }
        throw error("Unterminated string");
    }

    private String parseRawStringWithEscapes(StringBuilder sb, int maxLength) {
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
            return new JsonDoubleValue(Double.parseDouble(raw));
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
        int len = raw.length();
        int effectiveLen = raw.startsWith("-") ? len - 1 : len;
        if (effectiveLen < 19) {
            long l = Long.parseLong(raw);
            if (l >= 0 && l < SMALL_LONGS.length) return SMALL_LONGS[(int) l];
            return new JsonLongValue(l);
        }
        if (effectiveLen == 19) {
            try {
                return new JsonLongValue(Long.parseLong(raw));
            } catch (NumberFormatException e) {
                return new JsonDecimalValue(new BigDecimal(raw));
            }
        }
        return new JsonDecimalValue(new BigDecimal(raw));
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

    // JSON null has a single canonical value; the alternative is to throw. Shape kept
    // parallel to parseBoolean / parseString / parseNumber for symmetry in parseValue.
    @SuppressWarnings("SameReturnValue")
    private JsonValue parseNull() {
        if (pos + 4 <= endPos && input.startsWith("null", pos)) {
            pos += 4;
            return JsonNullValue.INSTANCE;
        }
        throw error("Expected 'null'");
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private String internKey(String key) {
        // Hand-rolled to avoid the per-call closure that
        // `computeIfAbsent(key, k -> k)` allocates even on cache hits.
        String existing = keyPool.get(key);
        if (existing != null) return existing;
        keyPool.put(key, key);
        return key;
    }

    /**
     * Returns a shared exact-fit key array for the given prefix when the same shape has
     * been seen before in this parse; otherwise interns the candidate. Replaces the
     * legacy {@code slack > 25%} trim path — every accepted candidate is exact-fit on
     * return so {@link CompactStringMap} sees consistent input. Bounded by
     * {@link #MAX_SHAPES} to cap heterogeneous-JSON worst case.
     *
     * <p>Uses an inline 64-bit hash + a {@code HashMap<Long, String[]>} so the lookup
     * does not allocate a per-call key wrapper. On the rare hash collision (different
     * shapes hashing to the same long), the second shape overwrites the first; both
     * remain functionally correct, only the cache-sharing benefit is lost for the
     * conflicting shapes.</p>
     *
     * @param candidate the key array buffer (may have trailing slack)
     * @param size      the number of valid keys in {@code candidate}
     * @return an exact-fit array (shared on cache hit, fresh on miss)
     */
    private String[] internShape(String[] candidate, int size) {
        if (shapeRegistry.size() >= MAX_SHAPES) {
            // Bypass past the cap — preserve the exact-fit invariant.
            return (size == candidate.length) ? candidate : Arrays.copyOf(candidate, size);
        }
        long hash = computeShapeHash(candidate, size);
        String[] existing = shapeRegistry.get(hash);
        if (existing != null
                && Arrays.equals(existing, 0, existing.length, candidate, 0, size)) {
            return existing;
        }
        // Cache miss or rare hash collision (different shape, same hash).
        String[] interned = (size == candidate.length) ? candidate : Arrays.copyOf(candidate, size);
        shapeRegistry.put(hash, interned);
        return interned;
    }

    private static long computeShapeHash(String[] keys, int size) {
        long h = 1L;
        for (int i = 0; i < size; i++) {
            h = 31L * h + keys[i].hashCode();
        }
        return h;
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
