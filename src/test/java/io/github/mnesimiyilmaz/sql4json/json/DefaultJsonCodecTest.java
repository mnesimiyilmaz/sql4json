// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

class DefaultJsonCodecTest {

    private final JsonCodec codec = new DefaultJsonCodec();

    @Test
    void parse_and_serialize_roundtrip() {
        String json = "{\"name\":\"Alice\",\"scores\":[90,85,92]}";
        JsonValue parsed = codec.parse(json);
        String serialized = codec.serialize(parsed);
        JsonValue reparsed = codec.parse(serialized);
        assertEquals(parsed, reparsed);
    }

    @Test
    void custom_codec_can_be_used() {
        // A trivial custom codec that wraps DefaultJsonCodec
        JsonCodec custom = new JsonCodec() {
            private final JsonCodec inner = new DefaultJsonCodec();

            @Override
            public JsonValue parse(String json) {
                return inner.parse(json);
            }

            @Override
            public String serialize(JsonValue value) {
                return inner.serialize(value);
            }
        };

        String json = "[{\"id\":1}]";
        JsonValue v = custom.parse(json);
        assertEquals(json, custom.serialize(v));
    }
}
