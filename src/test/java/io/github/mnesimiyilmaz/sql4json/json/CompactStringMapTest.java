// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.*;
import org.junit.jupiter.api.Test;

/**
 * Tests for CompactStringMap covering missed branches: containsKey non-String, equals, hashCode, KeySet/EntrySet
 * iterator exhaustion.
 */
class CompactStringMapTest {

    private CompactStringMap<JsonValue> createMap(String... keysAndValues) {
        var source = new LinkedHashMap<String, JsonValue>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            source.put(keysAndValues[i], new JsonStringValue(keysAndValues[i + 1]));
        }
        return new CompactStringMap<>(source);
    }

    @Test
    void containsKey_string() {
        var map = createMap("a", "1", "b", "2");
        assertTrue(map.containsKey("a"));
        assertTrue(map.containsKey("b"));
        assertFalse(map.containsKey("c"));
    }

    @Test
    void containsKey_nonString_returnsFalse() {
        var map = createMap("a", "1");
        assertFalse(map.containsKey(42));
        assertFalse(map.containsKey(null));
    }

    @Test
    void get_existingKey() {
        var map = createMap("a", "1");
        assertEquals(new JsonStringValue("1"), map.get("a"));
    }

    @Test
    void get_missingKey_returnsNull() {
        var map = createMap("a", "1");
        assertNull(map.get("b"));
    }

    @Test
    void getOrDefault_existingKey() {
        var map = createMap("a", "1");
        assertEquals(new JsonStringValue("1"), map.getOrDefault("a", JsonNullValue.INSTANCE));
    }

    @Test
    void getOrDefault_missingKey() {
        var map = createMap("a", "1");
        assertEquals(JsonNullValue.INSTANCE, map.getOrDefault("b", JsonNullValue.INSTANCE));
    }

    @Test
    void size_and_isEmpty() {
        var empty = createMap();
        assertEquals(0, empty.size());
        assertTrue(empty.isEmpty());

        var nonEmpty = createMap("a", "1");
        assertEquals(1, nonEmpty.size());
        assertFalse(nonEmpty.isEmpty());
    }

    @Test
    void equals_sameContent() {
        var map1 = createMap("a", "1", "b", "2");
        var map2 = createMap("a", "1", "b", "2");
        assertEquals(map1, map2);
    }

    @Test
    void equals_differentSize() {
        var map1 = createMap("a", "1", "b", "2");
        var map2 = createMap("a", "1");
        assertNotEquals(map1, map2);
    }

    @Test
    void equals_differentValues() {
        var map1 = createMap("a", "1", "b", "2");
        var map2 = createMap("a", "1", "b", "3");
        assertNotEquals(map1, map2);
    }

    @Test
    void equals_sameObject() {
        var map = createMap("a", "1");
        assertEquals(map, map);
    }

    @Test
    void equals_nonMap() {
        var map = createMap("a", "1");
        assertNotEquals("not a map", map);
    }

    @Test
    void equals_withLinkedHashMap() {
        var compact = createMap("a", "1", "b", "2");
        var linked = new LinkedHashMap<String, JsonValue>();
        linked.put("a", new JsonStringValue("1"));
        linked.put("b", new JsonStringValue("2"));
        assertEquals(compact, linked);
    }

    @Test
    void hashCode_consistent() {
        var map = createMap("a", "1", "b", "2");
        int h1 = map.hashCode();
        int h2 = map.hashCode();
        assertEquals(h1, h2);
    }

    @Test
    void hashCode_equalMapsEqualHash() {
        var map1 = createMap("a", "1");
        var map2 = createMap("a", "1");
        assertEquals(map1.hashCode(), map2.hashCode());
    }

    @Test
    void keySet_iteration() {
        var map = createMap("a", "1", "b", "2", "c", "3");
        Set<String> keys = map.keySet();
        assertEquals(3, keys.size());
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }

    @Test
    void keySet_iterator_exhausted_throws() {
        var map = createMap("a", "1");
        Iterator<String> it = map.keySet().iterator();
        assertTrue(it.hasNext());
        assertEquals("a", it.next());
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void entrySet_iteration() {
        var map = createMap("a", "1", "b", "2");
        Set<Map.Entry<String, JsonValue>> entries = map.entrySet();
        assertEquals(2, entries.size());
        var keys = new ArrayList<String>();
        for (var e : entries) {
            keys.add(e.getKey());
        }
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
    }

    @Test
    void entrySet_iterator_exhausted_throws() {
        var map = createMap("a", "1");
        Iterator<Map.Entry<String, JsonValue>> it = map.entrySet().iterator();
        assertTrue(it.hasNext());
        assertNotNull(it.next());
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void emptyMap() {
        var map = createMap();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertFalse(map.containsKey("a"));
        assertNull(map.get("a"));
        assertFalse(map.keySet().iterator().hasNext());
        assertFalse(map.entrySet().iterator().hasNext());
    }
}
