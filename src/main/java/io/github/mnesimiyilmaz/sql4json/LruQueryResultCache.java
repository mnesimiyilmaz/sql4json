// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

final class LruQueryResultCache implements QueryResultCache {

    private final Map<String, JsonValue> cache;

    LruQueryResultCache(int maxSize) {
        this.cache = Collections.synchronizedMap(new LinkedHashMap<>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, JsonValue> eldest) {
                return size() > maxSize;
            }
        });
    }

    @Override
    public JsonValue get(String sql) {
        return cache.get(sql);
    }

    @Override
    public void put(String sql, JsonValue result) {
        cache.put(sql, result);
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public int size() {
        return cache.size();
    }
}
