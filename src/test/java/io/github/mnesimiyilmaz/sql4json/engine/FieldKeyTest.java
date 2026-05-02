// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class FieldKeyTest {

    // Family computation: array indices stripped from path
    @Test
    void family_topLevel_noArrayIndex() {
        FieldKey fk = FieldKey.of("name");
        assertEquals("name", fk.getKey());
        assertEquals("name", fk.getFamily());
    }

    @Test
    void family_arrayIndex_stripped() {
        FieldKey fk = FieldKey.of("items[0]");
        assertEquals("items[0]", fk.getKey());
        assertEquals("items", fk.getFamily());
    }

    @Test
    void family_nestedArrayIndex_stripped() {
        FieldKey fk = FieldKey.of("data.items[2].value");
        assertEquals("data.items[2].value", fk.getKey());
        assertEquals("data.items.value", fk.getFamily());
    }

    @Test
    void family_multipleArrayIndices_allStripped() {
        FieldKey fk = FieldKey.of("a[0].b[1]");
        assertEquals("a.b", fk.getFamily());
    }

    // Interner: same string key → same String instance in pool
    @Test
    void interner_sameKey_sameStringInstance() {
        FieldKey.Interner interner = new FieldKey.Interner();
        FieldKey fk1 = FieldKey.of("name", interner);
        FieldKey fk2 = FieldKey.of("name", interner);
        // Both should equal
        assertEquals(fk1, fk2);
        // Same String instance (interned)
        assertSame(fk1.getKey(), fk2.getKey());
    }

    @Test
    void interner_differentKeys_differentInstances() {
        FieldKey.Interner interner = new FieldKey.Interner();
        FieldKey fk1 = FieldKey.of("name", interner);
        FieldKey fk2 = FieldKey.of("age", interner);
        assertNotEquals(fk1, fk2);
    }

    @Test
    void interner_separateInterners_noSharing() {
        FieldKey.Interner interner1 = new FieldKey.Interner();
        FieldKey.Interner interner2 = new FieldKey.Interner();
        FieldKey fk1 = FieldKey.of("name", interner1);
        FieldKey fk2 = FieldKey.of("name", interner2);
        // Equal by value but NOT same String instance (separate pools)
        assertEquals(fk1, fk2);
        // (String reference equality not guaranteed across separate interners — correct)
    }

    // equals / hashCode: key-based, consistent with FieldKey contract
    @Test
    void equals_sameKey_equal() {
        FieldKey fk1 = FieldKey.of("age");
        FieldKey fk2 = FieldKey.of("age");
        assertEquals(fk1, fk2);
        assertEquals(fk1.hashCode(), fk2.hashCode());
    }

    @Test
    void equals_differentKey_notEqual() {
        FieldKey fk1 = FieldKey.of("age");
        FieldKey fk2 = FieldKey.of("name");
        assertNotEquals(fk1, fk2);
    }

    @Test
    void equals_null_notEqual() {
        FieldKey fk = FieldKey.of("x");
        assertNotEquals(null, fk);
    }

    @Test
    void hashCode_preComputed_matchesKeyHash() {
        FieldKey fk = FieldKey.of("someKey");
        // Pre-computed hashCode must match key.hashCode()
        assertEquals("someKey".hashCode(), fk.hashCode());
    }

    // Convenience factory: of(String) for tests / one-off usage
    @Test
    void of_noInterner_works() {
        FieldKey fk = FieldKey.of("test.path[0]");
        assertNotNull(fk);
        assertEquals("test.path[0]", fk.getKey());
        assertEquals("test.path", fk.getFamily());
    }

    @Test
    void family_noArrayIndex_sameReferenceAsKey() {
        // Paths without array indices should return the same string instance
        FieldKey fk = FieldKey.of("address.city");
        assertSame(fk.getKey(), fk.getFamily());
    }

    @Test
    void family_consecutiveArrayIndices_stripped() {
        FieldKey fk = FieldKey.of("matrix[0][1]");
        assertEquals("matrix", fk.getFamily());
    }

    @Test
    void interner_sameKey_sameFieldKeyInstance() {
        FieldKey.Interner interner = new FieldKey.Interner();
        FieldKey fk1 = FieldKey.of("score", interner);
        FieldKey fk2 = FieldKey.of("score", interner);
        assertSame(fk1, fk2); // same FieldKey object, not just equal
    }
}
