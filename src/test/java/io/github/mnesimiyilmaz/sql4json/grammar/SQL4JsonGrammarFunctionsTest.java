// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.grammar;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SQL4JsonGrammarFunctionsTest {

    @Test
    void catalogIsNonEmptyAndUnmodifiable() {
        List<FunctionInfo> fns = SQL4JsonGrammar.functions();
        assertFalse(fns.isEmpty());
        assertThrows(
                UnsupportedOperationException.class,
                () -> fns.add(new FunctionInfo("x", Category.STRING, 0, 0, "X()", "")));
    }

    @Test
    void everyCategoryExceptValueHasAtLeastOneEntry() {
        Set<Category> present =
                SQL4JsonGrammar.functions().stream().map(FunctionInfo::category).collect(Collectors.toSet());
        assertTrue(present.contains(Category.STRING));
        assertTrue(present.contains(Category.MATH));
        assertTrue(present.contains(Category.DATE_TIME));
        assertTrue(present.contains(Category.CONVERSION));
        assertTrue(present.contains(Category.AGGREGATE));
        assertTrue(present.contains(Category.WINDOW));
        // VALUE is intentionally empty in 1.2.0.
    }

    @Test
    void arityInvariants() {
        for (FunctionInfo fn : SQL4JsonGrammar.functions()) {
            assertTrue(fn.minArity() >= 0, "minArity negative for " + fn.name());
            assertTrue(
                    fn.maxArity() == -1 || fn.maxArity() >= fn.minArity(),
                    "maxArity invariant broken for " + fn.name());
        }
    }

    @Test
    void signatureStartsWithUppercaseName() {
        for (FunctionInfo fn : SQL4JsonGrammar.functions()) {
            assertTrue(
                    fn.signature().startsWith(fn.name().toUpperCase()),
                    "signature mismatch: " + fn.name() + " / " + fn.signature());
        }
    }

    @Test
    void descriptionIsNonEmpty() {
        for (FunctionInfo fn : SQL4JsonGrammar.functions()) {
            assertFalse(fn.description().isBlank(), "empty description for " + fn.name());
        }
    }

    @Test
    void containsKnownSpots() {
        var byName = SQL4JsonGrammar.functions().stream().collect(Collectors.toMap(FunctionInfo::name, f -> f));
        assertEquals(Category.STRING, byName.get("lower").category());
        assertEquals(Category.MATH, byName.get("abs").category());
        assertEquals(Category.DATE_TIME, byName.get("now").category());
        assertEquals(Category.CONVERSION, byName.get("cast").category());
        assertEquals(Category.AGGREGATE, byName.get("count").category());
        assertEquals(Category.WINDOW, byName.get("row_number").category());
    }
}
