package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.registry.ConditionHandlerRegistry;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.registry.OperatorRegistry;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RegistrySingletonTest {

    @Test
    void functionRegistry_getDefault_returnsSameInstance() {
        assertSame(FunctionRegistry.getDefault(), FunctionRegistry.getDefault());
    }

    @Test
    void operatorRegistry_getDefault_returnsSameInstance() {
        assertSame(OperatorRegistry.getDefault(), OperatorRegistry.getDefault());
    }

    @Test
    void functionRegistry_default_hasExpectedFunctions() {
        FunctionRegistry reg = FunctionRegistry.getDefault();
        assertTrue(reg.getScalar("lower").isPresent(), "lower");
        assertTrue(reg.getScalar("upper").isPresent(), "upper");
        assertTrue(reg.getScalar("abs").isPresent(), "abs");
        assertTrue(reg.getValue("now").isPresent(), "now");
        assertTrue(reg.getAggregate("count").isPresent(), "count");
        assertTrue(reg.getAggregate("sum").isPresent(), "sum");
    }

    @Test
    void operatorRegistry_default_hasExpectedOperators() {
        OperatorRegistry reg = OperatorRegistry.getDefault();
        // getPredicate throws if the operator is absent, so no exception means present
        assertDoesNotThrow(() -> reg.getPredicate("="));
        assertDoesNotThrow(() -> reg.getPredicate("!="));
        assertDoesNotThrow(() -> reg.getPredicate(">"));
        assertDoesNotThrow(() -> reg.getPredicate("<"));
    }

    @Test
    void functionRegistry_default_isFrozen_registerScalarThrows() {
        FunctionRegistry reg = FunctionRegistry.getDefault();
        var fn = new io.github.mnesimiyilmaz.sql4json.registry.ScalarFunction(
                "dummy", (val, args) -> val);
        assertThrows(UnsupportedOperationException.class, () -> reg.registerScalar(fn));
    }

    @Test
    void functionRegistry_default_isFrozen_registerValueThrows() {
        FunctionRegistry reg = FunctionRegistry.getDefault();
        var fn = new io.github.mnesimiyilmaz.sql4json.registry.ValueFunction(
                "dummy", () -> io.github.mnesimiyilmaz.sql4json.types.SqlNull.INSTANCE);
        assertThrows(UnsupportedOperationException.class, () -> reg.registerValue(fn));
    }

    @Test
    void functionRegistry_default_isFrozen_registerAggregateThrows() {
        FunctionRegistry reg = FunctionRegistry.getDefault();
        var fn = new io.github.mnesimiyilmaz.sql4json.registry.AggregateFunction(
                "dummy", vals -> io.github.mnesimiyilmaz.sql4json.types.SqlNull.INSTANCE);
        assertThrows(UnsupportedOperationException.class, () -> reg.registerAggregate(fn));
    }

    @Test
    void operatorRegistry_default_isFrozen_registerThrows() {
        OperatorRegistry reg = OperatorRegistry.getDefault();
        var op = new io.github.mnesimiyilmaz.sql4json.registry.ComparisonOperatorDef(
                "??",
                io.github.mnesimiyilmaz.sql4json.registry.OperatorType.BINARY,
                (a, b) -> false);
        assertThrows(UnsupportedOperationException.class, () -> reg.register(op));
    }

    @Test
    void conditionHandlerRegistry_default_isFrozen_registerThrows() {
        ConditionHandlerRegistry reg =
                ConditionHandlerRegistry.forSettings(Sql4jsonSettings.defaults());
        var handler = new io.github.mnesimiyilmaz.sql4json.registry.ConditionHandler() {
            @Override
            public boolean canHandle(
                    io.github.mnesimiyilmaz.sql4json.registry.ConditionContext ctx) {
                return false;
            }

            @Override
            public io.github.mnesimiyilmaz.sql4json.registry.CriteriaNode handle(
                    io.github.mnesimiyilmaz.sql4json.registry.ConditionContext ctx,
                    OperatorRegistry operators,
                    FunctionRegistry functions) {
                return null;
            }
        };
        assertThrows(UnsupportedOperationException.class, () -> reg.register(handler));
    }

    @Test
    void functionRegistry_createDefault_returnsNewInstance() {
        FunctionRegistry fresh = FunctionRegistry.createDefault();
        assertNotSame(FunctionRegistry.getDefault(), fresh);
    }
}
