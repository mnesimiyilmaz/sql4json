package io.github.mnesimiyilmaz.sql4json.condition;

import io.github.mnesimiyilmaz.sql4json.utils.FieldKey;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * @author mnesimiyilmaz
 */
@Getter
@RequiredArgsConstructor
class ComparisonNode implements CriteriaNode {

    private final FieldKey                    key;
    private final Object                      testValue;
    private final BiPredicate<Object, Object> predicate;

    @Setter
    private Function<Object, Object> valueDecorator;

    @Setter
    private Function<Object, Object> testValueDecorator;

    @Override
    public boolean test(Map<FieldKey, Object> row) {
        Object value = row.get(key);
        Optional<Function<Object, Object>> decorator = getValueDecorator();
        if (decorator.isPresent() && value != null) {
            value = decorator.get().apply(value);
        }
        return predicate.test(value, testValue);
    }

    public Optional<Function<Object, Object>> getValueDecorator() {
        return Optional.ofNullable(valueDecorator);
    }

}
