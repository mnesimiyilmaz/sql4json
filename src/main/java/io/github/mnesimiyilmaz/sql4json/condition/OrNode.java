package io.github.mnesimiyilmaz.sql4json.condition;

import io.github.mnesimiyilmaz.sql4json.utils.FieldKey;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Map;

/**
 * @author mnesimiyilmaz
 */
@Getter
@RequiredArgsConstructor
public class OrNode implements CriteriaNode {

    private final CriteriaNode left;
    private final CriteriaNode right;

    @Override
    public boolean test(Map<FieldKey, Object> row) {
        return left.test(row) || right.test(row);
    }

}
