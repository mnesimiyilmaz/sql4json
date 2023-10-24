package io.github.mnesimiyilmaz.sql4json.condition;

import io.github.mnesimiyilmaz.sql4json.utils.FieldKey;

import java.util.Map;

/**
 * @author mnesimiyilmaz
 */
public interface CriteriaNode {

    boolean test(Map<FieldKey, Object> row);

}
