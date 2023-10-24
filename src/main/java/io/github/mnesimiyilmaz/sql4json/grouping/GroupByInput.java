package io.github.mnesimiyilmaz.sql4json.grouping;

import io.github.mnesimiyilmaz.sql4json.condition.CriteriaNode;
import io.github.mnesimiyilmaz.sql4json.definitions.JsonColumnWithNonAggFunctionDefinion;
import io.github.mnesimiyilmaz.sql4json.definitions.SelectColumnDefinition;
import io.github.mnesimiyilmaz.sql4json.utils.FieldKey;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * @author mnesimiyilmaz
 */
@Getter
@RequiredArgsConstructor
public class GroupByInput {

    private final List<Map<FieldKey, Object>>                flattenedInputJsonNode;
    private final List<SelectColumnDefinition>               selectedColumns;
    private final List<JsonColumnWithNonAggFunctionDefinion> groupByColumns;
    private final CriteriaNode                               havingClause;

}
