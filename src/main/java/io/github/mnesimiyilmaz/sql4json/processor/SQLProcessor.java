package io.github.mnesimiyilmaz.sql4json.processor;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.mnesimiyilmaz.sql4json.utils.FieldKey;
import io.github.mnesimiyilmaz.sql4json.utils.JsonUtils;

import java.util.*;
import java.util.stream.Collectors;

public class SQLProcessor {
    private final Map<String, SQLBuilder> mapOfSQLBuilders;

    public SQLProcessor() {
        this.mapOfSQLBuilders = new LinkedHashMap<>();
    }

    public SQLBuilder getBuilder(String name) {
        return mapOfSQLBuilders.computeIfAbsent(name, n -> new SQLBuilder());
    }


    public JsonNode process(JsonNode dataNode, String rootPath) {
        dataNode = JsonUtils.peelJsonNode(dataNode, rootPath);
        List<Map<FieldKey, Object>> flattenedData = JsonUtils.convertJsonToFlattenedListOfKeyValue(dataNode);

        Iterator<String> names = new LinkedList<>(mapOfSQLBuilders.keySet()).descendingIterator();

        while (names.hasNext()) {
            SQLConstruct sqlConstruct = SQLConstruct.newInstance(mapOfSQLBuilders.get(names.next()));
            flattenedData = sqlConstruct.apply(flattenedData);
        }

        return JsonUtils.convertStructuredMapToJsonNode(
                flattenedData.stream().map(JsonUtils::convertFlatMapToStructuredMap).collect(Collectors.toList()));

    }
}
