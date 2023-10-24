package io.github.mnesimiyilmaz.sql4json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.mnesimiyilmaz.sql4json.utils.JsonUtils;
import lombok.Getter;

import java.util.function.Supplier;

/**
 * @author mnesimiyilmaz
 */
@Getter
public class SQL4JsonInput {

    private final String   sql;
    private final JsonNode rootNode;

    public SQL4JsonInput(String sql, JsonNode data) {
        this.sql = sql;
        this.rootNode = data;
    }

    public static SQL4JsonInput fromJsonString(String sql, String data) throws JsonProcessingException {
        return new SQL4JsonInput(sql, JsonUtils.getObjectMapper().readTree(data));
    }

    public static SQL4JsonInput fromObject(String sql, Object data) {
        return new SQL4JsonInput(sql, JsonUtils.getObjectMapper().valueToTree(data));
    }

    public static SQL4JsonInput fromJsonNodeSupplier(String sql, Supplier<JsonNode> supplier) {
        return new SQL4JsonInput(sql, supplier.get());
    }

}
