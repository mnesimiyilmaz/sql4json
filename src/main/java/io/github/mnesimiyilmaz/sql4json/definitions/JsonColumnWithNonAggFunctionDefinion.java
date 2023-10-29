package io.github.mnesimiyilmaz.sql4json.definitions;

import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;
import io.github.mnesimiyilmaz.sql4json.utils.ParameterizedFunctionUtils;
import io.github.mnesimiyilmaz.sql4json.utils.ValueUtils;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author mnesimiyilmaz
 */
@Getter
public class JsonColumnWithNonAggFunctionDefinion {

    private final SQL4JsonParser.JsonColumnWithNonAggFunctionContext columnContext;
    private final String                                             columnName;
    private final Function<Object, Object>                           valueDecorator;

    public JsonColumnWithNonAggFunctionDefinion(SQL4JsonParser.JsonColumnWithNonAggFunctionContext columnContext) {
        this.columnContext = columnContext;
        this.columnName = columnContext.jsonColumn().getText();
        if (columnContext.NON_AGG_FUNCTION() != null) {
            List<Object> args = new ArrayList<>();
            if (columnContext.params() != null && columnContext.params().value() != null) {
                List<SQL4JsonParser.ValueContext> valueContexts = columnContext.params().value();
                for (SQL4JsonParser.ValueContext valueContext : valueContexts) {
                    args.add(ValueUtils.getValueFromContext(valueContext));
                }
            }
            this.valueDecorator = in -> ParameterizedFunctionUtils.getFunction(
                    columnContext.NON_AGG_FUNCTION().getText().toLowerCase()).apply(in, args);
        } else {
            this.valueDecorator = null;
        }
    }

    public Optional<Function<Object, Object>> getValueDecorator() {
        return Optional.ofNullable(valueDecorator);
    }

}
