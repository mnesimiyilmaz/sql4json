package io.github.mnesimiyilmaz.sql4json.definitions;

import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;
import io.github.mnesimiyilmaz.sql4json.utils.AggregateFunction;
import lombok.Getter;

/**
 * @author mnesimiyilmaz
 */
@Getter
public class JsonColumnWithAggFunctionDefinion {

    private final SQL4JsonParser.JsonColumnWithAggFunctionContext ctx;
    private final JsonColumnWithNonAggFunctionDefinion            columnDefinition;
    private final AggregateFunction                           aggregateFunction;
    private final boolean                                     isAsterisk;

    public JsonColumnWithAggFunctionDefinion(SQL4JsonParser.JsonColumnWithAggFunctionContext ctx) {
        this.ctx = ctx;
        if (ctx.ASTERISK() != null) {
            this.isAsterisk = true;
            this.columnDefinition = null;
        } else {
            this.columnDefinition = new JsonColumnWithNonAggFunctionDefinion(ctx.jsonColumnWithNonAggFunction());
            this.isAsterisk = false;
        }
        if (ctx.AGG_FUNCTION() != null) {
            this.aggregateFunction = AggregateFunction.valueOf(ctx.AGG_FUNCTION().getText().toUpperCase());
        } else {
            this.aggregateFunction = null;
        }
    }

}
