package io.github.mnesimiyilmaz.sql4json.definitions;

import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;
import io.github.mnesimiyilmaz.sql4json.utils.OrderByDirection;
import lombok.Getter;

/**
 * @author mnesimiyilmaz
 */
@Getter
public class OrderByColumnDefinion {

    private final SQL4JsonParser.OrderByColumnContext  columnContext;
    private final JsonColumnWithNonAggFunctionDefinion columnDefinion;
    private final OrderByDirection                     direction;

    public OrderByColumnDefinion(SQL4JsonParser.OrderByColumnContext columnContext) {
        this.columnContext = columnContext;
        this.columnDefinion = new JsonColumnWithNonAggFunctionDefinion(columnContext.jsonColumnWithNonAggFunction());
        if (columnContext.ORDER_DIRECTION() != null) {
            this.direction = OrderByDirection.valueOf(columnContext.ORDER_DIRECTION().toString().toUpperCase());
        } else {
            this.direction = OrderByDirection.ASC;
        }
    }

}
