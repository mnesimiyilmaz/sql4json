package io.github.mnesimiyilmaz.sql4json.utils;

import lombok.Getter;

/**
 * @author mnesimiyilmaz
 */
@Getter
public enum ComparisonOperator {
    EQ("="),
    NE("!="),
    LT("<"),
    GT(">"),
    LTE("<="),
    GTE(">=");

    private final String exp;

    ComparisonOperator(String exp) {
        this.exp = exp;
    }

}
