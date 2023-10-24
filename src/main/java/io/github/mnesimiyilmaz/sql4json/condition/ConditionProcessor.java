package io.github.mnesimiyilmaz.sql4json.condition;

import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;
import io.github.mnesimiyilmaz.sql4json.definitions.JsonColumnWithNonAggFunctionDefinion;
import io.github.mnesimiyilmaz.sql4json.utils.ComparisonUtils;
import io.github.mnesimiyilmaz.sql4json.utils.FieldKey;
import io.github.mnesimiyilmaz.sql4json.utils.ValueUtils;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.regex.Pattern;

import static io.github.mnesimiyilmaz.sql4json.utils.ComparisonOperator.EQ;
import static io.github.mnesimiyilmaz.sql4json.utils.ComparisonOperator.NE;

/**
 * @author mnesimiyilmaz
 */
@Getter
@RequiredArgsConstructor
public class ConditionProcessor {

    private static final List<Function<SQL4JsonParser.ConditionContext, Optional<ConditionParts>>> CONDITION_CONTEXTS;

    private final SQL4JsonParser.ConditionsContext conditions;
    private final List<Object>                     tokens = new ArrayList<>();

    public CriteriaNode process() {
        for (int i = 0; i < conditions.getChildCount(); i++) {
            tokenizeConditions(conditions.getChild(i));
        }
        List<Object> postfixTokens = reorderInfixTokensToPostfix();
        return buildAST(postfixTokens);
    }

    private CriteriaNode buildAST(List<Object> postfixTokens) {
        Deque<CriteriaNode> stack = new ArrayDeque<>();
        for (Object token : postfixTokens) {
            if (token instanceof CriteriaNode) {
                stack.push((CriteriaNode) token);
            } else if (token instanceof String) {
                CriteriaNode logicalNode;
                if ("and".equalsIgnoreCase((String) token)) {
                    logicalNode = new AndNode(stack.pop(), stack.pop());
                } else {
                    logicalNode = new OrNode(stack.pop(), stack.pop());
                }
                stack.push(logicalNode);
            }
        }
        return stack.isEmpty() ? null : stack.pop();
    }

    private List<Object> reorderInfixTokensToPostfix() {
        List<Object> orderedTokens = new ArrayList<>();
        Deque<String> operatorStack = new ArrayDeque<>();
        for (Object token : tokens) {
            if (token instanceof CriteriaNode) {
                orderedTokens.add(token);
            } else if ("(".equals(token)) {
                operatorStack.push("(");
            } else if (")".equals(token)) {
                while (!operatorStack.isEmpty() && !"(".equals(operatorStack.peek())) {
                    orderedTokens.add(operatorStack.pop());
                }
                operatorStack.pop();
            } else {
                operatorStack.push(token.toString());
            }
        }
        while (!operatorStack.isEmpty()) {
            orderedTokens.add(operatorStack.pop());
        }
        return orderedTokens;
    }

    private void tokenizeConditions(Object token) {
        if (token instanceof SQL4JsonParser.NestedConditionContext) {
            for (int i = 0; i < ((SQL4JsonParser.NestedConditionContext) token).getChildCount(); i++) {
                tokenizeConditions(((SQL4JsonParser.NestedConditionContext) token).getChild(i));
            }
        } else if (token instanceof SQL4JsonParser.ConditionContext) {
            tokens.add(processCondition((SQL4JsonParser.ConditionContext) token));
        } else if (token instanceof TerminalNode) {
            tokens.add(((TerminalNode) token).getText());
        } else {
            throw new IllegalStateException("Token is not supported " + token.toString());
        }
    }

    private CriteriaNode processCondition(SQL4JsonParser.ConditionContext condition) {
        ConditionParts conditionParts = getConditionParts(condition);
        ComparisonNode comparisonNode = new ComparisonNode(
                FieldKey.of(conditionParts.getColumnDefinition().getColumnName()),
                conditionParts.getTestValue(),
                conditionParts.getOperation());
        conditionParts.getColumnDefinition().getValueDecorator().ifPresent(comparisonNode::setValueDecorator);
        return comparisonNode;
    }

    private ConditionParts getConditionParts(SQL4JsonParser.ConditionContext condition) {
        for (Function<SQL4JsonParser.ConditionContext, Optional<ConditionParts>> conditionContext : CONDITION_CONTEXTS) {
            Optional<ConditionParts> result = conditionContext.apply(condition);
            if (result.isPresent()) {
                return result.get();
            }
        }
        throw new IllegalStateException("Condition is not supported " + condition.getText());
    }

    static {
        List<Function<SQL4JsonParser.ConditionContext, Optional<ConditionParts>>> contextList = new ArrayList<>();
        contextList.add(c -> {
            if (c.comparison() != null) {
                JsonColumnWithNonAggFunctionDefinion columnDefinition = new JsonColumnWithNonAggFunctionDefinion(c.comparison().jsonColumnWithNonAggFunction());
                BiPredicate<Object, Object> operation = ComparisonUtils.getComparisonPredicate(c.comparison().COMPARISON_OPERATOR().getText());
                Object testValue = ValueUtils.getValueFromContext(c.comparison().valueWithNonAggFunction());
                return Optional.of(new ConditionParts(columnDefinition, testValue, operation));
            }
            return Optional.empty();
        });
        contextList.add(c -> {
            if (c.isNull() != null) {
                JsonColumnWithNonAggFunctionDefinion columnDefinition = new JsonColumnWithNonAggFunctionDefinion(c.isNull().jsonColumnWithNonAggFunction());
                BiPredicate<Object, Object> operation = ComparisonUtils.getComparisonPredicate(EQ.getExp());
                return Optional.of(new ConditionParts(columnDefinition, null, operation));
            }
            return Optional.empty();
        });
        contextList.add(c -> {
            if (c.isNotNull() != null) {
                JsonColumnWithNonAggFunctionDefinion columnDefinition = new JsonColumnWithNonAggFunctionDefinion(c.isNotNull().jsonColumnWithNonAggFunction());
                BiPredicate<Object, Object> operation = ComparisonUtils.getComparisonPredicate(NE.getExp());
                return Optional.of(new ConditionParts(columnDefinition, null, operation));
            }
            return Optional.empty();
        });
        contextList.add(c -> {
            if (c.like() != null) {
                JsonColumnWithNonAggFunctionDefinion columnDefinition = new JsonColumnWithNonAggFunctionDefinion(c.like().jsonColumnWithNonAggFunction());
                BiPredicate<Object, Object> operation = (x, y) -> {
                    if (x == null || y == null) {
                        return false;
                    }
                    return Pattern.compile(convertSqlLikeToRegex((String) y)).matcher((String) x).matches();
                };
                String testValue = c.like().STRING().getText();
                return Optional.of(new ConditionParts(columnDefinition, testValue.substring(1, testValue.length() - 1), operation));
            }
            return Optional.empty();
        });
        CONDITION_CONTEXTS = Collections.unmodifiableList(contextList);
    }

    private static String convertSqlLikeToRegex(String sqlLike) {
        return sqlLike.replace("%", ".*");
    }

    @Getter
    @RequiredArgsConstructor
    private static class ConditionParts {
        private final JsonColumnWithNonAggFunctionDefinion columnDefinition;
        private final Object                               testValue;
        private final BiPredicate<Object, Object>          operation;
    }

}
