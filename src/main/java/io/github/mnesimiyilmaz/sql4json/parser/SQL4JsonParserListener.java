package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.Expression.WhenClause;
import io.github.mnesimiyilmaz.sql4json.engine.WindowSpec;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonBaseListener;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;
import io.github.mnesimiyilmaz.sql4json.registry.*;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.*;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.Interval;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * ANTLR listener that walks a parsed sql4json tree and produces a QueryDefinition.
 * Uses ConditionHandlerRegistry to build v2 registry.CriteriaNode for WHERE/HAVING.
 * <p>
 * One instance per parse — not thread-safe, not reusable.
 * <p>
 * NOTE: buildCriteriaNode/buildConditionContext perform their own recursive traversal of the
 * conditions subtree. Do not add enter* overrides for rules within conditions (comparison,
 * like, isNull, isNotNull) as they would fire redundantly.
 */
class SQL4JsonParserListener extends SQL4JsonBaseListener {

    private final CommonTokenStream        tokenStream;
    private final ConditionHandlerRegistry conditionHandlerRegistry;
    private final FunctionRegistry         functionRegistry;
    private final Sql4jsonSettings         settings;

    // Accumulated query state
    private final List<SelectColumnDef>  selectedColumns          = new ArrayList<>();
    private       String                 rootPath                 = "$r";
    private       String                 fromSubQuery             = null;
    private       CriteriaNode           whereClause              = null;
    private       List<Expression>       groupBy                  = null;
    private       CriteriaNode           havingClause             = null;
    private       List<OrderByColumnDef> orderBy                  = null;
    private final Set<String>            referencedFields         = new LinkedHashSet<>();
    private       boolean                distinct                 = false;
    private       Integer                limit                    = null;
    private       Integer                offset                   = null;
    private       boolean                containsNonDeterministic = false;
    private       String                 rootAlias                = null;
    private       List<JoinDef>          joins                    = null;

    // Depth guard: once we enter a FROM subquery, suppress callbacks for inner nodes.
    // The inner SQL is reparsed independently by QueryExecutor.
    private boolean insideSubquery = false;

    SQL4JsonParserListener(CommonTokenStream tokenStream,
                           ConditionHandlerRegistry conditionHandlerRegistry,
                           FunctionRegistry functionRegistry,
                           Sql4jsonSettings settings) {
        this.tokenStream = tokenStream;
        this.conditionHandlerRegistry = conditionHandlerRegistry;
        this.functionRegistry = functionRegistry;
        this.settings = settings;
    }

    // ── Listener overrides ────────────────────────────────────────────────────

    @Override
    public void enterSelectedColumns(SQL4JsonParser.SelectedColumnsContext ctx) {
        if (insideSubquery) return;
        if (ctx.ASTERISK() != null) {
            selectedColumns.add(SelectColumnDef.asterisk());
        }
    }

    @Override
    public void enterSelectColumn(SQL4JsonParser.SelectColumnContext ctx) {
        if (insideSubquery) return;
        selectedColumns.add(buildSelectColumnDef(ctx));
    }

    @Override
    public void enterRootNode(SQL4JsonParser.RootNodeContext ctx) {
        if (insideSubquery) return;
        if (ctx.sql4json() != null) {
            // FROM (SELECT ...) subquery — capture raw SQL text preserving original whitespace.
            // tokenStream.getText() strips hidden whitespace tokens; use the char stream instead.
            var inner = ctx.sql4json();
            this.fromSubQuery = inner.start.getInputStream()
                    .getText(new Interval(inner.start.getStartIndex(), inner.stop.getStopIndex()));
            this.insideSubquery = true; // suppress callbacks for the inner sql4json subtree
        } else if (ctx.ROOT() != null) {
            // FROM $r or FROM $r.path
            this.rootPath = ctx.getText();
        } else if (ctx.identifierOrKeyword() != null) {
            // FROM tableName alias?
            this.rootPath = ctx.identifierOrKeyword().getText();
            var aliasToken = ctx.IDENTIFIER();
            this.rootAlias = aliasToken != null ? aliasToken.getText() : this.rootPath;
        }
    }

    @Override
    public void exitRootNode(SQL4JsonParser.RootNodeContext ctx) {
        // After leaving the rootNode that contained the subquery, re-enable outer-query callbacks.
        // This allows the outer WHERE / GROUP BY / HAVING / ORDER BY clauses to be processed.
        if (ctx.sql4json() != null) {
            this.insideSubquery = false;
        }
    }

    @Override
    public void enterSql4json(SQL4JsonParser.Sql4jsonContext ctx) {
        if (insideSubquery) return;
        this.distinct = ctx.DISTINCT() != null;
    }

    @Override
    public void exitSql4json(SQL4JsonParser.Sql4jsonContext ctx) {
        if (insideSubquery) return;
        if (ctx.LIMIT() != null) {
            this.limit = parseNonNegativeToken(ctx.LIMIT().getSymbol().getTokenIndex(), "LIMIT");
            if (ctx.OFFSET() != null) {
                this.offset = parseNonNegativeToken(ctx.OFFSET().getSymbol().getTokenIndex(), "OFFSET");
            }
        }
    }

    /**
     * Finds the NUMBER token immediately following the given keyword token and validates it
     * is non-negative.
     */
    private Integer parseNonNegativeToken(int keywordTokenIndex, String clauseName) {
        org.antlr.v4.runtime.Token numToken = nextNonHiddenToken(keywordTokenIndex + 1);
        if (numToken == null) return null;
        int parsed = Integer.parseInt(numToken.getText());
        if (parsed < 0) {
            throw new SQL4JsonExecutionException(
                    clauseName + " must be non-negative, got: " + parsed);
        }
        return parsed;
    }

    /**
     * Returns the first non-hidden-channel token at or after the given index.
     */
    private org.antlr.v4.runtime.Token nextNonHiddenToken(int startIndex) {
        var tokens = tokenStream.getTokens();
        for (int i = startIndex; i < tokens.size(); i++) {
            var t = tokens.get(i);
            if (t.getChannel() == org.antlr.v4.runtime.Token.DEFAULT_CHANNEL) {
                return t;
            }
        }
        return null;
    }

    @Override
    public void enterWhereConditions(SQL4JsonParser.WhereConditionsContext ctx) {
        if (insideSubquery) return;
        this.whereClause = buildCriteriaNode(ctx.conditions());
    }

    @Override
    public void enterHavingConditions(SQL4JsonParser.HavingConditionsContext ctx) {
        if (insideSubquery) return;
        this.havingClause = buildCriteriaNode(ctx.conditions());
    }

    @Override
    public void enterGroupByColumn(SQL4JsonParser.GroupByColumnContext ctx) {
        if (insideSubquery) return;
        if (groupBy == null) groupBy = new ArrayList<>();
        groupBy.add(trackExpression(ctx.columnExpr()));
    }

    @Override
    public void enterOrderByColumn(SQL4JsonParser.OrderByColumnContext ctx) {
        if (insideSubquery) return;
        if (orderBy == null) orderBy = new ArrayList<>();
        String dir = ctx.ORDER_DIRECTION() != null
                ? ctx.ORDER_DIRECTION().getText().toUpperCase()
                : "ASC";
        orderBy.add(OrderByColumnDef.of(trackExpression(ctx.columnExpr()), dir));
    }

    @Override
    public void enterJoinClause(SQL4JsonParser.JoinClauseContext ctx) {
        if (insideSubquery) return;
        if (joins == null) joins = new ArrayList<>();

        // Parse join type (default INNER)
        JoinType joinType = JoinType.INNER;
        if (ctx.joinType() != null) {
            joinType = switch (ctx.joinType().getText().toUpperCase()) {
                case "INNER" -> JoinType.INNER;
                case "LEFT" -> JoinType.LEFT;
                case "RIGHT" -> JoinType.RIGHT;
                default -> throw new SQL4JsonExecutionException(
                        "Invalid join type: " + ctx.joinType().getText());
            };
        }

        // Parse table name and optional alias
        String tableName = ctx.identifierOrKeyword().getText();
        var aliasToken = ctx.IDENTIFIER();
        String alias = aliasToken != null ? aliasToken.getText() : tableName;

        // Parse ON conditions
        var onConditions = new ArrayList<JoinEquality>();
        for (var eqCtx : ctx.joinCondition().joinEquality()) {
            String op = eqCtx.COMPARISON_OPERATOR().getText();
            if (!"=".equals(op)) {
                int line = eqCtx.COMPARISON_OPERATOR().getSymbol().getLine();
                int col = eqCtx.COMPARISON_OPERATOR().getSymbol().getCharPositionInLine();
                throw new SQL4JsonParseException(
                        "ON condition only supports '=' operator, got: '" + op + "'",
                        line, col);
            }
            String leftPath = eqCtx.jsonColumn(0).getText();
            String rightPath = eqCtx.jsonColumn(1).getText();
            onConditions.add(new JoinEquality(leftPath, rightPath));
        }

        joins.add(new JoinDef(tableName, alias, joinType, onConditions));
    }

    // ── Build result ──────────────────────────────────────────────────────────

    QueryDefinition buildQueryDefinition() {
        return new QueryDefinition(
                List.copyOf(selectedColumns),
                rootPath,
                fromSubQuery,
                whereClause,
                groupBy,
                havingClause,
                orderBy,
                Set.copyOf(referencedFields),
                distinct,
                limit,
                offset,
                containsNonDeterministic,
                rootAlias,
                joins != null ? List.copyOf(joins) : null);
    }

    // ── Conditions tree → v2 CriteriaNode ────────────────────────────────────

    private CriteriaNode buildCriteriaNode(SQL4JsonParser.ConditionsContext ctx) {
        if (ctx instanceof SQL4JsonParser.OrConditionsContext orCtx) {
            return new OrNode(
                    buildCriteriaNode(orCtx.conditions(0)),
                    buildCriteriaNode(orCtx.conditions(1)));
        } else if (ctx instanceof SQL4JsonParser.AndConditionsContext andCtx) {
            return new AndNode(
                    buildCriteriaNode(andCtx.conditions(0)),
                    buildCriteriaNode(andCtx.conditions(1)));
        } else if (ctx instanceof SQL4JsonParser.ParenConditionsContext parenCtx) {
            return buildCriteriaNode(parenCtx.conditions());
        } else if (ctx instanceof SQL4JsonParser.SingleConditionContext singleCtx) {
            ConditionContext cc = buildConditionContext(singleCtx.condition());
            if (cc.lhsExpression() != null) {
                trackReferencedFields(cc.lhsExpression());
            }
            return conditionHandlerRegistry.resolve(cc);
        }
        throw new SQL4JsonExecutionException(
                "Unsupported conditions context: " + ctx.getClass().getSimpleName());
    }

    private ConditionContext buildConditionContext(SQL4JsonParser.ConditionContext ctx) {
        if (ctx instanceof SQL4JsonParser.ComparisonConditionContext compCtx) {
            var comp = compCtx.comparison();
            Expression lhs = buildExpression(comp.columnExpr());
            Expression rhs = buildRhsExpression(comp.rhsValue());
            SqlValue testVal = (rhs instanceof Expression.LiteralVal(var val)) ? val : null;
            return new ConditionContext(ConditionContext.ConditionType.COMPARISON,
                    lhs, comp.COMPARISON_OPERATOR().getText(), testVal, rhs, null, null, null);
        } else if (ctx instanceof SQL4JsonParser.LikeConditionContext likeCtx) {
            Expression lhs = buildExpression(likeCtx.like().columnExpr());
            Expression rhs = buildRhsExpression(likeCtx.like().rhsValue());
            SqlValue testVal = (rhs instanceof Expression.LiteralVal(var val)) ? val : null;
            enforceLikeWildcardLimit(testVal);
            return new ConditionContext(ConditionContext.ConditionType.LIKE,
                    lhs, "LIKE", testVal, rhs, null, null, null);
        } else if (ctx instanceof SQL4JsonParser.IsNullConditionContext nullCtx) {
            Expression lhs = buildExpression(nullCtx.isNull().columnExpr());
            return new ConditionContext(ConditionContext.ConditionType.IS_NULL,
                    lhs, null, null, null, null, null, null);
        } else if (ctx instanceof SQL4JsonParser.IsNotNullConditionContext notNullCtx) {
            Expression lhs = buildExpression(notNullCtx.isNotNull().columnExpr());
            return new ConditionContext(ConditionContext.ConditionType.IS_NOT_NULL,
                    lhs, null, null, null, null, null, null);
        } else if (ctx instanceof SQL4JsonParser.InConditionContext inCtx) {
            var inRule = inCtx.in();
            Expression lhs = buildExpression(inRule.columnExpr());
            List<SqlValue> values = inRule.rhsValue().stream()
                    .map(this::getRhsSqlValue)
                    .toList();
            enforceInListSizeLimit(values.size());
            return new ConditionContext(ConditionContext.ConditionType.IN,
                    lhs, null, null, null, values, null, null);
        } else if (ctx instanceof SQL4JsonParser.NotInConditionContext notInCtx) {
            var notInRule = notInCtx.notIn();
            Expression lhs = buildExpression(notInRule.columnExpr());
            List<SqlValue> values = notInRule.rhsValue().stream()
                    .map(this::getRhsSqlValue)
                    .toList();
            enforceInListSizeLimit(values.size());
            return new ConditionContext(ConditionContext.ConditionType.NOT_IN,
                    lhs, null, null, null, values, null, null);
        } else if (ctx instanceof SQL4JsonParser.BetweenConditionContext betweenCtx) {
            var rule = betweenCtx.between();
            return buildBetweenContext(rule.columnExpr(), rule.rhsValue(0), rule.rhsValue(1),
                    ConditionContext.ConditionType.BETWEEN);
        } else if (ctx instanceof SQL4JsonParser.NotBetweenConditionContext notBetweenCtx) {
            var rule = notBetweenCtx.notBetween();
            return buildBetweenContext(rule.columnExpr(), rule.rhsValue(0), rule.rhsValue(1),
                    ConditionContext.ConditionType.NOT_BETWEEN);
        } else if (ctx instanceof SQL4JsonParser.NotLikeConditionContext notLikeCtx) {
            var notLikeRule = notLikeCtx.notLike();
            Expression lhs = buildExpression(notLikeRule.columnExpr());
            Expression rhs = buildRhsExpression(notLikeRule.rhsValue());
            SqlValue testVal = (rhs instanceof Expression.LiteralVal(var val)) ? val : null;
            enforceLikeWildcardLimit(testVal);
            return new ConditionContext(ConditionContext.ConditionType.NOT_LIKE,
                    lhs, "NOT LIKE", testVal, rhs, null, null, null);
        }
        throw new SQL4JsonExecutionException(
                "Unsupported condition context: " + ctx.getClass().getSimpleName());
    }

    private ConditionContext buildBetweenContext(SQL4JsonParser.ColumnExprContext colExpr,
                                                 SQL4JsonParser.RhsValueContext lowerRhs,
                                                 SQL4JsonParser.RhsValueContext upperRhs,
                                                 ConditionContext.ConditionType type) {
        Expression lhs = buildExpression(colExpr);
        SqlValue lower = getRhsSqlValue(lowerRhs);
        SqlValue upper = getRhsSqlValue(upperRhs);
        return new ConditionContext(type, lhs, null, null, null, null, lower, upper);
    }

    // ── Expression tree builders ─────────────────────────────────────────────

    /**
     * Recursively builds an Expression tree from a columnExpr parse node.
     */
    private Expression buildExpression(SQL4JsonParser.ColumnExprContext ctx) {
        if (ctx instanceof SQL4JsonParser.CaseExprColumnContext caseCtx) {
            return buildCaseExpression(caseCtx.caseExpr());
        }
        if (ctx instanceof SQL4JsonParser.WindowFunctionExprContext winCtx) {
            return buildWindowExpression(winCtx.windowFunctionCall());
        }
        if (ctx instanceof SQL4JsonParser.AggFunctionColumnExprContext aggCtx) {
            String aggFn = aggCtx.AGG_FUNCTION().getText().toUpperCase();
            Expression inner = aggCtx.ASTERISK() != null ? null : buildExpression(aggCtx.columnExpr());
            return new Expression.AggregateFnCall(aggFn, inner);
        }
        if (ctx instanceof SQL4JsonParser.FunctionCallExprContext fnCtx) {
            var scalarCall = (SQL4JsonParser.ScalarFunctionCallContext) fnCtx.functionCall();
            String name = scalarCall.identifierOrKeyword().getText().toLowerCase();
            var args = new ArrayList<Expression>();
            if (scalarCall.functionArgs() != null) {
                for (var argCtx : scalarCall.functionArgs().functionArg()) {
                    args.add(buildFunctionArgExpression(argCtx));
                }
            }
            return new Expression.ScalarFnCall(name, args);
        }
        if (ctx instanceof SQL4JsonParser.CastExprColumnContext castCtx) {
            String typeName = castCtx.castExpr().castType().getText().toUpperCase();
            Expression inner = buildExpression(castCtx.castExpr().columnExpr());
            return new Expression.ScalarFnCall("cast", List.of(inner, new Expression.LiteralVal(new SqlString(typeName))));
        }
        if (ctx instanceof SQL4JsonParser.LiteralColumnExprContext litCtx) {
            if (litCtx.value().VALUE_FUNCTION() != null) {
                containsNonDeterministic = true;
                return new Expression.NowRef();
            }
            return new Expression.LiteralVal(toSqlValue(litCtx.value()));
        }
        // SimpleColumnExprContext
        return new Expression.ColumnRef(ctx.getText());
    }

    private Expression buildCaseExpression(SQL4JsonParser.CaseExprContext ctx) {
        var columnExprs = ctx.columnExpr();
        var conditionsCtxList = ctx.conditions();

        if (!conditionsCtxList.isEmpty()) {
            // Searched CASE: CASE (WHEN conditions THEN columnExpr)+ (ELSE columnExpr)? END
            var whenClauses = new ArrayList<WhenClause.SearchWhen>();
            for (int i = 0; i < conditionsCtxList.size(); i++) {
                CriteriaNode criteriaNode = buildCriteriaNode(conditionsCtxList.get(i));
                var condFields = new LinkedHashSet<String>();
                collectConditionFields(conditionsCtxList.get(i), condFields);
                Expression result = buildExpression(columnExprs.get(i));
                whenClauses.add(new WhenClause.SearchWhen(criteriaNode, Set.copyOf(condFields), result));
            }
            Expression elseExpr = columnExprs.size() > conditionsCtxList.size()
                    ? buildExpression(columnExprs.getLast())
                    : null;
            return new Expression.SearchedCaseWhen(whenClauses, elseExpr);
        } else {
            // Simple CASE: CASE columnExpr (WHEN columnExpr THEN columnExpr)+ (ELSE columnExpr)? END
            Expression subject = buildExpression(columnExprs.getFirst());
            var whenClauses = new ArrayList<WhenClause.ValueWhen>();
            int i = 1;
            while (i + 1 < columnExprs.size()) {
                if (i + 2 == columnExprs.size() && ctx.ELSE() != null) break;
                Expression value = buildExpression(columnExprs.get(i));
                Expression result = buildExpression(columnExprs.get(i + 1));
                whenClauses.add(new WhenClause.ValueWhen(value, result));
                i += 2;
            }
            Expression elseExpr = ctx.ELSE() != null
                    ? buildExpression(columnExprs.getLast())
                    : null;
            return new Expression.SimpleCaseWhen(subject, whenClauses, elseExpr);
        }
    }

    private void collectConditionFields(SQL4JsonParser.ConditionsContext ctx, Set<String> fields) {
        if (ctx instanceof SQL4JsonParser.OrConditionsContext orCtx) {
            collectConditionFields(orCtx.conditions(0), fields);
            collectConditionFields(orCtx.conditions(1), fields);
        } else if (ctx instanceof SQL4JsonParser.AndConditionsContext andCtx) {
            collectConditionFields(andCtx.conditions(0), fields);
            collectConditionFields(andCtx.conditions(1), fields);
        } else if (ctx instanceof SQL4JsonParser.ParenConditionsContext parenCtx) {
            collectConditionFields(parenCtx.conditions(), fields);
        } else if (ctx instanceof SQL4JsonParser.SingleConditionContext singleCtx) {
            ConditionContext cc = buildConditionContext(singleCtx.condition());
            if (cc.lhsExpression() != null) {
                cc.lhsExpression().collectReferencedFields(fields);
            }
        }
    }

    /**
     * Builds a WindowFnCall expression from a windowFunctionCall parse node.
     */
    private Expression buildWindowExpression(SQL4JsonParser.WindowFunctionCallContext winCall) {
        String name = winCall.windowFunctionName().getText().toUpperCase();

        List<Expression> args = new ArrayList<>();
        if (winCall.windowFunctionArgs() != null) {
            var argsCtx = winCall.windowFunctionArgs();
            if (argsCtx.ASTERISK() == null) {
                for (var colExpr : argsCtx.columnExpr()) {
                    args.add(buildExpression(colExpr));
                }
            }
            // COUNT(*) → empty args list (signals "count all rows")
        }

        WindowSpec spec = buildWindowSpec(winCall.windowSpec());
        return new Expression.WindowFnCall(name, args, spec);
    }

    private WindowSpec buildWindowSpec(SQL4JsonParser.WindowSpecContext ctx) {
        List<Expression> partitionBy = new ArrayList<>();
        if (ctx.PARTITION() != null) {
            for (var colExpr : ctx.columnExpr()) {
                partitionBy.add(buildExpression(colExpr));
            }
        }
        List<OrderByColumnDef> orderBySpec = new ArrayList<>();
        if (ctx.ORDER() != null) {
            for (var obCol : ctx.orderByColumn()) {
                String dir = obCol.ORDER_DIRECTION() != null
                        ? obCol.ORDER_DIRECTION().getText().toUpperCase()
                        : "ASC";
                orderBySpec.add(OrderByColumnDef.of(buildExpression(obCol.columnExpr()), dir));
            }
        }
        return new WindowSpec(partitionBy, orderBySpec);
    }

    /**
     * Builds an Expression from a columnExpr and registers its referenced fields.
     */
    private Expression trackExpression(SQL4JsonParser.ColumnExprContext ctx) {
        Expression expr = buildExpression(ctx);
        trackReferencedFields(expr);
        return expr;
    }

    /**
     * Registers all column paths in the given expression as referenced fields.
     */
    private void trackReferencedFields(Expression expr) {
        var fields = new LinkedHashSet<String>();
        expr.collectReferencedFields(fields);
        referencedFields.addAll(fields);
    }

    /**
     * Builds an Expression from a functionArg parse node.
     * After grammar change, functionArg is: jsonColumnWithAggFunction | value
     */
    private Expression buildFunctionArgExpression(SQL4JsonParser.FunctionArgContext ctx) {
        if (ctx instanceof SQL4JsonParser.ExprFunctionArgContext exprCtx) {
            var aggOrExpr = exprCtx.jsonColumnWithAggFunction();
            if (aggOrExpr instanceof SQL4JsonParser.WindowFunctionAggExprContext winAggCtx) {
                return buildWindowExpression(winAggCtx.windowFunctionCall());
            }
            if (aggOrExpr instanceof SQL4JsonParser.AggFunctionExprContext aggCtx) {
                String aggFn = aggCtx.AGG_FUNCTION().getText().toUpperCase();
                Expression inner = aggCtx.ASTERISK() != null ? null : buildExpression(aggCtx.columnExpr());
                return new Expression.AggregateFnCall(aggFn, inner);
            }
            var nonAgg = (SQL4JsonParser.NonAggExprContext) aggOrExpr;
            return buildExpression(nonAgg.columnExpr());
        }
        if (ctx instanceof SQL4JsonParser.ValueFunctionArgContext vfa) {
            if (vfa.value().VALUE_FUNCTION() != null) {
                containsNonDeterministic = true;
                return new Expression.NowRef();
            }
            return new Expression.LiteralVal(toSqlValue(vfa.value()));
        }
        throw new SQL4JsonExecutionException("Unsupported function arg: " + ctx.getClass().getSimpleName());
    }

    // ── RHS value/expression extraction ─────────────────────────────────────────

    /**
     * Builds an Expression from an rhsValue parse node.
     * Used by COMPARISON, LIKE, NOT LIKE to support column refs, functions, CAST, and literals.
     */
    private Expression buildRhsExpression(SQL4JsonParser.RhsValueContext ctx) {
        if (ctx instanceof SQL4JsonParser.RhsPlainValueContext pv) {
            return new Expression.LiteralVal(toSqlValue(pv.value()));
        }
        if (ctx instanceof SQL4JsonParser.RhsColumnRefContext colCtx) {
            Expression colExpr = new Expression.ColumnRef(colCtx.jsonColumn().getText());
            trackReferencedFields(colExpr);
            return colExpr;
        }
        if (ctx instanceof SQL4JsonParser.RhsCastExprContext castCtx) {
            var castExpr = castCtx.castExpr();
            String typeName = castExpr.castType().getText().toUpperCase();
            Expression inner = buildExpression(castExpr.columnExpr());
            return new Expression.ScalarFnCall("cast", List.of(inner, new Expression.LiteralVal(new SqlString(typeName))));
        }
        // RhsFunctionCallContext — eagerly evaluate since no column refs on RHS functions
        var fnCtx = (SQL4JsonParser.RhsFunctionCallContext) ctx;
        var fn = (SQL4JsonParser.ScalarFunctionCallContext) fnCtx.functionCall();
        SqlValue evaluated = evaluateRhsScalarFunction(fn);
        return new Expression.LiteralVal(evaluated);
    }

    /**
     * Eagerly evaluates an rhsValue to a SqlValue. Used by IN/NOT IN and BETWEEN
     * where column references on the RHS are not supported.
     */
    private SqlValue getRhsSqlValue(SQL4JsonParser.RhsValueContext ctx) {
        if (ctx instanceof SQL4JsonParser.RhsPlainValueContext pv) {
            return toSqlValue(pv.value());
        }
        if (ctx instanceof SQL4JsonParser.RhsColumnRefContext) {
            throw new SQL4JsonExecutionException(
                    "Column references not supported in IN/BETWEEN lists: " + ctx.getText());
        }
        if (ctx instanceof SQL4JsonParser.RhsCastExprContext castCtx) {
            return evaluateRhsCast(castCtx.castExpr());
        }
        var fnCtx = (SQL4JsonParser.RhsFunctionCallContext) ctx;
        var fn = (SQL4JsonParser.ScalarFunctionCallContext) fnCtx.functionCall();
        return evaluateRhsScalarFunction(fn);
    }

    private void enforceInListSizeLimit(int actualSize) {
        int max = settings.limits().maxInListSize();
        if (actualSize > max) {
            throw new SQL4JsonExecutionException(
                    "IN list size exceeds configured maximum (" + max + ")");
        }
    }

    private void enforceLikeWildcardLimit(SqlValue testVal) {
        if (!(testVal instanceof SqlString(String patternStr))) {
            return; // not a literal string — cannot check statically, skip
        }
        int count = 0;
        for (int i = 0; i < patternStr.length(); i++) {
            if (patternStr.charAt(i) == '%') count++;
        }
        int max = settings.security().maxLikeWildcards();
        if (count > max) {
            throw new SQL4JsonExecutionException(
                    "LIKE pattern wildcard count exceeds configured maximum (" + max + ")");
        }
    }

    /**
     * Eagerly evaluates a column expression on the RHS (for IN/BETWEEN lists and RHS CAST inner).
     * Supports literals, functions, and CAST — but not column references.
     */
    private SqlValue evaluateRhsColumnExpr(SQL4JsonParser.ColumnExprContext ctx) {
        if (ctx instanceof SQL4JsonParser.FunctionCallExprContext fnCtx) {
            var fn = (SQL4JsonParser.ScalarFunctionCallContext) fnCtx.functionCall();
            return evaluateRhsScalarFunction(fn);
        }
        if (ctx instanceof SQL4JsonParser.LiteralColumnExprContext litCtx) {
            return toSqlValue(litCtx.value());
        }
        if (ctx instanceof SQL4JsonParser.CastExprColumnContext castCtx) {
            return evaluateRhsCast(castCtx.castExpr());
        }
        if (ctx instanceof SQL4JsonParser.WindowFunctionExprContext) {
            throw new SQL4JsonExecutionException(
                    "Window functions not supported in this context: " + ctx.getText());
        }
        throw new SQL4JsonExecutionException(
                "Column references not supported in this context: " + ctx.getText());
    }

    private SqlValue evaluateRhsCast(SQL4JsonParser.CastExprContext castExpr) {
        String typeName = castExpr.castType().getText().toUpperCase();
        SqlValue innerVal = evaluateRhsColumnExpr(castExpr.columnExpr());
        return functionRegistry.getScalar("cast")
                .map(f -> f.apply().apply(innerVal, List.of(new SqlString(typeName))))
                .orElseThrow(() -> new SQL4JsonExecutionException("CAST function not registered"));
    }

    /**
     * Evaluates a scalar function call where all arguments must resolve to literal values
     * (no column references). Supports nested function calls on the RHS.
     */
    private SqlValue evaluateRhsScalarFunction(SQL4JsonParser.ScalarFunctionCallContext fn) {
        String name = fn.identifierOrKeyword().getText().toLowerCase();
        var evaluatedArgs = evaluateRhsFunctionArgs(fn.functionArgs());
        SqlValue primary = evaluatedArgs.isEmpty() ? null : evaluatedArgs.getFirst();
        List<SqlValue> extraArgs = evaluatedArgs.size() <= 1
                ? List.of()
                : List.copyOf(evaluatedArgs.subList(1, evaluatedArgs.size()));
        return functionRegistry.getScalar(name)
                .map(f -> f.apply().apply(primary, extraArgs))
                .orElseThrow(() -> new SQL4JsonExecutionException(
                        "Unknown function in RHS: " + name));
    }

    /**
     * Evaluates function arguments on the RHS — each must be a literal value or a nested
     * function call (no column references or aggregates).
     */
    private List<SqlValue> evaluateRhsFunctionArgs(SQL4JsonParser.FunctionArgsContext argsCtx) {
        if (argsCtx == null || argsCtx.functionArg().isEmpty()) return List.of();
        var result = new ArrayList<SqlValue>();
        for (var arg : argsCtx.functionArg()) {
            result.add(evaluateRhsFunctionArg(arg));
        }
        return result;
    }

    private SqlValue evaluateRhsFunctionArg(SQL4JsonParser.FunctionArgContext arg) {
        if (arg instanceof SQL4JsonParser.ValueFunctionArgContext vfa) {
            return toSqlValue(vfa.value());
        }
        var exprArg = (SQL4JsonParser.ExprFunctionArgContext) arg;
        var aggOrExpr = exprArg.jsonColumnWithAggFunction();
        if (aggOrExpr instanceof SQL4JsonParser.WindowFunctionAggExprContext) {
            throw new SQL4JsonExecutionException(
                    "Window function not supported in this context: " + arg.getText());
        }
        if (aggOrExpr instanceof SQL4JsonParser.AggFunctionExprContext) {
            throw new SQL4JsonExecutionException(
                    "Aggregate function not supported in RHS: " + arg.getText());
        }
        var nonAgg = (SQL4JsonParser.NonAggExprContext) aggOrExpr;
        return evaluateRhsColumnExpr(nonAgg.columnExpr());
    }

    private SqlValue toSqlValue(SQL4JsonParser.ValueContext ctx) {
        if (ctx.STRING() != null) {
            String raw = ctx.STRING().getText();
            return new SqlString(raw.substring(1, raw.length() - 1).replace("''", "'"));
        }
        if (ctx.NUMBER() != null) {
            return SqlNumber.of(Double.parseDouble(ctx.NUMBER().getText()));
        }
        if (ctx.BOOLEAN() != null) {
            return SqlBoolean.of(Boolean.parseBoolean(ctx.BOOLEAN().getText().toLowerCase()));
        }
        if (ctx.NULL() != null) {
            return SqlNull.INSTANCE;
        }
        if (ctx.VALUE_FUNCTION() != null) {
            // NOW() — evaluated eagerly at parse time
            containsNonDeterministic = true;
            return new SqlDateTime(LocalDateTime.now());
        }
        throw new SQL4JsonExecutionException("Unsupported value: " + ctx.getText());
    }

    // ── SELECT column builder ─────────────────────────────────────────────────

    private SelectColumnDef buildSelectColumnDef(SQL4JsonParser.SelectColumnContext ctx) {
        String alias = (ctx.AS() != null && ctx.jsonColumn() != null)
                ? ctx.jsonColumn().getText()
                : null;
        var aggFnCtx = ctx.jsonColumnWithAggFunction();
        Expression expression = switch (aggFnCtx) {
            case SQL4JsonParser.WindowFunctionAggExprContext winAggCtx ->
                    buildWindowExpression(winAggCtx.windowFunctionCall());
            case SQL4JsonParser.AggFunctionExprContext aggCtx -> {
                String aggFn = aggCtx.AGG_FUNCTION().getText().toUpperCase();
                Expression inner = aggCtx.ASTERISK() != null ? null : buildExpression(aggCtx.columnExpr());
                yield new Expression.AggregateFnCall(aggFn, inner);
            }
            default -> buildExpression(((SQL4JsonParser.NonAggExprContext) aggFnCtx).columnExpr());
        };
        trackReferencedFields(expression);
        return SelectColumnDef.of(expression, alias);
    }
}
