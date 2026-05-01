package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.Expression.WhenClause;
import io.github.mnesimiyilmaz.sql4json.engine.WindowSpec;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonBaseListener;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;
import io.github.mnesimiyilmaz.sql4json.grammar.Category;
import io.github.mnesimiyilmaz.sql4json.grammar.SQL4JsonGrammar;
import io.github.mnesimiyilmaz.sql4json.registry.*;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.*;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * ANTLR listener that walks a parsed sql4json tree and produces a QueryDefinition.
 * Uses ConditionHandlerRegistry to build registry.CriteriaNode for WHERE/HAVING.
 * <p>
 * One instance per parse — not thread-safe, not reusable.
 * <p>
 * NOTE: buildCriteriaNode/buildConditionContext perform their own recursive traversal of the
 * conditions subtree. Do not add enter* overrides for rules within conditions (comparison,
 * like, isNull, isNotNull) as they would fire redundantly.
 */
class SQL4JsonParserListener extends SQL4JsonBaseListener {

    /**
     * Names of functions that are valid only inside an {@code OVER (...)} window call.
     * Derived (not duplicated) from the single source of truth: the grammar catalog
     * (`SQL4JsonGrammar.functions()` filtered by `Category.WINDOW`). Adding a new window
     * function to that catalog automatically extends this guard.
     */
    private static final Set<String> WINDOW_ONLY_FUNCTIONS = SQL4JsonGrammar.functions().stream()
            .filter(fi -> fi.category() == Category.WINDOW)
            .map(fi -> fi.name().toLowerCase())
            .collect(Collectors.toUnmodifiableSet());

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

    // Every WindowFnCall instance built during parsing — used by WindowStage to know
    // which windows to compute, including those buried inside CriteriaNodes for
    // CASE WHEN conditions (which are otherwise opaque). Records compare by value, so
    // syntactically equivalent calls share storage when looked up on the row.
    private final List<Expression.WindowFnCall> windowFunctionCalls = new ArrayList<>();

    // Depth guard: once we enter a FROM subquery, suppress callbacks for inner nodes.
    // The inner SQL is reparsed independently by QueryExecutor.
    private boolean insideSubquery = false;

    // Depth guard: orderByColumn is shared between the top-level ORDER BY clause and the
    // OVER (... ORDER BY ...) sub-rule of windowSpec. ANTLR's tree walk fires
    // enterOrderByColumn for both contexts; without this flag the window's order-by keys
    // would be appended to the query-level orderBy list and silently override the user's
    // outer ORDER BY at execution time. windowSpec is built via the explicit
    // buildWindowSpec recursive descent, so suppressing the listener inside it is safe.
    private int windowSpecDepth = 0;

    // Parameter-binding state
    private       boolean                         sawPositional   = false;
    private       boolean                         sawNamed        = false;
    private       int                             positionalCount = 0;
    private final java.util.LinkedHashSet<String> namedParameters = new java.util.LinkedHashSet<>();
    private       Expression.ParameterRef         limitParam      = null;
    private       Expression.ParameterRef         offsetParam     = null;

    // Stack of parameter-position kinds. The top of the stack is the kind applied to any
    // ParameterRef constructed during the current builder region. Pushed/popped via
    // withParameterKind around regions that change the syntactic context (IN list,
    // ARRAY[...] elements, bare-array RHS).
    private final java.util.Deque<ParameterPositionKind> parameterKindStack     =
            new java.util.ArrayDeque<>(java.util.List.of(ParameterPositionKind.REGULAR_SCALAR));
    private final int                                    positionalOffset;  // injected via constructor — starting offset for subquery re-parse
    private       int                                    capturedSubqueryOffset = 0;  // set when entering a FROM subquery; consumed by the inner re-parse

    SQL4JsonParserListener(CommonTokenStream tokenStream,
                           ConditionHandlerRegistry conditionHandlerRegistry,
                           FunctionRegistry functionRegistry,
                           Sql4jsonSettings settings) {
        this(tokenStream, conditionHandlerRegistry, functionRegistry, settings, 0);
    }

    SQL4JsonParserListener(CommonTokenStream tokenStream,
                           ConditionHandlerRegistry conditionHandlerRegistry,
                           FunctionRegistry functionRegistry,
                           Sql4jsonSettings settings,
                           int positionalOffset) {
        this.tokenStream = tokenStream;
        this.conditionHandlerRegistry = conditionHandlerRegistry;
        this.functionRegistry = functionRegistry;
        this.settings = settings;
        this.positionalOffset = positionalOffset;
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
            // Capture outer's positional count BEFORE the pre-scan — this is the offset the inner
            // re-parse will use. Must happen before preScanSubqueryTokens advances it.
            this.capturedSubqueryOffset = this.positionalCount;
            this.fromSubQuery = inner.start.getInputStream()
                    .getText(new Interval(inner.start.getStartIndex(), inner.stop.getStopIndex()));
            // Pre-scan the subquery's token range to advance the outer's global counters as if
            // those placeholders were consumed — JDBC numbers positional params globally.
            preScanSubqueryTokens(inner.start.getTokenIndex(), inner.stop.getTokenIndex());
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
        if (ctx.LIMIT() != null && !ctx.limitValue().isEmpty()) {
            processLimitValue(ctx.limitValue(0), true);
            if (ctx.OFFSET() != null && ctx.limitValue().size() > 1) {
                processLimitValue(ctx.limitValue(1), false);
            }
        }
    }

    /**
     * Processes a single {@code limitValue} rule context (LIMIT or OFFSET). A literal NUMBER
     * is assigned directly to {@link #limit} / {@link #offset} (after non-negativity validation);
     * a parameter placeholder is captured as {@link Expression.ParameterRef} into
     * {@link #limitParam} / {@link #offsetParam} for later substitution.
     *
     * @param lvCtx   the {@code limitValue} rule context from the parse tree
     * @param isLimit {@code true} if this is LIMIT, {@code false} if OFFSET (affects which
     *                field is populated and which clause name appears in error messages)
     */
    private void processLimitValue(SQL4JsonParser.LimitValueContext lvCtx, boolean isLimit) {
        if (lvCtx.NUMBER() != null) {
            int v = Integer.parseInt(lvCtx.NUMBER().getText());
            if (v < 0) {
                throw new SQL4JsonExecutionException(
                        (isLimit ? "LIMIT" : "OFFSET") + " must be non-negative, got: " + v);
            }
            if (isLimit) {
                this.limit = v;
            } else {
                this.offset = v;
            }
        } else if (lvCtx.parameter() != null) {
            Expression.ParameterRef p = toParameterRef(lvCtx.parameter());
            if (isLimit) {
                this.limitParam = p;
            } else {
                this.offsetParam = p;
            }
        }
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
        if (insideSubquery || windowSpecDepth > 0) return;
        if (orderBy == null) orderBy = new ArrayList<>();
        String dir = ctx.ORDER_DIRECTION() != null
                ? ctx.ORDER_DIRECTION().getText().toUpperCase()
                : "ASC";
        orderBy.add(OrderByColumnDef.of(trackExpression(ctx.columnExpr()), dir));
    }

    @Override
    public void enterWindowSpec(SQL4JsonParser.WindowSpecContext ctx) {
        windowSpecDepth++;
    }

    @Override
    public void exitWindowSpec(SQL4JsonParser.WindowSpecContext ctx) {
        windowSpecDepth--;
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
        int maxParams = settings.limits().maxParameters();
        int totalParams = positionalCount + namedParameters.size();
        if (totalParams > maxParams) {
            throw new SQL4JsonParseException(
                    "Query parameter count " + totalParams
                            + " exceeds configured maximum (" + maxParams + ")",
                    0, 0);
        }
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
                joins != null ? List.copyOf(joins) : null,
                limitParam,
                offsetParam,
                positionalCount,
                Set.copyOf(namedParameters),
                capturedSubqueryOffset,
                List.copyOf(windowFunctionCalls));
    }

    // ── Conditions tree → CriteriaNode ────────────────────────────────────

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
            return new SingleConditionNode(cc, conditionHandlerRegistry.resolve(cc));
        }
        throw new SQL4JsonExecutionException(
                "Unsupported conditions context: " + ctx.getClass().getSimpleName());
    }

    private ConditionContext buildConditionContext(SQL4JsonParser.ConditionContext ctx) {
        return switch (ctx) {
            case SQL4JsonParser.ComparisonConditionContext compCtx -> buildComparisonContext(compCtx);
            case SQL4JsonParser.LikeConditionContext likeCtx ->
                    buildLikeContext(likeCtx.like().columnExpr(), likeCtx.like().rhsValue(),
                            ConditionContext.ConditionType.LIKE, "LIKE");
            case SQL4JsonParser.NotLikeConditionContext notLikeCtx ->
                    buildLikeContext(notLikeCtx.notLike().columnExpr(), notLikeCtx.notLike().rhsValue(),
                            ConditionContext.ConditionType.NOT_LIKE, "NOT LIKE");
            case SQL4JsonParser.IsNullConditionContext nullCtx -> buildNullCheckContext(nullCtx.isNull().columnExpr(),
                    ConditionContext.ConditionType.IS_NULL);
            case SQL4JsonParser.IsNotNullConditionContext notNullCtx ->
                    buildNullCheckContext(notNullCtx.isNotNull().columnExpr(),
                            ConditionContext.ConditionType.IS_NOT_NULL);
            case SQL4JsonParser.InConditionContext inCtx ->
                    buildInContext(inCtx.in().columnExpr(), inCtx.in().rhsValue(),
                            ConditionContext.ConditionType.IN);
            case SQL4JsonParser.NotInConditionContext notInCtx ->
                    buildInContext(notInCtx.notIn().columnExpr(), notInCtx.notIn().rhsValue(),
                            ConditionContext.ConditionType.NOT_IN);
            case SQL4JsonParser.BetweenConditionContext betweenCtx ->
                    buildBetweenContext(betweenCtx.between().columnExpr(),
                            betweenCtx.between().rhsValue(0), betweenCtx.between().rhsValue(1),
                            ConditionContext.ConditionType.BETWEEN);
            case SQL4JsonParser.NotBetweenConditionContext notBetweenCtx ->
                    buildBetweenContext(notBetweenCtx.notBetween().columnExpr(),
                            notBetweenCtx.notBetween().rhsValue(0),
                            notBetweenCtx.notBetween().rhsValue(1),
                            ConditionContext.ConditionType.NOT_BETWEEN);
            case SQL4JsonParser.ContainsConditionContext containsCtx -> buildContainsContext(containsCtx.contains());
            case SQL4JsonParser.ArrayContainsConditionContext acc ->
                    buildArrayPredicateContext(acc.arrayContains().columnExpr(),
                            acc.arrayContains().arrayRhs(),
                            ConditionContext.ConditionType.ARRAY_CONTAINS, "@>");
            case SQL4JsonParser.ArrayContainedByConditionContext acb ->
                    buildArrayPredicateContext(acb.arrayContainedBy().columnExpr(),
                            acb.arrayContainedBy().arrayRhs(),
                            ConditionContext.ConditionType.ARRAY_CONTAINED_BY, "<@");
            case SQL4JsonParser.ArrayOverlapConditionContext aol ->
                    buildArrayPredicateContext(aol.arrayOverlap().columnExpr(),
                            aol.arrayOverlap().arrayRhs(),
                            ConditionContext.ConditionType.ARRAY_OVERLAP, "&&");
            default -> throw new SQL4JsonExecutionException(
                    "Unsupported condition context: " + ctx.getClass().getSimpleName());
        };
    }

    /**
     * Builds a {@link ConditionContext} for a comparison ({@code =}, {@code !=}, {@code <},
     * {@code >}, {@code <=}, {@code >=}). Routes {@code ARRAY[...]} right-hand sides to
     * {@link #buildArrayEqualityContext} (for {@code =}/{@code !=}) or rejects them with a
     * parse-time error (for ordering operators).
     */
    private ConditionContext buildComparisonContext(SQL4JsonParser.ComparisonConditionContext compCtx) {
        var comp = compCtx.comparison();
        String op = comp.COMPARISON_OPERATOR().getText();
        var rhsValue = comp.rhsValue();
        if (rhsValue instanceof SQL4JsonParser.RhsArrayLiteralContext arrLit) {
            return buildArrayEqualityContext(comp, arrLit, op);
        }
        Expression lhs = buildExpression(comp.columnExpr());
        Expression rhs = buildRhsExpression(rhsValue);
        SqlValue testVal = (rhs instanceof Expression.LiteralVal(var val)) ? val : null;
        return new ConditionContext(ConditionContext.ConditionType.COMPARISON,
                lhs, op, testVal, rhs, null, null, null,
                null, null, null);
    }

    private ConditionContext buildArrayEqualityContext(SQL4JsonParser.ComparisonContext comp,
                                                       SQL4JsonParser.RhsArrayLiteralContext arrLit,
                                                       String op) {
        if (!"=".equals(op) && !"!=".equals(op)) {
            var opTok = comp.COMPARISON_OPERATOR().getSymbol();
            throw new SQL4JsonParseException(
                    "comparison operator '" + op + "' does not support array right-hand side; "
                            + "use =, !=, @>, <@, &&, or CONTAINS instead",
                    opTok.getLine(), opTok.getCharPositionInLine());
        }
        Expression lhs = buildExpression(comp.columnExpr());
        List<Expression> elements = withParameterKind(ParameterPositionKind.ARRAY_ELEMENT, () ->
                arrLit.arrayLiteral().rhsValue().stream()
                        .map(this::getRhsExpression)
                        .toList());
        ConditionContext.ConditionType type = "=".equals(op)
                ? ConditionContext.ConditionType.ARRAY_EQUALS
                : ConditionContext.ConditionType.ARRAY_NOT_EQUALS;
        return new ConditionContext(type, lhs, op, null, null,
                null, null, null, elements, null, null);
    }

    private ConditionContext buildLikeContext(SQL4JsonParser.ColumnExprContext colExpr,
                                              SQL4JsonParser.RhsValueContext rhsValue,
                                              ConditionContext.ConditionType type,
                                              String operator) {
        Expression lhs = buildExpression(colExpr);
        Expression rhs = buildRhsExpression(rhsValue);
        SqlValue testVal = (rhs instanceof Expression.LiteralVal(var val)) ? val : null;
        enforceLikeWildcardLimit(testVal);
        return new ConditionContext(type, lhs, operator, testVal, rhs, null, null, null,
                null, null, null);
    }

    private ConditionContext buildNullCheckContext(SQL4JsonParser.ColumnExprContext colExpr,
                                                   ConditionContext.ConditionType type) {
        Expression lhs = buildExpression(colExpr);
        return new ConditionContext(type, lhs, null, null, null, null, null, null,
                null, null, null);
    }

    /**
     * Builds an IN / NOT IN {@link ConditionContext}. When every rhs element is a literal, the
     * classic {@code valueList} field is populated. When at least one element is a parameter
     * placeholder, {@code valueList} is left {@code null} and the parallel {@code valueExpressions}
     * list is populated for later substitution.
     *
     * @param colExpr   the column expression parse context for the LHS
     * @param rhsValues the list of rhs value parse contexts (element candidates)
     * @param type      the condition type — either {@link ConditionContext.ConditionType#IN}
     *                  or {@link ConditionContext.ConditionType#NOT_IN}
     * @return the assembled {@link ConditionContext}
     */
    private ConditionContext buildInContext(SQL4JsonParser.ColumnExprContext colExpr,
                                            List<SQL4JsonParser.RhsValueContext> rhsValues,
                                            ConditionContext.ConditionType type) {
        Expression lhs = buildExpression(colExpr);
        List<Expression> exprList = withParameterKind(ParameterPositionKind.IN_LIST, () ->
                rhsValues.stream().map(this::getRhsExpression).toList());
        enforceInListSizeLimit(exprList.size());
        // Route to valueExpressions whenever any element is not a plain literal —
        // this covers both ParameterRef (deferred substitution) and NowRef (per-row evaluation).
        boolean anyNonLiteral = exprList.stream()
                .anyMatch(e -> !(e instanceof Expression.LiteralVal));
        List<SqlValue> literalValues = anyNonLiteral
                ? null
                : exprList.stream()
                  .map(e -> ((Expression.LiteralVal) e).value())
                  .toList();
        List<Expression> exprs = anyNonLiteral ? List.copyOf(exprList) : null;
        return new ConditionContext(type, lhs, null, null, null, literalValues, null, null,
                exprs, null, null);
    }

    private ConditionContext buildBetweenContext(SQL4JsonParser.ColumnExprContext colExpr,
                                                 SQL4JsonParser.RhsValueContext lowerRhs,
                                                 SQL4JsonParser.RhsValueContext upperRhs,
                                                 ConditionContext.ConditionType type) {
        Expression lhs = buildExpression(colExpr);
        Expression lowerExpr = getRhsExpression(lowerRhs);
        Expression upperExpr = getRhsExpression(upperRhs);
        SqlValue lower = (lowerExpr instanceof Expression.LiteralVal(var v)) ? v : null;
        SqlValue upper = (upperExpr instanceof Expression.LiteralVal(var v)) ? v : null;
        // Route any non-literal bound (ParameterRef or NowRef) through the expression fields
        // so that BetweenConditionHandler can evaluate it per-row.
        Expression lowerBoundExpr = !(lowerExpr instanceof Expression.LiteralVal) ? lowerExpr : null;
        Expression upperBoundExpr = !(upperExpr instanceof Expression.LiteralVal) ? upperExpr : null;
        return new ConditionContext(type, lhs, null, null, null, null, lower, upper,
                null, lowerBoundExpr, upperBoundExpr);
    }

    /**
     * Builds a {@link ConditionContext} for a {@code CONTAINS} (scalar-membership)
     * condition. The RHS is a single scalar {@code rhsValue}; literal RHS goes into
     * {@code testValue}, non-literal RHS (parameter ref or {@link Expression.NowRef})
     * goes into {@code rhsExpression}.
     */
    private ConditionContext buildContainsContext(SQL4JsonParser.ContainsContext c) {
        Expression lhs = buildExpression(c.columnExpr());
        Expression rhs = buildRhsExpression(c.rhsValue());
        SqlValue testValue = (rhs instanceof Expression.LiteralVal(var v)) ? v : null;
        Expression rhsExpr = (rhs instanceof Expression.LiteralVal) ? null : rhs;
        return new ConditionContext(
                ConditionContext.ConditionType.CONTAINS,
                lhs, "CONTAINS", testValue, rhsExpr,
                null, null, null, null, null, null);
    }

    /**
     * Builds a {@link ConditionContext} for the array-set operators ({@code @>}, {@code <@},
     * {@code &&}). Dispatches on the {@code arrayRhs} alternative label:
     * <ul>
     *   <li>{@code ArrayRhsLiteralContext} — populates {@code valueExpressions} with the
     *       evaluated element expressions (one entry per array literal element).</li>
     *   <li>{@code ArrayRhsColumnRefContext} — populates {@code rhsExpression} with the
     *       column-ref expression so the handler resolves the array per-row.</li>
     *   <li>{@code ArrayRhsParameterContext} — populates {@code rhsExpression} with the
     *       {@link Expression.ParameterRef}; {@code ParameterSubstitutor} replaces it
     *       with literal {@code valueExpressions} at execute time (T12).</li>
     * </ul>
     *
     * @param colExpr  the LHS column expression parse context
     * @param rhsCtx   the RHS array context (one of three alternative labels)
     * @param type     the condition type (ARRAY_CONTAINS, ARRAY_CONTAINED_BY, ARRAY_OVERLAP)
     * @param operator the textual operator symbol used in error messages and tracing
     * @return the assembled {@link ConditionContext}
     */
    private ConditionContext buildArrayPredicateContext(SQL4JsonParser.ColumnExprContext colExpr,
                                                        SQL4JsonParser.ArrayRhsContext rhsCtx,
                                                        ConditionContext.ConditionType type,
                                                        String operator) {
        Expression lhs = buildExpression(colExpr);

        if (rhsCtx instanceof SQL4JsonParser.ArrayRhsLiteralContext lit) {
            List<Expression> elements = withParameterKind(ParameterPositionKind.ARRAY_ELEMENT, () ->
                    lit.arrayLiteral().rhsValue().stream()
                            .map(this::getRhsExpression)
                            .toList());
            return new ConditionContext(type, lhs, operator, null, null,
                    null, null, null, elements, null, null);
        }
        if (rhsCtx instanceof SQL4JsonParser.ArrayRhsColumnRefContext colRef) {
            Expression rhs = new Expression.ColumnRef(colRef.jsonColumn().getText());
            trackReferencedFields(rhs);
            return new ConditionContext(type, lhs, operator, null, rhs,
                    null, null, null, null, null, null);
        }
        if (rhsCtx instanceof SQL4JsonParser.ArrayRhsParameterContext paramCtx) {
            Expression rhs = withParameterKind(ParameterPositionKind.BARE_ARRAY_RHS, () ->
                    toParameterRef(paramCtx.parameter()));
            return new ConditionContext(type, lhs, operator, null, rhs,
                    null, null, null, null, null, null);
        }
        throw new IllegalStateException("Unexpected arrayRhs alternative: " + rhsCtx.getClass().getSimpleName());
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
            return resolveFunctionCallExpression(name, args, scalarCall.identifierOrKeyword().getStart());
        }
        if (ctx instanceof SQL4JsonParser.CastExprColumnContext castCtx) {
            String typeName = castCtx.castExpr().castType().getText().toUpperCase();
            Expression inner = buildExpression(castCtx.castExpr().columnExpr());
            return new Expression.ScalarFnCall("cast", List.of(inner, new Expression.LiteralVal(new SqlString(typeName))));
        }
        if (ctx instanceof SQL4JsonParser.LiteralColumnExprContext litCtx) {
            return toExpression(litCtx.value());
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
        Expression.WindowFnCall built = new Expression.WindowFnCall(name, args, spec);
        windowFunctionCalls.add(built);
        return built;
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
            return toExpression(vfa.value());
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
            return toExpression(pv.value());
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
        // RhsFunctionCallContext — lazy-dispatch zero-arg value functions, otherwise
        // eagerly evaluate since no column refs on RHS scalar functions
        var fnCtx = (SQL4JsonParser.RhsFunctionCallContext) ctx;
        var fn = (SQL4JsonParser.ScalarFunctionCallContext) fnCtx.functionCall();
        Expression lazyValueFn = tryDispatchRhsValueFunction(fn);
        if (lazyValueFn != null) return lazyValueFn;
        SqlValue evaluated = evaluateRhsScalarFunction(fn);
        return new Expression.LiteralVal(evaluated);
    }

    /**
     * Non-eagerly produces an {@link Expression} from an rhsValue context — supports parameter
     * placeholders and dynamic value functions. Used by IN / NOT IN and BETWEEN where list
     * elements and bounds must defer evaluation when they contain {@code ?} / {@code :name}
     * placeholders or zero-arg value functions such as {@code NOW()}.
     *
     * <p>The result is one of three types:</p>
     * <ul>
     *   <li>{@link Expression.LiteralVal} — for plain literal values</li>
     *   <li>{@link Expression.ParameterRef} — for {@code ?} or {@code :name} placeholders</li>
     *   <li>{@link Expression.NowRef} — for zero-arg value functions (e.g. {@code NOW()})
     *       dispatched lazily so they are evaluated per-row rather than at parse time</li>
     * </ul>
     *
     * <p>CAST expressions and non-value RHS scalar function calls are still eagerly evaluated
     * to a {@link Expression.LiteralVal} since they cannot contain dynamic elements.</p>
     *
     * @param ctx the rhsValue rule context from the parse tree
     * @return the corresponding {@link Expression} (literal, parameter ref, or NowRef)
     */
    private Expression getRhsExpression(SQL4JsonParser.RhsValueContext ctx) {
        if (ctx instanceof SQL4JsonParser.RhsPlainValueContext pv) {
            return toExpression(pv.value());
        }
        if (ctx instanceof SQL4JsonParser.RhsColumnRefContext) {
            throw new SQL4JsonExecutionException(
                    "Column references not supported in IN/BETWEEN lists: " + ctx.getText());
        }
        if (ctx instanceof SQL4JsonParser.RhsCastExprContext castCtx) {
            return new Expression.LiteralVal(evaluateRhsCast(castCtx.castExpr()));
        }
        var fnCtx = (SQL4JsonParser.RhsFunctionCallContext) ctx;
        var fn = (SQL4JsonParser.ScalarFunctionCallContext) fnCtx.functionCall();
        Expression lazyValueFn = tryDispatchRhsValueFunction(fn);
        if (lazyValueFn != null) return lazyValueFn;
        return new Expression.LiteralVal(evaluateRhsScalarFunction(fn));
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
        return resolveRhsFunctionCall(name, evaluatedArgs, fn.identifierOrKeyword().getStart(), () -> {
            SqlValue primary = evaluatedArgs.isEmpty() ? null : evaluatedArgs.getFirst();
            List<SqlValue> extraArgs = evaluatedArgs.size() <= 1
                    ? List.of()
                    : List.copyOf(evaluatedArgs.subList(1, evaluatedArgs.size()));
            return functionRegistry.getScalar(name)
                    .map(f -> f.apply().apply(primary, extraArgs))
                    .orElseThrow(() -> new SQL4JsonExecutionException(
                            "Unknown function in RHS: " + name));
        });
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

    /**
     * Converts an ANTLR {@code parameter} context to a {@link Expression.ParameterRef}.
     * Enforces the "no mixing {@code ?} and {@code :name}" rule and advances the global
     * positional counter or named-parameter set as appropriate.
     *
     * @param ctx the parameter rule context from the parse tree
     * @return a {@link Expression.ParameterRef} for this placeholder
     */
    private Expression.ParameterRef toParameterRef(SQL4JsonParser.ParameterContext ctx) {
        ParameterPositionKind kind = parameterKindStack.peek();
        if (ctx.POSITIONAL_PARAM() != null) {
            if (sawNamed) {
                throw new SQL4JsonParseException(
                        "Cannot mix positional (?) and named (:name) parameters in the same query",
                        ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
            }
            sawPositional = true;
            int idx = positionalOffset + positionalCount;
            positionalCount++;
            return new Expression.ParameterRef(null, idx, kind);
        }
        // NAMED_PARAM: text is ":foo" — strip the leading colon
        String text = ctx.NAMED_PARAM().getText();
        String name = text.startsWith(":") ? text.substring(1) : text;
        if (sawPositional) {
            throw new SQL4JsonParseException(
                    "Cannot mix positional (?) and named (:name) parameters in the same query",
                    ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
        }
        sawNamed = true;
        namedParameters.add(name);
        return new Expression.ParameterRef(name, -1, kind);
    }

    /**
     * Runs {@code body} with the parameter-position kind stack pushed to {@code kind},
     * so that any {@link Expression.ParameterRef} constructed during the call is tagged
     * with that kind. The previous kind is restored on return (also if {@code body}
     * throws).
     *
     * @param kind the kind to apply to any {@link Expression.ParameterRef} constructed
     *             during {@code body}
     * @param body the body to run
     * @param <T>  return type of {@code body}
     * @return the result of {@code body}
     */
    private <T> T withParameterKind(ParameterPositionKind kind, java.util.function.Supplier<T> body) {
        parameterKindStack.push(kind);
        try {
            return body.get();
        } finally {
            parameterKindStack.pop();
        }
    }

    /**
     * Pre-scans the given token range for placeholder tokens. Called when the outer listener
     * enters a FROM subquery — the subquery's inner listener won't walk during the outer
     * parse (because {@code insideSubquery} suppresses its callbacks), so we need to count
     * the placeholders statically and advance the outer's global counters accordingly.
     *
     * <p>JDBC semantics: positional placeholders are numbered globally across subquery boundaries;
     * named placeholders share a global name space.
     *
     * @param startTokenIndex inclusive start of the subquery's token range
     * @param stopTokenIndex  inclusive end of the subquery's token range
     */
    private void preScanSubqueryTokens(int startTokenIndex, int stopTokenIndex) {
        var tokens = tokenStream.getTokens();
        int stop = Math.min(stopTokenIndex, tokens.size() - 1);
        for (int i = startTokenIndex; i <= stop; i++) {
            var t = tokens.get(i);
            if (t.getChannel() != org.antlr.v4.runtime.Token.DEFAULT_CHANNEL) continue;
            int tType = t.getType();
            if (tType == io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.POSITIONAL_PARAM) {
                if (sawNamed) {
                    throw new SQL4JsonParseException(
                            "Cannot mix positional (?) and named (:name) parameters in the same query",
                            t.getLine(), t.getCharPositionInLine());
                }
                sawPositional = true;
                positionalCount++;
            } else if (tType == io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.NAMED_PARAM) {
                if (sawPositional) {
                    throw new SQL4JsonParseException(
                            "Cannot mix positional (?) and named (:name) parameters in the same query",
                            t.getLine(), t.getCharPositionInLine());
                }
                sawNamed = true;
                String text = t.getText();
                String name = text.startsWith(":") ? text.substring(1) : text;
                namedParameters.add(name);
            }
        }
    }

    /**
     * Dispatches a scalar-shaped function call to either {@link Expression.NowRef}
     * (when the name is a registered zero-arg value function) or the generic
     * {@link Expression.ScalarFnCall}. Used in column-expression position
     * (SELECT / GROUP BY / ORDER BY / HAVING / nested function args).
     *
     * <p>When a value function is dispatched, {@code containsNonDeterministic} is set
     * — value functions cannot participate in the result cache.
     *
     * @param name    the lower-cased function name
     * @param args    parsed argument expressions
     * @param nameTok the token that lexed the function name, used for error positioning
     */
    private Expression resolveFunctionCallExpression(String name, List<Expression> args, Token nameTok) {
        rejectIfWindowOnly(name, nameTok);
        if (args.isEmpty() && functionRegistry.getValue(name).isPresent()) {
            containsNonDeterministic = true;
            return new Expression.NowRef();
        }
        return new Expression.ScalarFnCall(name, args);
    }

    /**
     * Throws a parse-time error if {@code name} is a window-only function called outside
     * a {@code windowFunctionCall} parse path (i.e. without {@code OVER (...)}). The grammar
     * allows these tokens as plain {@code identifierOrKeyword} entries inside {@code functionCall},
     * so without this guard the call falls through to a misleading evaluator-time error.
     */
    private static void rejectIfWindowOnly(String name, Token tok) {
        if (WINDOW_ONLY_FUNCTIONS.contains(name)) {
            String upper = name.toUpperCase();
            throw new SQL4JsonParseException(
                    upper + " must be used with OVER (...): e.g. "
                            + upper + "() OVER (PARTITION BY col ORDER BY col)",
                    tok != null ? tok.getLine() : 0,
                    tok != null ? tok.getCharPositionInLine() : 0);
        }
    }

    /**
     * Pre-check for RHS positions that should stay LAZY (per-row evaluation) — namely
     * comparison/LIKE RHS and IN/BETWEEN list elements. In v1.1.x these routed NOW()
     * through RhsPlainValue → toExpression → NowRef, bypassing eager evaluation.
     * Post-refactor NOW() lexes as a regular function call, so we must intercept here
     * to preserve that semantic.
     *
     * @return {@link Expression.NowRef} if the call is a zero-arg value function, else {@code null}
     */
    private Expression tryDispatchRhsValueFunction(SQL4JsonParser.ScalarFunctionCallContext fn) {
        String name = fn.identifierOrKeyword().getText().toLowerCase();
        rejectIfWindowOnly(name, fn.identifierOrKeyword().getStart());
        boolean zeroArgs = fn.functionArgs() == null || fn.functionArgs().functionArg().isEmpty();
        if (zeroArgs && functionRegistry.getValue(name).isPresent()) {
            containsNonDeterministic = true;
            return new Expression.NowRef();
        }
        return null;
    }

    /**
     * Eager RHS variant of {@link #resolveFunctionCallExpression(String, List, Token)}:
     * when the name resolves to a value function and args are empty, evaluates the
     * supplier immediately and returns its {@code SqlValue}; otherwise falls through
     * to the provided scalar fallback. Used in eager positions ({@code CAST(NOW() AS ...)}
     * inner, nested function args on RHS).
     */
    private SqlValue resolveRhsFunctionCall(
            String name,
            List<SqlValue> args,
            Token nameTok,
            Supplier<SqlValue> scalarFallback) {
        rejectIfWindowOnly(name, nameTok);
        if (args.isEmpty()) {
            var vf = functionRegistry.getValue(name);
            if (vf.isPresent()) {
                containsNonDeterministic = true;
                return vf.get().apply().get();
            }
        }
        return scalarFallback.get();
    }

    /**
     * Converts a {@code value} context to an {@link Expression} — {@link Expression.LiteralVal}
     * for literals, or {@link Expression.ParameterRef} for placeholders. All listener paths
     * that consume values in positions where a parameter placeholder is permitted route
     * through this helper.
     *
     * @param ctx the value rule context from the parse tree
     * @return the corresponding {@link Expression}
     */
    private Expression toExpression(SQL4JsonParser.ValueContext ctx) {
        if (ctx.parameter() != null) {
            return toParameterRef(ctx.parameter());
        }
        return new Expression.LiteralVal(toSqlValue(ctx));
    }

    private SqlValue toSqlValue(SQL4JsonParser.ValueContext ctx) {
        if (ctx.parameter() != null) {
            // All listener callsites that accept placeholders route through toExpression(...).
            // Reaching this guard means a value context was consumed in a non-bindable position —
            // the throw surfaces the exact token so the offending code path is easy to locate.
            throw new SQL4JsonParseException(
                    "Parameter placeholder used in a non-bindable position: " + ctx.getText(),
                    ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine());
        }
        if (ctx.STRING() != null) {
            String raw = ctx.STRING().getText();
            return new SqlString(raw.substring(1, raw.length() - 1).replace("''", "'"));
        }
        if (ctx.NUMBER() != null) {
            // Mirror FunctionRegistry::numOf — whole-number doubles within long range
            // are stored as SqlLong so they round-trip as 42 rather than 42.0.
            double d = Double.parseDouble(ctx.NUMBER().getText());
            if (d == Math.floor(d) && !Double.isInfinite(d)
                    && d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
                return SqlNumber.of((long) d);
            }
            return SqlNumber.of(d);
        }
        if (ctx.BOOLEAN() != null) {
            return SqlBoolean.of(Boolean.parseBoolean(ctx.BOOLEAN().getText().toLowerCase()));
        }
        if (ctx.NULL() != null) {
            return SqlNull.INSTANCE;
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
