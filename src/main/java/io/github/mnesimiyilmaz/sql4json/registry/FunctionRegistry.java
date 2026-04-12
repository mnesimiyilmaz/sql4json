package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.sorting.SqlValueComparator;
import io.github.mnesimiyilmaz.sql4json.types.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Registry of all built-in SQL functions: scalar, value-producing, and aggregate.
 * A frozen singleton is shared JVM-wide via {@link #getDefault()}.
 */
public final class FunctionRegistry {

    private Map<String, ScalarFunction>    scalarFunctions    = new HashMap<>();
    private Map<String, ValueFunction>     valueFunctions     = new HashMap<>();
    private Map<String, AggregateFunction> aggregateFunctions = new HashMap<>();

    /**
     * Creates a new empty function registry.
     */
    public FunctionRegistry() {
    }


    private static final FunctionRegistry DEFAULT = createDefault();

    /**
     * Returns the JVM-wide shared default registry with all built-in functions.
     *
     * @return the default function registry singleton
     */
    public static FunctionRegistry getDefault() {
        return DEFAULT;
    }

    /**
     * Makes all function maps unmodifiable. Called after all functions are registered.
     */
    public void freeze() {
        scalarFunctions = Collections.unmodifiableMap(scalarFunctions);
        valueFunctions = Collections.unmodifiableMap(valueFunctions);
        aggregateFunctions = Collections.unmodifiableMap(aggregateFunctions);
    }

    /**
     * Registers a scalar function. Keys are normalized to lowercase for case-insensitive lookup.
     *
     * @param fn the scalar function to register
     */
    public void registerScalar(ScalarFunction fn) {
        scalarFunctions.put(fn.name().toLowerCase(), fn);
    }

    /**
     * Registers a value-producing function.
     *
     * @param fn the value-producing function to register
     */
    public void registerValue(ValueFunction fn) {
        valueFunctions.put(fn.name().toLowerCase(), fn);
    }

    /**
     * Registers an aggregate function.
     *
     * @param fn the aggregate function to register
     */
    public void registerAggregate(AggregateFunction fn) {
        aggregateFunctions.put(fn.name().toLowerCase(), fn);
    }

    /**
     * Looks up a scalar function by name (case-insensitive).
     *
     * @param name the function name to look up
     * @return an {@link Optional} containing the scalar function, or empty if not found
     */
    public Optional<ScalarFunction> getScalar(String name) {
        return Optional.ofNullable(scalarFunctions.get(name.toLowerCase()));
    }

    /**
     * Looks up a value-producing function by name (case-insensitive).
     *
     * @param name the function name to look up
     * @return an {@link Optional} containing the value function, or empty if not found
     */
    public Optional<ValueFunction> getValue(String name) {
        return Optional.ofNullable(valueFunctions.get(name.toLowerCase()));
    }

    /**
     * Looks up an aggregate function by name (case-insensitive).
     *
     * @param name the function name to look up
     * @return an {@link Optional} containing the aggregate function, or empty if not found
     */
    public Optional<AggregateFunction> getAggregate(String name) {
        return Optional.ofNullable(aggregateFunctions.get(name.toLowerCase()));
    }

    /**
     * Returns a SqlNumber, normalizing whole-number doubles to int (so that
     * SqlNumber.of(3) equals numOf(3.0) via record equality).
     */
    private static SqlNumber numOf(double d) {
        if (d == Math.floor(d) && !Double.isInfinite(d) && d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE) {
            return SqlNumber.of((int) d);
        }
        return SqlNumber.of(d);
    }

    /**
     * Returns a SqlNumber from a long, using int when in range so it matches
     * SqlNumber.of(int) cache equality.
     */
    private static SqlNumber numOf(long l) {
        if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
            return SqlNumber.of((int) l);
        }
        return SqlNumber.of(l);
    }

    /**
     * Creates and freezes a new registry populated with all built-in functions.
     *
     * @return a new frozen registry with all built-in functions registered
     */
    public static FunctionRegistry createDefault() {
        var r = new FunctionRegistry();
        registerStringFunctions(r);
        registerMathFunctions(r);
        registerDateFunctions(r);
        registerConversionFunctions(r);
        registerValueFunctions(r);
        registerAggregateFunctions(r);
        r.freeze();
        return r;
    }

    // ── String functions ─────────────────────────────────────────────────

    private static void registerStringFunctions(FunctionRegistry r) {
        registerCoreStringFunctions(r);
        registerStringManipulationFunctions(r);
        registerStringPaddingFunctions(r);
    }

    private static void registerCoreStringFunctions(FunctionRegistry r) {
        r.registerScalar(new ScalarFunction("lower", FunctionRegistry::lowerFn));
        r.registerScalar(new ScalarFunction("upper", FunctionRegistry::upperFn));
        r.registerScalar(new ScalarFunction("coalesce", (val, args) -> val.isNull()
                ? args.stream().filter(a -> !a.isNull()).findFirst().orElse(SqlNull.INSTANCE)
                : val));
        r.registerScalar(new ScalarFunction("to_date", FunctionRegistry::toDateFn));
    }

    private static void registerStringManipulationFunctions(FunctionRegistry r) {
        r.registerScalar(new ScalarFunction("concat", FunctionRegistry::concatFn));
        r.registerScalar(new ScalarFunction("substring", FunctionRegistry::substringFn));
        r.registerScalar(new ScalarFunction("trim", FunctionRegistry::trimFn));
        r.registerScalar(new ScalarFunction("length", FunctionRegistry::lengthFn));
        r.registerScalar(new ScalarFunction("replace", FunctionRegistry::replaceFn));
    }

    private static void registerStringPaddingFunctions(FunctionRegistry r) {
        r.registerScalar(new ScalarFunction("left", FunctionRegistry::leftFn));
        r.registerScalar(new ScalarFunction("right", FunctionRegistry::rightFn));
        r.registerScalar(new ScalarFunction("lpad", FunctionRegistry::lpadFn));
        r.registerScalar(new ScalarFunction("rpad", FunctionRegistry::rpadFn));
        r.registerScalar(new ScalarFunction("reverse", FunctionRegistry::reverseFn));
        r.registerScalar(new ScalarFunction("position", FunctionRegistry::positionFn));
    }

    // ── Math functions ────────────────────────────────────────────────────

    private static void registerMathFunctions(FunctionRegistry r) {
        registerBasicMathFunctions(r);
        registerAdvancedMathFunctions(r);
    }

    private static void registerBasicMathFunctions(FunctionRegistry r) {
        r.registerScalar(new ScalarFunction("abs", FunctionRegistry::absFn));
        r.registerScalar(new ScalarFunction("round", FunctionRegistry::roundFn));
        r.registerScalar(new ScalarFunction("ceil", FunctionRegistry::ceilFn));
        r.registerScalar(new ScalarFunction("floor", FunctionRegistry::floorFn));
    }

    private static void registerAdvancedMathFunctions(FunctionRegistry r) {
        r.registerScalar(new ScalarFunction("mod", FunctionRegistry::modFn));
        r.registerScalar(new ScalarFunction("power", FunctionRegistry::powerFn));
        r.registerScalar(new ScalarFunction("sqrt", FunctionRegistry::sqrtFn));
        r.registerScalar(new ScalarFunction("sign", FunctionRegistry::signFn));
    }

    // ── Date functions ────────────────────────────────────────────────────

    private static void registerDateFunctions(FunctionRegistry r) {
        r.registerScalar(new ScalarFunction("year", FunctionRegistry::yearFn));
        r.registerScalar(new ScalarFunction("month", FunctionRegistry::monthFn));
        r.registerScalar(new ScalarFunction("day", FunctionRegistry::dayFn));
        r.registerScalar(new ScalarFunction("hour", FunctionRegistry::hourFn));
        r.registerScalar(new ScalarFunction("minute", FunctionRegistry::minuteFn));
        r.registerScalar(new ScalarFunction("second", FunctionRegistry::secondFn));
        r.registerScalar(new ScalarFunction("date_add", FunctionRegistry::dateAddFn));
        r.registerScalar(new ScalarFunction("date_diff", FunctionRegistry::dateDiffFn));
    }

    private static SqlValue yearFn(SqlValue val, List<SqlValue> args) {
        var d = DateCoercion.toLocalDate(val);
        return d != null ? SqlNumber.of(d.getYear()) : SqlNull.INSTANCE;
    }

    private static SqlValue monthFn(SqlValue val, List<SqlValue> args) {
        var d = DateCoercion.toLocalDate(val);
        return d != null ? SqlNumber.of(d.getMonthValue()) : SqlNull.INSTANCE;
    }

    private static SqlValue dayFn(SqlValue val, List<SqlValue> args) {
        var d = DateCoercion.toLocalDate(val);
        return d != null ? SqlNumber.of(d.getDayOfMonth()) : SqlNull.INSTANCE;
    }

    private static SqlValue hourFn(SqlValue val, List<SqlValue> args) {
        var dt = DateCoercion.toLocalDateTime(val);
        return dt != null ? SqlNumber.of(dt.getHour()) : SqlNull.INSTANCE;
    }

    private static SqlValue minuteFn(SqlValue val, List<SqlValue> args) {
        var dt = DateCoercion.toLocalDateTime(val);
        return dt != null ? SqlNumber.of(dt.getMinute()) : SqlNull.INSTANCE;
    }

    private static SqlValue secondFn(SqlValue val, List<SqlValue> args) {
        var dt = DateCoercion.toLocalDateTime(val);
        return dt != null ? SqlNumber.of(dt.getSecond()) : SqlNull.INSTANCE;
    }

    private static SqlValue dateAddFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        long amount = ((SqlNumber) args.get(0)).longValue();
        String unit = ((SqlString) args.get(1)).value().toUpperCase();
        ChronoUnit chronoUnit = parseChronoUnit(unit);
        // SqlDate preserves its type for date-only units; time units promote to SqlDateTime
        if (val instanceof SqlDate(var dateValue)) {
            if (chronoUnit == ChronoUnit.HOURS
                    || chronoUnit == ChronoUnit.MINUTES
                    || chronoUnit == ChronoUnit.SECONDS) {
                return new SqlDateTime(dateValue.atStartOfDay().plus(amount, chronoUnit));
            }
            return new SqlDate(dateValue.plus(amount, chronoUnit));
        }
        var ldt = DateCoercion.toLocalDateTime(val);
        if (ldt == null) return SqlNull.INSTANCE;
        return new SqlDateTime(ldt.plus(amount, chronoUnit));
    }

    private static SqlValue dateDiffFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        SqlValue date2 = args.get(0);
        String unit = ((SqlString) args.get(1)).value().toUpperCase();
        ChronoUnit chronoUnit = parseChronoUnit(unit);
        var ldt1 = DateCoercion.toLocalDateTime(val);
        var ldt2 = DateCoercion.toLocalDateTime(date2);
        if (ldt1 == null || ldt2 == null) return SqlNull.INSTANCE;
        return numOf(chronoUnit.between(ldt2, ldt1));
    }

    private static ChronoUnit parseChronoUnit(String unit) {
        try {
            return ChronoUnit.valueOf(unit + "S");
        } catch (IllegalArgumentException e) {
            throw new SQL4JsonExecutionException("Unsupported date unit: " + unit, e);
        }
    }

    // ── Conversion functions (NULLIF, CAST, etc.) ────────────────────────

    private static void registerConversionFunctions(FunctionRegistry r) {
        r.registerScalar(new ScalarFunction("nullif", (val, args) -> {
            SqlValue compareVal = args.getFirst();
            return SqlValueComparator.compare(val, compareVal) == 0 ? SqlNull.INSTANCE : val;
        }));

        r.registerScalar(new ScalarFunction("cast", (val, args) -> {
            if (val.isNull()) return SqlNull.INSTANCE;
            String typeName = ((SqlString) args.getFirst()).value().toUpperCase();
            return castValue(val, typeName);
        }));
    }

    private static SqlValue castValue(SqlValue val, String typeName) {
        return switch (typeName) {
            case "STRING" -> switch (val) {
                case SqlString s -> s;
                case SqlNumber n -> new SqlString(n.rawValue().toString());
                case SqlBoolean(var value) -> new SqlString(Boolean.toString(value));
                case SqlDate(var value) -> new SqlString(value.toString());
                case SqlDateTime(var value) -> new SqlString(value.toString());
                case SqlNull ignored -> SqlNull.INSTANCE;
            };
            case "NUMBER", "DECIMAL" -> switch (val) {
                case SqlNumber n -> n;
                case SqlString(var value) -> {
                    try {
                        yield SqlNumber.of(Double.parseDouble(value));
                    } catch (NumberFormatException e) {
                        throw new SQL4JsonExecutionException(
                                "Cannot cast '" + value + "' to NUMBER", e);
                    }
                }
                case SqlBoolean(var value) -> SqlNumber.of(value ? 1.0 : 0.0);
                case SqlDate(var value) -> SqlNumber.of((double) value.toEpochDay());
                case SqlDateTime(var value) -> SqlNumber.of(
                        (double) value.toEpochSecond(ZoneOffset.UTC));
                case SqlNull ignored -> SqlNull.INSTANCE;
            };
            case "INTEGER" -> switch (val) {
                case SqlNumber n -> SqlNumber.of((double) (long) n.doubleValue());
                case SqlString(var value) -> {
                    try {
                        long parsed = parseStringToLong(value);
                        yield SqlNumber.of((double) parsed);
                    } catch (NumberFormatException e) {
                        throw new SQL4JsonExecutionException(
                                "Cannot cast '" + value + "' to INTEGER", e);
                    }
                }
                case SqlBoolean(var value) -> SqlNumber.of(value ? 1.0 : 0.0);
                default -> throw new SQL4JsonExecutionException(
                        "Cannot cast " + val.getClass().getSimpleName() + " to INTEGER");
            };
            case "BOOLEAN" -> switch (val) {
                case SqlBoolean b -> b;
                case SqlString(var value) -> SqlBoolean.of(Boolean.parseBoolean(value));
                case SqlNumber n -> SqlBoolean.of(n.doubleValue() != 0);
                default -> throw new SQL4JsonExecutionException(
                        "Cannot cast " + val.getClass().getSimpleName() + " to BOOLEAN");
            };
            case "DATE" -> switch (val) {
                case SqlDate d -> d;
                case SqlDateTime(var value) -> new SqlDate(value.toLocalDate());
                case SqlString(var value) -> {
                    try {
                        yield new SqlDate(LocalDate.parse(value));
                    } catch (DateTimeParseException e) {
                        throw new SQL4JsonExecutionException(
                                "Cannot cast '" + value + "' to DATE", e);
                    }
                }
                default -> throw new SQL4JsonExecutionException(
                        "Cannot cast " + val.getClass().getSimpleName() + " to DATE");
            };
            case "DATETIME" -> switch (val) {
                case SqlDateTime dt -> dt;
                case SqlDate(var value) -> new SqlDateTime(value.atStartOfDay());
                case SqlString(var value) -> {
                    try {
                        yield new SqlDateTime(LocalDateTime.parse(value));
                    } catch (DateTimeParseException e) {
                        throw new SQL4JsonExecutionException(
                                "Cannot cast '" + value + "' to DATETIME", e);
                    }
                }
                default -> throw new SQL4JsonExecutionException(
                        "Cannot cast " + val.getClass().getSimpleName() + " to DATETIME");
            };
            default -> throw new SQL4JsonExecutionException("Unknown CAST target type: " + typeName);
        };
    }

    // ── Value functions ──────────────────────────────────────────────────

    private static void registerValueFunctions(FunctionRegistry r) {
        r.registerValue(new ValueFunction("now", () -> new SqlDateTime(LocalDateTime.now())));
    }

    // ── Aggregate functions ──────────────────────────────────────────────

    private static void registerAggregateFunctions(FunctionRegistry r) {
        r.registerAggregate(new AggregateFunction("count",
                values -> SqlNumber.of(values.size())));

        r.registerAggregate(new AggregateFunction("sum",
                values -> SqlNumber.of(values.stream()
                        .filter(SqlNumber.class::isInstance)
                        .mapToDouble(v -> ((SqlNumber) v).doubleValue())
                        .sum())));

        r.registerAggregate(new AggregateFunction("avg", values -> {
            List<SqlNumber> nums = values.stream()
                    .filter(SqlNumber.class::isInstance)
                    .map(v -> (SqlNumber) v)
                    .toList();
            if (nums.isEmpty()) return SqlNull.INSTANCE;
            return SqlNumber.of(nums.stream().mapToDouble(SqlNumber::doubleValue).sum() / nums.size());
        }));

        r.registerAggregate(new AggregateFunction("min",
                values -> values.stream()
                        .filter(v -> !v.isNull())
                        .min(SqlValueComparator::compare)
                        .orElse(SqlNull.INSTANCE)));

        r.registerAggregate(new AggregateFunction("max",
                values -> values.stream()
                        .filter(v -> !v.isNull())
                        .max(SqlValueComparator::compare)
                        .orElse(SqlNull.INSTANCE)));
    }

    // ── Core string function bodies ────────────────────────────────────

    private static SqlValue lowerFn(SqlValue val, List<SqlValue> args) {
        if (!(val instanceof SqlString(var value))) return val;
        Locale locale = args.isEmpty() ? Locale.getDefault()
                : Locale.forLanguageTag(((SqlString) args.getFirst()).value());
        return new SqlString(value.toLowerCase(locale));
    }

    private static SqlValue upperFn(SqlValue val, List<SqlValue> args) {
        if (!(val instanceof SqlString(var value))) return val;
        Locale locale = args.isEmpty() ? Locale.getDefault()
                : Locale.forLanguageTag(((SqlString) args.getFirst()).value());
        return new SqlString(value.toUpperCase(locale));
    }

    private static SqlValue toDateFn(SqlValue val, List<SqlValue> args) {
        if (!(val instanceof SqlString(var value))) return val;
        if (args.isEmpty()) {
            return parseToDateWithoutFormat(value);
        }
        String format = ((SqlString) args.getFirst()).value();
        return parseToDateWithFormat(value, format);
    }

    // ── String manipulation function bodies ────────────────────────────

    private static SqlValue concatFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        for (SqlValue arg : args) {
            if (arg.isNull()) return SqlNull.INSTANCE;
        }
        var sb = new StringBuilder(val.rawValue().toString());
        for (SqlValue arg : args) {
            sb.append(arg.rawValue().toString());
        }
        return new SqlString(sb.toString());
    }

    private static SqlValue substringFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlString(var value))) return val;
        int start = args.isEmpty() ? 1 : (int) ((SqlNumber) args.get(0)).doubleValue();
        int length = args.size() < 2 ? value.length() : (int) ((SqlNumber) args.get(1)).doubleValue();
        if (length <= 0) return new SqlString("");
        int zeroStart;
        if (start < 1) {
            length = length + start - 1;
            zeroStart = 0;
        } else {
            zeroStart = start - 1;
        }
        if (length <= 0 || zeroStart >= value.length()) return new SqlString("");
        int end = Math.min(zeroStart + length, value.length());
        return new SqlString(value.substring(zeroStart, end));
    }

    private static SqlValue trimFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlString(var value))) return val;
        return new SqlString(value.strip());
    }

    private static SqlValue lengthFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlString(var value))) return val;
        return SqlNumber.of(value.length());
    }

    private static SqlValue replaceFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlString(var value))) return val;
        String search = ((SqlString) args.get(0)).value();
        String replacement = ((SqlString) args.get(1)).value();
        return new SqlString(value.replace(search, replacement));
    }

    // ── String padding function bodies ──────────────────────────────────

    private static SqlValue leftFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlString(var value))) return val;
        int n = (int) ((SqlNumber) args.getFirst()).doubleValue();
        int end = Math.min(n, value.length());
        return new SqlString(value.substring(0, end));
    }

    private static SqlValue rightFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlString(var value))) return val;
        int n = (int) ((SqlNumber) args.getFirst()).doubleValue();
        int len = value.length();
        int start = Math.max(0, len - n);
        return new SqlString(value.substring(start));
    }

    private static SqlValue lpadFn(SqlValue val, List<SqlValue> args) {
        return padFn(val, args, true);
    }

    private static SqlValue rpadFn(SqlValue val, List<SqlValue> args) {
        return padFn(val, args, false);
    }

    private static SqlValue padFn(SqlValue val, List<SqlValue> args, boolean leftPad) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlString(var value))) return val;
        int targetLen = (int) ((SqlNumber) args.get(0)).doubleValue();
        String padStr = ((SqlString) args.get(1)).value();
        if (value.length() >= targetLen) return new SqlString(value.substring(0, targetLen));
        String padding = repeatPad(padStr, targetLen - value.length());
        return new SqlString(leftPad ? padding + value : value + padding);
    }

    private static String repeatPad(String padStr, int needed) {
        var sb = new StringBuilder();
        while (sb.length() < needed) sb.append(padStr);
        return sb.substring(0, needed);
    }

    private static SqlValue reverseFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlString(var value))) return val;
        return new SqlString(new StringBuilder(value).reverse().toString());
    }

    private static SqlValue positionFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlString(var value))) return val;
        String str = ((SqlString) args.getFirst()).value();
        int idx = str.indexOf(value);
        return SqlNumber.of(idx < 0 ? 0 : idx + 1);
    }

    // ── Math function bodies ────────────────────────────────────────────

    private static SqlValue absFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlNumber n)) return SqlNull.INSTANCE;
        return numOf(Math.abs(n.doubleValue()));
    }

    private static SqlValue roundFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlNumber n)) return SqlNull.INSTANCE;
        int decimals = args.isEmpty() ? 0 : (int) ((SqlNumber) args.getFirst()).doubleValue();
        double result = BigDecimal.valueOf(n.doubleValue())
                .setScale(decimals, RoundingMode.HALF_UP)
                .doubleValue();
        return numOf(result);
    }

    private static SqlValue ceilFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlNumber n)) return SqlNull.INSTANCE;
        return numOf(Math.ceil(n.doubleValue()));
    }

    private static SqlValue floorFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlNumber n)) return SqlNull.INSTANCE;
        return numOf(Math.floor(n.doubleValue()));
    }

    private static SqlValue modFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlNumber n)) return SqlNull.INSTANCE;
        double divisor = ((SqlNumber) args.getFirst()).doubleValue();
        if (divisor == 0) return SqlNull.INSTANCE;
        return numOf(n.doubleValue() % divisor);
    }

    private static SqlValue powerFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlNumber n)) return SqlNull.INSTANCE;
        double exp = ((SqlNumber) args.getFirst()).doubleValue();
        double result = Math.pow(n.doubleValue(), exp);
        if (Double.isInfinite(result) || Double.isNaN(result)) return SqlNull.INSTANCE;
        return numOf(result);
    }

    private static SqlValue sqrtFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlNumber n)) return SqlNull.INSTANCE;
        if (n.doubleValue() < 0) return SqlNull.INSTANCE;
        return numOf(Math.sqrt(n.doubleValue()));
    }

    private static SqlValue signFn(SqlValue val, List<SqlValue> args) {
        if (val.isNull()) return SqlNull.INSTANCE;
        if (!(val instanceof SqlNumber n)) return SqlNull.INSTANCE;
        return SqlNumber.of(Double.compare(n.doubleValue(), 0));
    }

    // ── Private helper methods ───────────────────────────────────────────

    /**
     * Parses a date string without a format pattern: tries ISO datetime first,
     * then falls back to ISO date.
     */
    private static SqlValue parseToDateWithoutFormat(String value) {
        try {
            return new SqlDateTime(LocalDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME));
        } catch (DateTimeParseException ignored) {
            return new SqlDate(LocalDate.parse(value, DateTimeFormatter.ISO_DATE));
        }
    }

    /**
     * Parses a date string with a given format pattern: tries datetime first,
     * then falls back to date-only.
     */
    private static SqlValue parseToDateWithFormat(String value, String format) {
        DateTimeFormatter formatter;
        try {
            formatter = DateTimeFormatter.ofPattern(format);
        } catch (IllegalArgumentException e) {
            throw new SQL4JsonExecutionException("Invalid date format pattern", e);
        }
        try {
            return new SqlDateTime(LocalDateTime.parse(value, formatter));
        } catch (DateTimeParseException ignored) {
            return new SqlDate(LocalDate.parse(value, formatter));
        }
    }

    /**
     * Parses a string to long, trying Long.parseLong first and falling back
     * to parsing as double as truncating.
     */
    private static long parseStringToLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return (long) Double.parseDouble(value);
        }
    }
}
