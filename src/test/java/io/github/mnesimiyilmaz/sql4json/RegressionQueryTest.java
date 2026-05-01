package io.github.mnesimiyilmaz.sql4json;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end regression suite that pins every query result down to the byte against
 * the committed fixtures {@code regression_users.json} and {@code regression_orders.json}.
 *
 * <p>The fixtures are produced by {@code generate_regression_data.py} (sibling to the JSON
 * files) using only integer arithmetic on the row index — output is reproducible across
 * Python versions and OSes. Re-running the script must produce byte-identical output;
 * if it doesn't, the generator has drifted and tests will fail.
 *
 * <p>Each test calls {@link #assertExact(String, String, String)} with the literal JSON
 * we expect SQL4Json to return. Any drift — formatting, ordering, value computation,
 * field-name change — fails the assertion. To re-capture an output after intentional data
 * or query change, temporarily replace {@code assertExact} with {@code captureAndPrint} and
 * paste the printed JSON back into the {@code EXPECTED_*} constant.
 */
class RegressionQueryTest {

    private static final Path USERS_FILE  =
            Path.of("src/test/resources/data-files/regression_users.json");
    private static final Path ORDERS_FILE =
            Path.of("src/test/resources/data-files/regression_orders.json");

    private static String USERS;
    private static String ORDERS;

    @BeforeAll
    static void loadFixtures() throws IOException {
        USERS = Files.readString(USERS_FILE);
        ORDERS = Files.readString(ORDERS_FILE);
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q1 — Aggregation + GROUP BY + HAVING + ORDER BY + ROUND
    // ──────────────────────────────────────────────────────────────────────

    private static final String Q1_SQL = """
            SELECT country,
                   COUNT(*)            AS cnt,
                   ROUND(AVG(score),2) AS avg_score,
                   MIN(salary)         AS min_sal,
                   MAX(salary)         AS max_sal
            FROM $r
            WHERE is_active = true
            GROUP BY country
            HAVING cnt >= 50
            ORDER BY country
            """;

    // Each country has 150 users; with is_active = i%5 != 0 and i%5 distributed
    // evenly across countries, exactly 120 active per country (all 8 pass HAVING).
    // Balanced score distribution makes avg_score=50 for every country.
    private static final String Q1_EXPECTED = "["
            + "{\"country\":\"BR\",\"cnt\":120,\"avg_score\":50,\"min_sal\":40391,\"max_sal\":119727},"
            + "{\"country\":\"CA\",\"cnt\":120,\"avg_score\":50,\"min_sal\":40149,\"max_sal\":118461},"
            + "{\"country\":\"DE\",\"cnt\":120,\"avg_score\":50,\"min_sal\":40118,\"max_sal\":119758},"
            + "{\"country\":\"FR\",\"cnt\":120,\"avg_score\":50,\"min_sal\":40844,\"max_sal\":119516},"
            + "{\"country\":\"IN\",\"cnt\":120,\"avg_score\":50,\"min_sal\":40602,\"max_sal\":119938},"
            + "{\"country\":\"JP\",\"cnt\":120,\"avg_score\":50,\"min_sal\":40329,\"max_sal\":119969},"
            + "{\"country\":\"UK\",\"cnt\":120,\"avg_score\":50,\"min_sal\":40211,\"max_sal\":119907},"
            + "{\"country\":\"US\",\"cnt\":120,\"avg_score\":50,\"min_sal\":41024,\"max_sal\":119696}"
            + "]";

    @Test
    void q1_groupBy_having_orderBy_with_round() {
        assertExact(Q1_SQL, Q1_EXPECTED, "Q1");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q2 — DISTINCT + ORDER BY DESC + LIMIT + OFFSET
    // ──────────────────────────────────────────────────────────────────────

    private static final String Q2_SQL = """
            SELECT DISTINCT tier
            FROM $r
            ORDER BY tier DESC
            LIMIT 3 OFFSET 1
            """;

    // DISTINCT runs AFTER LIMIT in SQL4Json's pipeline (per QueryPipeline). With 1200
    // users each tier appearing ~300 times, ORDER BY tier DESC produces a long run of
    // "silver" rows; LIMIT 3 OFFSET 1 picks rows 2-4 (still all "silver"); DISTINCT
    // collapses them. Locking this in as a regression for the documented stage order.
    private static final String Q2_EXPECTED = "[{\"tier\":\"silver\"}]";

    @Test
    void q2_distinct_orderBy_limit_offset() {
        assertExact(Q2_SQL, Q2_EXPECTED, "Q2");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q3 — Compound WHERE: IN, BETWEEN, LIKE, IS NULL, AND/OR, parens
    // ──────────────────────────────────────────────────────────────────────

    private static final String Q3_SQL = """
            SELECT id, country, age, manager_id
            FROM $r
            WHERE country IN ('US','UK','DE')
              AND age BETWEEN 30 AND 35
              AND email LIKE '%@dom1%'
              AND (manager_id IS NULL OR is_active = true)
            ORDER BY id
            LIMIT 10
            """;

    private static final String Q3_EXPECTED = "["
            + "{\"id\":97,\"country\":\"US\",\"age\":30,\"manager_id\":49},"
            + "{\"id\":217,\"country\":\"US\",\"age\":30,\"manager_id\":409},"
            + "{\"id\":337,\"country\":\"US\",\"age\":30,\"manager_id\":769},"
            + "{\"id\":457,\"country\":\"US\",\"age\":30,\"manager_id\":1129},"
            + "{\"id\":577,\"country\":\"US\",\"age\":30,\"manager_id\":289},"
            + "{\"id\":697,\"country\":\"US\",\"age\":30,\"manager_id\":649},"
            + "{\"id\":817,\"country\":\"US\",\"age\":30,\"manager_id\":1009},"
            + "{\"id\":937,\"country\":\"US\",\"age\":30,\"manager_id\":169},"
            + "{\"id\":1057,\"country\":\"US\",\"age\":30,\"manager_id\":null},"
            + "{\"id\":1177,\"country\":\"US\",\"age\":30,\"manager_id\":889}"
            + "]";

    @Test
    void q3_compound_where_conditions() {
        assertExact(Q3_SQL, Q3_EXPECTED, "Q3");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q4 — Window funcs: ROW_NUMBER, RANK, DENSE_RANK, NTILE
    // ──────────────────────────────────────────────────────────────────────

    private static final String Q4_SQL = """
            SELECT id,
                   score,
                   ROW_NUMBER() OVER (ORDER BY score DESC, id ASC) AS rn,
                   RANK()       OVER (ORDER BY score DESC)         AS rk,
                   DENSE_RANK() OVER (ORDER BY score DESC)         AS drk,
                   NTILE(4)     OVER (ORDER BY score DESC, id ASC) AS q
            FROM $r
            WHERE id <= 12
            ORDER BY rn
            """;

    // For ids 1..12 the scores are unique (i*31 mod 100 for i=0..11), so RANK,
    // DENSE_RANK and ROW_NUMBER all match. NTILE(4) buckets the 12 rows 3-3-3-3.
    private static final String Q4_EXPECTED = "["
            + "{\"id\":4,\"score\":93,\"rn\":1,\"rk\":1,\"drk\":1,\"q\":1},"
            + "{\"id\":7,\"score\":86,\"rn\":2,\"rk\":2,\"drk\":2,\"q\":1},"
            + "{\"id\":10,\"score\":79,\"rn\":3,\"rk\":3,\"drk\":3,\"q\":1},"
            + "{\"id\":3,\"score\":62,\"rn\":4,\"rk\":4,\"drk\":4,\"q\":2},"
            + "{\"id\":6,\"score\":55,\"rn\":5,\"rk\":5,\"drk\":5,\"q\":2},"
            + "{\"id\":9,\"score\":48,\"rn\":6,\"rk\":6,\"drk\":6,\"q\":2},"
            + "{\"id\":12,\"score\":41,\"rn\":7,\"rk\":7,\"drk\":7,\"q\":3},"
            + "{\"id\":2,\"score\":31,\"rn\":8,\"rk\":8,\"drk\":8,\"q\":3},"
            + "{\"id\":5,\"score\":24,\"rn\":9,\"rk\":9,\"drk\":9,\"q\":3},"
            + "{\"id\":8,\"score\":17,\"rn\":10,\"rk\":10,\"drk\":10,\"q\":4},"
            + "{\"id\":11,\"score\":10,\"rn\":11,\"rk\":11,\"drk\":11,\"q\":4},"
            + "{\"id\":1,\"score\":0,\"rn\":12,\"rk\":12,\"drk\":12,\"q\":4}"
            + "]";

    @Test
    void q4_window_ranking_functions() {
        assertExact(Q4_SQL, Q4_EXPECTED, "Q4");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q5 — Window funcs: LAG, LEAD, AVG OVER (running average)
    // ──────────────────────────────────────────────────────────────────────

    // Subquery form: window in inner SELECT, scalar wrap in outer. The direct form
    // (e.g. SELECT ROUND(AVG(x) OVER (...), 2)) is also supported — see
    // WindowFunctionTest#scalar_function_wraps_window_function.
    private static final String Q5_SQL = """
            SELECT id, score, lag_s, lead_s, ROUND(run_avg, 2) AS run_avg
            FROM (
                SELECT id, score,
                       LAG(score, 1)  OVER (ORDER BY id) AS lag_s,
                       LEAD(score, 1) OVER (ORDER BY id) AS lead_s,
                       AVG(score)     OVER (ORDER BY id) AS run_avg
                FROM $r
                WHERE id <= 8
            )
            ORDER BY id
            """;

    // SQL4Json computes AVG OVER (ORDER BY ...) as a partition-wide aggregate, NOT a
    // running average — there is no window-frame support. So run_avg is the same value
    // (46 = sum 368 / 8) for every row. LAG(score, 1) for the first row is null;
    // LEAD(score, 1) for the last row is null. This regression test pins that
    // behaviour — change the expected output if the engine ever gains frame support.
    private static final String Q5_EXPECTED = "["
            + "{\"id\":1,\"score\":0,\"lag_s\":null,\"lead_s\":31,\"run_avg\":46},"
            + "{\"id\":2,\"score\":31,\"lag_s\":0,\"lead_s\":62,\"run_avg\":46},"
            + "{\"id\":3,\"score\":62,\"lag_s\":31,\"lead_s\":93,\"run_avg\":46},"
            + "{\"id\":4,\"score\":93,\"lag_s\":62,\"lead_s\":24,\"run_avg\":46},"
            + "{\"id\":5,\"score\":24,\"lag_s\":93,\"lead_s\":55,\"run_avg\":46},"
            + "{\"id\":6,\"score\":55,\"lag_s\":24,\"lead_s\":86,\"run_avg\":46},"
            + "{\"id\":7,\"score\":86,\"lag_s\":55,\"lead_s\":17,\"run_avg\":46},"
            + "{\"id\":8,\"score\":17,\"lag_s\":86,\"lead_s\":null,\"run_avg\":46}"
            + "]";

    @Test
    void q5_window_lag_lead_running_avg() {
        assertExact(Q5_SQL, Q5_EXPECTED, "Q5");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q6 — INNER JOIN + GROUP BY + cross-source aggregate
    // ──────────────────────────────────────────────────────────────────────

    private static final String Q6_SQL = """
            SELECT u.country               AS country,
                   COUNT(o.order_id)       AS orders,
                   ROUND(SUM(o.amount), 2) AS revenue
            FROM users u
            INNER JOIN orders o ON u.id = o.user_id
            WHERE o.status = 'paid'
            GROUP BY u.country
            ORDER BY u.country
            """;

    private static final String Q6_EXPECTED = "["
            + "{\"country\":\"BR\",\"orders\":37,\"revenue\":6452.55},"
            + "{\"country\":\"CA\",\"orders\":37,\"revenue\":6367.7},"
            + "{\"country\":\"DE\",\"orders\":37,\"revenue\":6409.25},"
            + "{\"country\":\"FR\",\"orders\":37,\"revenue\":6326.15},"
            + "{\"country\":\"IN\",\"orders\":38,\"revenue\":6583.75},"
            + "{\"country\":\"JP\",\"orders\":38,\"revenue\":6539.8},"
            + "{\"country\":\"UK\",\"orders\":38,\"revenue\":6627.7},"
            + "{\"country\":\"US\",\"orders\":38,\"revenue\":6495.85}"
            + "]";

    @Test
    void q6_inner_join_group_aggregate() {
        String actual = SQL4Json.query(Q6_SQL,
                Map.of("users", USERS, "orders", ORDERS));
        assertExpected(Q6_EXPECTED, actual, "Q6");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q7 — LEFT JOIN to find users without paid orders
    // ──────────────────────────────────────────────────────────────────────

    private static final String Q7_SQL = """
            SELECT u.id, u.username, COUNT(o.order_id) AS n_orders
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.id BETWEEN 1 AND 10
            GROUP BY u.id, u.username
            ORDER BY u.id
            """;

    // Note: alias-prefixed fields ("u.id", "u.username") are preserved as-is in the
    // JOIN output — SQL4Json does not strip aliases. This is current behaviour pinned
    // by the assertion. Several users (5, 8, 1) happen to receive a 2nd order in
    // ids 1..10 because of the (j*7) % 1200 user_id formula in generate_regression_data.py.
    private static final String Q7_EXPECTED = "["
            + "{\"u.id\":1,\"u.username\":\"user_0000\",\"n_orders\":2},"
            + "{\"u.id\":2,\"u.username\":\"user_0001\",\"n_orders\":1},"
            + "{\"u.id\":3,\"u.username\":\"user_0002\",\"n_orders\":1},"
            + "{\"u.id\":4,\"u.username\":\"user_0003\",\"n_orders\":1},"
            + "{\"u.id\":5,\"u.username\":\"user_0004\",\"n_orders\":2},"
            + "{\"u.id\":6,\"u.username\":\"user_0005\",\"n_orders\":1},"
            + "{\"u.id\":7,\"u.username\":\"user_0006\",\"n_orders\":1},"
            + "{\"u.id\":8,\"u.username\":\"user_0007\",\"n_orders\":2},"
            + "{\"u.id\":9,\"u.username\":\"user_0008\",\"n_orders\":1},"
            + "{\"u.id\":10,\"u.username\":\"user_0009\",\"n_orders\":1}"
            + "]";

    @Test
    void q7_left_join_with_unmatched() {
        String actual = SQL4Json.query(Q7_SQL,
                Map.of("users", USERS, "orders", ORDERS));
        assertExpected(Q7_EXPECTED, actual, "Q7");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q8 — Subquery in FROM + simple + searched CASE
    // ──────────────────────────────────────────────────────────────────────

    private static final String Q8_SQL = """
            SELECT band, region, COUNT(*) AS cnt
            FROM (
                SELECT id,
                       CASE
                           WHEN score >= 75 THEN 'A'
                           WHEN score >= 50 THEN 'B'
                           WHEN score >= 25 THEN 'C'
                           ELSE 'D'
                       END AS band,
                       CASE country
                           WHEN 'US' THEN 'NA'
                           WHEN 'CA' THEN 'NA'
                           WHEN 'UK' THEN 'EU'
                           WHEN 'DE' THEN 'EU'
                           WHEN 'FR' THEN 'EU'
                           ELSE 'OTHER'
                       END AS region
                FROM $r
            )
            GROUP BY band, region
            ORDER BY band, region
            """;

    // 4 bands × 3 regions = 12 buckets. NA = US+CA = 2/8 of users; EU = UK+DE+FR =
    // 3/8; OTHER = JP+BR+IN = 3/8. Score-band split per the searched CASE thresholds.
    // Sum of all cnt values must equal 1200 (total users).
    private static final String Q8_EXPECTED = "["
            + "{\"band\":\"A\",\"region\":\"EU\",\"cnt\":114},"
            + "{\"band\":\"A\",\"region\":\"NA\",\"cnt\":72},"
            + "{\"band\":\"A\",\"region\":\"OTHER\",\"cnt\":114},"
            + "{\"band\":\"B\",\"region\":\"EU\",\"cnt\":114},"
            + "{\"band\":\"B\",\"region\":\"NA\",\"cnt\":72},"
            + "{\"band\":\"B\",\"region\":\"OTHER\",\"cnt\":114},"
            + "{\"band\":\"C\",\"region\":\"EU\",\"cnt\":108},"
            + "{\"band\":\"C\",\"region\":\"NA\",\"cnt\":78},"
            + "{\"band\":\"C\",\"region\":\"OTHER\",\"cnt\":114},"
            + "{\"band\":\"D\",\"region\":\"EU\",\"cnt\":114},"
            + "{\"band\":\"D\",\"region\":\"NA\",\"cnt\":78},"
            + "{\"band\":\"D\",\"region\":\"OTHER\",\"cnt\":108}"
            + "]";

    @Test
    void q8_subquery_with_case_expressions() {
        assertExact(Q8_SQL, Q8_EXPECTED, "Q8");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q9 — Parameterized queries (named + positional)
    // ──────────────────────────────────────────────────────────────────────

    private static final String Q9_NAMED_SQL = """
            SELECT id, country, tier, age
            FROM $r
            WHERE tier = :t AND age < :max_age
            ORDER BY id
            LIMIT 5
            """;

    private static final String Q9_NAMED_EXPECTED = "["
            + "{\"id\":2,\"country\":\"UK\",\"tier\":\"platinum\",\"age\":25},"
            + "{\"id\":10,\"country\":\"UK\",\"tier\":\"platinum\",\"age\":21},"
            + "{\"id\":54,\"country\":\"BR\",\"tier\":\"platinum\",\"age\":29},"
            + "{\"id\":62,\"country\":\"BR\",\"tier\":\"platinum\",\"age\":25},"
            + "{\"id\":70,\"country\":\"BR\",\"tier\":\"platinum\",\"age\":21}"
            + "]";

    @Test
    void q9a_parameterized_named() {
        String actual = SQL4Json.prepare(Q9_NAMED_SQL).execute(USERS,
                BoundParameters.named().bind("t", "platinum").bind("max_age", 30));
        assertExpected(Q9_NAMED_EXPECTED, actual, "Q9-named");
    }

    private static final String Q9_POS_SQL = """
            SELECT id, country, score
            FROM $r
            WHERE country = ? AND score >= ?
            ORDER BY id
            LIMIT 5
            """;

    private static final String Q9_POS_EXPECTED = "["
            + "{\"id\":17,\"country\":\"US\",\"score\":96},"
            + "{\"id\":33,\"country\":\"US\",\"score\":92},"
            + "{\"id\":49,\"country\":\"US\",\"score\":88},"
            + "{\"id\":65,\"country\":\"US\",\"score\":84},"
            + "{\"id\":81,\"country\":\"US\",\"score\":80}"
            + "]";

    @Test
    void q9b_parameterized_positional() {
        String actual = SQL4Json.prepare(Q9_POS_SQL).execute(USERS,
                BoundParameters.of("US", 80));
        assertExpected(Q9_POS_EXPECTED, actual, "Q9-pos");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q10 — String + math + date + conversion functions in one shot
    // ──────────────────────────────────────────────────────────────────────

    private static final String Q10_SQL = """
            SELECT id,
                   UPPER(country)                  AS c_upper,
                   LENGTH(username)                AS uname_len,
                   CONCAT(username, '@', country)  AS handle,
                   ROUND(SQRT(score), 2)           AS sqrt_score,
                   MOD(id, 7)                      AS id_mod_7,
                   ABS(score)                      AS abs_score,
                   POWER(2, 5)                     AS two_pow_5,
                   FLOOR(salary)                   AS sal_floor,
                   CEIL(salary)                    AS sal_ceil,
                   YEAR(registered_at)             AS year,
                   MONTH(registered_at)            AS month,
                   DAY(registered_at)              AS day,
                   CAST(score AS DECIMAL)          AS score_dec,
                   COALESCE(bio, 'N/A')            AS safe_bio
            FROM $r
            WHERE id IN (1, 7, 42, 100, 365)
            ORDER BY id
            """;

    private static final String Q10_EXPECTED = "["
            + "{\"id\":1,\"c_upper\":\"US\",\"uname_len\":9,\"handle\":\"user_0000@US\","
            + "\"sqrt_score\":0,\"id_mod_7\":1,\"abs_score\":0,\"two_pow_5\":32,"
            + "\"sal_floor\":40000,\"sal_ceil\":40000,\"year\":2018,\"month\":1,\"day\":1,"
            + "\"score_dec\":0,\"safe_bio\":\"N/A\"},"
            + "{\"id\":7,\"c_upper\":\"IN\",\"uname_len\":9,\"handle\":\"user_0006@IN\","
            + "\"sqrt_score\":9.27,\"id_mod_7\":0,\"abs_score\":86,\"two_pow_5\":32,"
            + "\"sal_floor\":41266,\"sal_ceil\":41266,\"year\":2018,\"month\":2,\"day\":24,"
            + "\"score_dec\":86,\"safe_bio\":\"loves chess\"},"
            + "{\"id\":42,\"c_upper\":\"UK\",\"uname_len\":9,\"handle\":\"user_0041@UK\","
            + "\"sqrt_score\":8.43,\"id_mod_7\":0,\"abs_score\":71,\"two_pow_5\":32,"
            + "\"sal_floor\":48651,\"sal_ceil\":48651,\"year\":2019,\"month\":1,\"day\":5,"
            + "\"score_dec\":71,\"safe_bio\":\"coffee snob\"},"
            + "{\"id\":100,\"c_upper\":\"JP\",\"uname_len\":9,\"handle\":\"user_0099@JP\","
            + "\"sqrt_score\":8.31,\"id_mod_7\":2,\"abs_score\":69,\"two_pow_5\":32,"
            + "\"sal_floor\":60889,\"sal_ceil\":60889,\"year\":2020,\"month\":6,\"day\":10,"
            + "\"score_dec\":69,\"safe_bio\":\"cycling fan\"},"
            + "{\"id\":365,\"c_upper\":\"FR\",\"uname_len\":9,\"handle\":\"user_0364@FR\","
            + "\"sqrt_score\":9.17,\"id_mod_7\":1,\"abs_score\":84,\"two_pow_5\":32,"
            + "\"sal_floor\":116804,\"sal_ceil\":116804,\"year\":2026,\"month\":12,\"day\":21,"
            + "\"score_dec\":84,\"safe_bio\":\"N/A\"}"
            + "]";

    @Test
    void q10_string_math_date_conversion_functions() {
        assertExact(Q10_SQL, Q10_EXPECTED, "Q10");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Q11 — Kitchen sink: subquery + GROUP BY + HAVING + every condition
    //        shape + every category of scalar function + simple/searched
    //        CASE + every window function + DISTINCT + multi-key ORDER BY
    //        + LIMIT + OFFSET — all in one statement.
    //
    //  NOW() is intentionally absent: it would break byte-exact assertion.
    //  Date functions are exercised against TO_DATE literals + registered_at.
    // ──────────────────────────────────────────────────────────────────────

    private static final String Q11_SQL = """
            SELECT DISTINCT
                country,
                total_users,
                total_salary,
                avg_score,
                min_score,
                max_score,

                ROUND(avg_score, 2)              AS avg_score_2dp,
                CEIL(avg_score)                  AS avg_ceil,
                FLOOR(min_score)                 AS min_floor,
                ABS(min_score)                   AS min_abs,
                POWER(max_score, 2)              AS max_squared,
                ROUND(SQRT(total_salary), 2)     AS sqrt_total_salary,
                MOD(total_users, 7)              AS users_mod_7,
                SIGN(min_score)                  AS min_sign,

                UPPER(country)                   AS country_upper,
                LOWER(country)                   AS country_lower,
                CONCAT('Country: ', country, ' [', LEFT(country, 1), ']') AS label,
                LENGTH(country)                  AS country_len,
                RIGHT(country, 1)                AS country_last,
                LPAD(country, 4, '*')            AS country_lpad,
                RPAD(country, 4, '*')            AS country_rpad,
                REPLACE(country, 'U', 'X')       AS country_replaced,
                REVERSE(country)                 AS country_reversed,
                SUBSTRING(country, 1, 1)         AS country_sub,
                TRIM(country)                    AS country_trimmed,
                POSITION('S', country)           AS s_pos,

                CAST(total_users AS DECIMAL)     AS users_dec,
                CAST(country AS STRING)          AS country_str,
                CAST(avg_score AS INTEGER)       AS avg_score_int,
                COALESCE(country, 'XX')          AS safe_country,
                NULLIF(country, 'ZZ')            AS not_zz,

                CASE
                    WHEN avg_score >= 70 THEN 'HIGH'
                    WHEN avg_score >= 40 THEN 'MID'
                    ELSE 'LOW'
                END                              AS score_band,

                CASE country
                    WHEN 'US' THEN 'NA'
                    WHEN 'CA' THEN 'NA'
                    WHEN 'UK' THEN 'EU'
                    WHEN 'DE' THEN 'EU'
                    WHEN 'FR' THEN 'EU'
                    WHEN 'JP' THEN 'Asia'
                    WHEN 'IN' THEN 'Asia'
                    ELSE 'Other'
                END                              AS region,

                YEAR(TO_DATE('2026-01-15'))                                       AS fixed_year,
                MONTH(TO_DATE('2026-01-15'))                                      AS fixed_month,
                DAY(TO_DATE('2026-01-15'))                                        AS fixed_day,
                DATE_DIFF(TO_DATE('2026-12-31'), TO_DATE('2020-01-01'), 'DAY')    AS days_2020_to_2026,
                DATE_ADD(TO_DATE('2026-01-15'), 30, 'DAY')                        AS plus_30_days,

                ROW_NUMBER() OVER (ORDER BY total_users DESC, country ASC) AS user_rank,
                RANK()       OVER (ORDER BY avg_score   DESC, country ASC) AS avg_rank,
                DENSE_RANK() OVER (ORDER BY avg_score   DESC, country ASC) AS avg_drank,
                NTILE(4)     OVER (ORDER BY total_users DESC, country ASC) AS users_quartile,
                LAG(total_users, 1)  OVER (ORDER BY country ASC)           AS prev_country_users,
                LEAD(total_users, 1) OVER (ORDER BY country ASC)           AS next_country_users,
                AVG(avg_score)       OVER (ORDER BY country ASC)           AS partition_avg_score
            FROM (
                SELECT
                    country               AS country,
                    COUNT(*)              AS total_users,
                    SUM(salary)           AS total_salary,
                    AVG(score)            AS avg_score,
                    MIN(score)            AS min_score,
                    MAX(score)            AS max_score
                FROM $r
                WHERE
                        is_active = true
                    AND score IS NOT NULL
                    AND age >= 18
                    AND age <= 80
                    AND age BETWEEN 18 AND 80
                    AND age NOT BETWEEN 0 AND 17
                    AND country IS NOT NULL
                    AND country != 'ZZ'
                    AND country IN ('US', 'UK', 'DE', 'JP', 'CA', 'IN', 'BR', 'FR')
                    AND tier NOT IN ('not_a_real_tier')
                    AND email LIKE '%@%'
                    AND email NOT LIKE '%spam%'
                    AND LENGTH(username) > 5
                    AND ABS(salary) >= 1
                    AND YEAR(registered_at) >= 2018
                GROUP BY country
                HAVING total_users > 5 AND avg_score > 30
            )
            ORDER BY avg_score DESC, country ASC
            LIMIT 8 OFFSET 0
            """;

    // 8 country groups (avg_score=50.0 for all; ORDER BY avg_score DESC, country ASC
    // → ties broken alphabetically). LAG/LEAD null at the partition boundaries.
    // partition_avg_score is a window aggregate without frame support → same value
    // for every row (50.0). NTILE(4) splits the 8 rows 2-2-2-2.
    private static final String Q11_EXPECTED = "["
            + "{\"country\":\"BR\",\"total_users\":120,\"total_salary\":9398600.0,\"avg_score\":50.0,"
            + "\"min_score\":3,\"max_score\":99,\"avg_score_2dp\":50,\"avg_ceil\":50,\"min_floor\":3,"
            + "\"min_abs\":3,\"max_squared\":9801,\"sqrt_total_salary\":3065.71,\"users_mod_7\":1,"
            + "\"min_sign\":1,\"country_upper\":\"BR\",\"country_lower\":\"br\","
            + "\"label\":\"Country: BR [B]\",\"country_len\":2,\"country_last\":\"R\","
            + "\"country_lpad\":\"**BR\",\"country_rpad\":\"BR**\",\"country_replaced\":\"BR\","
            + "\"country_reversed\":\"RB\",\"country_sub\":\"B\",\"country_trimmed\":\"BR\","
            + "\"s_pos\":0,\"users_dec\":120,\"country_str\":\"BR\",\"avg_score_int\":50.0,"
            + "\"safe_country\":\"BR\",\"not_zz\":\"BR\",\"score_band\":\"MID\",\"region\":\"Other\","
            + "\"fixed_year\":2026,\"fixed_month\":1,\"fixed_day\":15,\"days_2020_to_2026\":2556,"
            + "\"plus_30_days\":\"2026-02-14\",\"user_rank\":1,\"avg_rank\":1,\"avg_drank\":1,"
            + "\"users_quartile\":1,\"prev_country_users\":null,\"next_country_users\":120,"
            + "\"partition_avg_score\":50.0},"
            + "{\"country\":\"CA\",\"total_users\":120,\"total_salary\":9318600.0,\"avg_score\":50.0,"
            + "\"min_score\":1,\"max_score\":97,\"avg_score_2dp\":50,\"avg_ceil\":50,\"min_floor\":1,"
            + "\"min_abs\":1,\"max_squared\":9409,\"sqrt_total_salary\":3052.64,\"users_mod_7\":1,"
            + "\"min_sign\":1,\"country_upper\":\"CA\",\"country_lower\":\"ca\","
            + "\"label\":\"Country: CA [C]\",\"country_len\":2,\"country_last\":\"A\","
            + "\"country_lpad\":\"**CA\",\"country_rpad\":\"CA**\",\"country_replaced\":\"CA\","
            + "\"country_reversed\":\"AC\",\"country_sub\":\"C\",\"country_trimmed\":\"CA\","
            + "\"s_pos\":0,\"users_dec\":120,\"country_str\":\"CA\",\"avg_score_int\":50.0,"
            + "\"safe_country\":\"CA\",\"not_zz\":\"CA\",\"score_band\":\"MID\",\"region\":\"NA\","
            + "\"fixed_year\":2026,\"fixed_month\":1,\"fixed_day\":15,\"days_2020_to_2026\":2556,"
            + "\"plus_30_days\":\"2026-02-14\",\"user_rank\":2,\"avg_rank\":2,\"avg_drank\":2,"
            + "\"users_quartile\":1,\"prev_country_users\":120,\"next_country_users\":120,"
            + "\"partition_avg_score\":50.0},"
            + "{\"country\":\"DE\",\"total_users\":120,\"total_salary\":9352000.0,\"avg_score\":50.0,"
            + "\"min_score\":2,\"max_score\":98,\"avg_score_2dp\":50,\"avg_ceil\":50,\"min_floor\":2,"
            + "\"min_abs\":2,\"max_squared\":9604,\"sqrt_total_salary\":3058.1,\"users_mod_7\":1,"
            + "\"min_sign\":1,\"country_upper\":\"DE\",\"country_lower\":\"de\","
            + "\"label\":\"Country: DE [D]\",\"country_len\":2,\"country_last\":\"E\","
            + "\"country_lpad\":\"**DE\",\"country_rpad\":\"DE**\",\"country_replaced\":\"DE\","
            + "\"country_reversed\":\"ED\",\"country_sub\":\"D\",\"country_trimmed\":\"DE\","
            + "\"s_pos\":0,\"users_dec\":120,\"country_str\":\"DE\",\"avg_score_int\":50.0,"
            + "\"safe_country\":\"DE\",\"not_zz\":\"DE\",\"score_band\":\"MID\",\"region\":\"EU\","
            + "\"fixed_year\":2026,\"fixed_month\":1,\"fixed_day\":15,\"days_2020_to_2026\":2556,"
            + "\"plus_30_days\":\"2026-02-14\",\"user_rank\":3,\"avg_rank\":3,\"avg_drank\":3,"
            + "\"users_quartile\":2,\"prev_country_users\":120,\"next_country_users\":120,"
            + "\"partition_avg_score\":50.0},"
            + "{\"country\":\"FR\",\"total_users\":120,\"total_salary\":9432000.0,\"avg_score\":50.0,"
            + "\"min_score\":4,\"max_score\":96,\"avg_score_2dp\":50,\"avg_ceil\":50,\"min_floor\":4,"
            + "\"min_abs\":4,\"max_squared\":9216,\"sqrt_total_salary\":3071.16,\"users_mod_7\":1,"
            + "\"min_sign\":1,\"country_upper\":\"FR\",\"country_lower\":\"fr\","
            + "\"label\":\"Country: FR [F]\",\"country_len\":2,\"country_last\":\"R\","
            + "\"country_lpad\":\"**FR\",\"country_rpad\":\"FR**\",\"country_replaced\":\"FR\","
            + "\"country_reversed\":\"RF\",\"country_sub\":\"F\",\"country_trimmed\":\"FR\","
            + "\"s_pos\":0,\"users_dec\":120,\"country_str\":\"FR\",\"avg_score_int\":50.0,"
            + "\"safe_country\":\"FR\",\"not_zz\":\"FR\",\"score_band\":\"MID\",\"region\":\"EU\","
            + "\"fixed_year\":2026,\"fixed_month\":1,\"fixed_day\":15,\"days_2020_to_2026\":2556,"
            + "\"plus_30_days\":\"2026-02-14\",\"user_rank\":4,\"avg_rank\":4,\"avg_drank\":4,"
            + "\"users_quartile\":2,\"prev_country_users\":120,\"next_country_users\":120,"
            + "\"partition_avg_score\":50.0},"
            + "{\"country\":\"IN\",\"total_users\":120,\"total_salary\":9432000.0,\"avg_score\":50.0,"
            + "\"min_score\":2,\"max_score\":98,\"avg_score_2dp\":50,\"avg_ceil\":50,\"min_floor\":2,"
            + "\"min_abs\":2,\"max_squared\":9604,\"sqrt_total_salary\":3071.16,\"users_mod_7\":1,"
            + "\"min_sign\":1,\"country_upper\":\"IN\",\"country_lower\":\"in\","
            + "\"label\":\"Country: IN [I]\",\"country_len\":2,\"country_last\":\"N\","
            + "\"country_lpad\":\"**IN\",\"country_rpad\":\"IN**\",\"country_replaced\":\"IN\","
            + "\"country_reversed\":\"NI\",\"country_sub\":\"I\",\"country_trimmed\":\"IN\","
            + "\"s_pos\":0,\"users_dec\":120,\"country_str\":\"IN\",\"avg_score_int\":50.0,"
            + "\"safe_country\":\"IN\",\"not_zz\":\"IN\",\"score_band\":\"MID\",\"region\":\"Asia\","
            + "\"fixed_year\":2026,\"fixed_month\":1,\"fixed_day\":15,\"days_2020_to_2026\":2556,"
            + "\"plus_30_days\":\"2026-02-14\",\"user_rank\":5,\"avg_rank\":5,\"avg_drank\":5,"
            + "\"users_quartile\":3,\"prev_country_users\":120,\"next_country_users\":120,"
            + "\"partition_avg_score\":50.0},"
            + "{\"country\":\"JP\",\"total_users\":120,\"total_salary\":9385400.0,\"avg_score\":50.0,"
            + "\"min_score\":1,\"max_score\":97,\"avg_score_2dp\":50,\"avg_ceil\":50,\"min_floor\":1,"
            + "\"min_abs\":1,\"max_squared\":9409,\"sqrt_total_salary\":3063.56,\"users_mod_7\":1,"
            + "\"min_sign\":1,\"country_upper\":\"JP\",\"country_lower\":\"jp\","
            + "\"label\":\"Country: JP [J]\",\"country_len\":2,\"country_last\":\"P\","
            + "\"country_lpad\":\"**JP\",\"country_rpad\":\"JP**\",\"country_replaced\":\"JP\","
            + "\"country_reversed\":\"PJ\",\"country_sub\":\"J\",\"country_trimmed\":\"JP\","
            + "\"s_pos\":0,\"users_dec\":120,\"country_str\":\"JP\",\"avg_score_int\":50.0,"
            + "\"safe_country\":\"JP\",\"not_zz\":\"JP\",\"score_band\":\"MID\",\"region\":\"Asia\","
            + "\"fixed_year\":2026,\"fixed_month\":1,\"fixed_day\":15,\"days_2020_to_2026\":2556,"
            + "\"plus_30_days\":\"2026-02-14\",\"user_rank\":6,\"avg_rank\":6,\"avg_drank\":6,"
            + "\"users_quartile\":3,\"prev_country_users\":120,\"next_country_users\":120,"
            + "\"partition_avg_score\":50.0},"
            + "{\"country\":\"UK\",\"total_users\":120,\"total_salary\":9465400.0,\"avg_score\":50.0,"
            + "\"min_score\":3,\"max_score\":99,\"avg_score_2dp\":50,\"avg_ceil\":50,\"min_floor\":3,"
            + "\"min_abs\":3,\"max_squared\":9801,\"sqrt_total_salary\":3076.59,\"users_mod_7\":1,"
            + "\"min_sign\":1,\"country_upper\":\"UK\",\"country_lower\":\"uk\","
            + "\"label\":\"Country: UK [U]\",\"country_len\":2,\"country_last\":\"K\","
            + "\"country_lpad\":\"**UK\",\"country_rpad\":\"UK**\",\"country_replaced\":\"XK\","
            + "\"country_reversed\":\"KU\",\"country_sub\":\"U\",\"country_trimmed\":\"UK\","
            + "\"s_pos\":0,\"users_dec\":120,\"country_str\":\"UK\",\"avg_score_int\":50.0,"
            + "\"safe_country\":\"UK\",\"not_zz\":\"UK\",\"score_band\":\"MID\",\"region\":\"EU\","
            + "\"fixed_year\":2026,\"fixed_month\":1,\"fixed_day\":15,\"days_2020_to_2026\":2556,"
            + "\"plus_30_days\":\"2026-02-14\",\"user_rank\":7,\"avg_rank\":7,\"avg_drank\":7,"
            + "\"users_quartile\":4,\"prev_country_users\":120,\"next_country_users\":120,"
            + "\"partition_avg_score\":50.0},"
            + "{\"country\":\"US\",\"total_users\":120,\"total_salary\":9432000.0,\"avg_score\":50.0,"
            + "\"min_score\":4,\"max_score\":96,\"avg_score_2dp\":50,\"avg_ceil\":50,\"min_floor\":4,"
            + "\"min_abs\":4,\"max_squared\":9216,\"sqrt_total_salary\":3071.16,\"users_mod_7\":1,"
            + "\"min_sign\":1,\"country_upper\":\"US\",\"country_lower\":\"us\","
            + "\"label\":\"Country: US [U]\",\"country_len\":2,\"country_last\":\"S\","
            + "\"country_lpad\":\"**US\",\"country_rpad\":\"US**\",\"country_replaced\":\"XS\","
            + "\"country_reversed\":\"SU\",\"country_sub\":\"U\",\"country_trimmed\":\"US\","
            + "\"s_pos\":2,\"users_dec\":120,\"country_str\":\"US\",\"avg_score_int\":50.0,"
            + "\"safe_country\":\"US\",\"not_zz\":\"US\",\"score_band\":\"MID\",\"region\":\"NA\","
            + "\"fixed_year\":2026,\"fixed_month\":1,\"fixed_day\":15,\"days_2020_to_2026\":2556,"
            + "\"plus_30_days\":\"2026-02-14\",\"user_rank\":8,\"avg_rank\":8,\"avg_drank\":8,"
            + "\"users_quartile\":4,\"prev_country_users\":120,\"next_country_users\":null,"
            + "\"partition_avg_score\":50.0}"
            + "]";

    @Test
    void q11_kitchen_sink_full_pipeline() {
        assertExact(Q11_SQL, Q11_EXPECTED, "Q11");
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Helpers
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Run a single-source query against {@link #USERS} and assert the result is byte-identical
     * to {@code expected}. When {@code expected} is empty, the test prints the actual JSON and
     * fails — useful while bootstrapping a new query (capture mode).
     */
    private static void assertExact(String sql, String expected, String label) {
        String actual = SQL4Json.query(sql, USERS);
        assertExpected(expected, actual, label);
    }

    private static void assertExpected(String expected, String actual, String label) {
        if (expected == null || expected.isEmpty()) {
            // Capture mode — print actual so we can copy it into the EXPECTED_* constant.
            System.out.println("───── " + label + " (capture mode) ─────");
            System.out.println(actual);
            System.out.println("──────────────────────────────────────");
            org.junit.jupiter.api.Assertions.fail(
                    label + ": expected JSON is empty (capture mode). "
                            + "Copy the actual output above into the EXPECTED_* constant.");
        }
        assertEquals(expected, actual, label);
    }
}
