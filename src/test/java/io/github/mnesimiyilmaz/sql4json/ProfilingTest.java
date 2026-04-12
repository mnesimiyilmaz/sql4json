package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.ToLongFunction;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Scaling profiling suite for SQL4Json.
 *
 * <p>Runs a representative set of queries — including JOIN and window functions — against 7
 * JSON file sizes (8/16/32/64/128/256/512 MB). Captures wall-clock, thread CPU time, user time,
 * heap before/after/delta, and peak heap per run via JMX. Emits a markdown report at
 * {@code target/profiling/profiling-report-<timestamp>.md} plus live stdout output.
 *
 * <p>Tiering: the full scenario list runs against 8/16/32/64 MB. A curated subset (one
 * representative per query shape) runs against 128/256/512 MB to keep total runtime reasonable
 * while still drawing a scaling curve at the heavy sizes.
 *
 * <p>Usage:
 * <pre>
 *   ./mvnw test -Dtest="ProfilingTest" -P large-tests
 *   ./mvnw test -Dtest="ProfilingTest#profile_00_008mb" -P large-tests
 * </pre>
 */
@Tag("large")
@TestMethodOrder(MethodOrderer.MethodName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProfilingTest {

    private static final Path                   DATA_DIR   = Path.of("src/test/resources/data-files");
    private static final ThreadMXBean           TMX        = ManagementFactory.getThreadMXBean();
    private static final List<MemoryPoolMXBean> HEAP_POOLS =
            ManagementFactory.getMemoryPoolMXBeans().stream()
                    .filter(p -> p.getType() == MemoryType.HEAP)
                    .toList();

    private static final List<QueryScenario> SCENARIOS = buildScenarios();

    private       String        countryLookupJson;
    private final List<SizeRun> allRuns = new ArrayList<>();

    // ──────────────────────────────────────────────────────────────
    //  Inner types
    // ──────────────────────────────────────────────────────────────

    private enum Tier {FULL, CURATED}

    @FunctionalInterface
    private interface ResultAssertion {
        void check(JsonValue result);
    }

    private record QueryScenario(
            String label,
            String sql,
            Tier tier,
            boolean isJoin,
            ResultAssertion assertion) {
    }

    private record RunMetrics(
            String label,
            String sizeLabel,
            String sql,
            Tier tier,
            long wallMs,
            long cpuMs,
            long userMs,
            long heapBeforeMB,
            long heapAfterMB,
            long heapDeltaMB,
            long peakHeapMB,
            long rows,
            String error) {
    }

    private static final class SizeRun {
        final String           sizeLabel;
        final long             bytes;
        final long             loadMs;
        final List<RunMetrics> results = new ArrayList<>();

        SizeRun(String sizeLabel, long bytes, long loadMs) {
            this.sizeLabel = sizeLabel;
            this.bytes = bytes;
            this.loadMs = loadMs;
        }
    }

    // ──────────────────────────────────────────────────────────────
    //  Lifecycle
    // ──────────────────────────────────────────────────────────────

    @BeforeAll
    void setup() {
        countryLookupJson = buildCountryLookup();
        assertTrue(countryLookupJson.startsWith("[") && countryLookupJson.endsWith("]"),
                "country lookup must be a JSON array");
        assertTrue(countryLookupJson.length() > 500,
                "country lookup should have real content, got " + countryLookupJson.length() + " chars");

        System.out.println("═══════════════════════════════════════════════════════════════");
        System.out.println("  SQL4Json Profiling Suite — scaling sweep 8 MB → 512 MB");
        System.out.printf("  %d scenarios loaded; tiering: FULL={8,16,32,64}MB, CURATED={128,256,512}MB%n",
                SCENARIOS.size());
        System.out.println("═══════════════════════════════════════════════════════════════");
    }

    @AfterAll
    void summary() throws IOException {
        writeReport();
        System.out.println("═══════════════════════════════════════════════════════════════");
        System.out.println("  Profiling complete. Report written to target/profiling/");
        System.out.println("═══════════════════════════════════════════════════════════════");
    }

    // ──────────────────────────────────────────────────────────────
    //  Tests — one per file size
    // ──────────────────────────────────────────────────────────────

    @Test
    void profile_00_008mb() throws IOException {
        runSize("8mb", "data_8mb.json", Tier.FULL);
    }

    @Test
    void profile_01_016mb() throws IOException {
        runSize("16mb", "data_16mb.json", Tier.FULL);
    }

    @Test
    void profile_02_032mb() throws IOException {
        runSize("32mb", "data_32mb.json", Tier.FULL);
    }

    @Test
    void profile_03_064mb() throws IOException {
        runSize("64mb", "data_64mb.json", Tier.FULL);
    }

    @Test
    void profile_04_128mb() throws IOException {
        runSize("128mb", "data_128mb.json", Tier.CURATED);
    }

    @Test
    void profile_05_256mb() throws IOException {
        runSize("256mb", "data_256mb.json", Tier.CURATED);
    }

    @Test
    void profile_06_512mb() throws IOException {
        runSize("512mb", "data_512mb.json", Tier.CURATED);
    }

    // ──────────────────────────────────────────────────────────────
    //  Size sweep
    // ──────────────────────────────────────────────────────────────

    private void runSize(String sizeLabel, String fileName, Tier tier) throws IOException {
        Path file = DATA_DIR.resolve(fileName);
        Assumptions.assumeTrue(Files.exists(file), "Missing data file: " + file);

        long bytes = Files.size(file);
        System.gc();
        long t0 = System.nanoTime();
        String jsonData = Files.readString(file);
        long loadMs = (System.nanoTime() - t0) / 1_000_000;

        SizeRun sizeRun = new SizeRun(sizeLabel, bytes, loadMs);
        allRuns.add(sizeRun);

        System.out.printf("%n▼ %-6s  %,d bytes  loaded in %,d ms%n", sizeLabel, bytes, loadMs);
        System.out.println("───────────────────────────────────────────────────────────────");

        warmup(jsonData);

        for (QueryScenario scenario : SCENARIOS) {
            if (!shouldRun(scenario, tier)) continue;
            sizeRun.results.add(runOne(scenario, jsonData, sizeLabel));
        }
    }

    private static boolean shouldRun(QueryScenario s, Tier sizeTier) {
        if (sizeTier == Tier.FULL) return true;
        return s.tier() == Tier.CURATED;
    }

    // ──────────────────────────────────────────────────────────────
    //  Metrics helpers
    // ──────────────────────────────────────────────────────────────

    private RunMetrics runOne(QueryScenario scenario, String jsonData, String sizeLabel) {
        System.gc();
        HEAP_POOLS.forEach(MemoryPoolMXBean::resetPeakUsage);
        long heapBefore = usedHeapMB();
        long cpuBefore = TMX.getCurrentThreadCpuTime();
        long userBefore = TMX.getCurrentThreadUserTime();
        long wallStart = System.nanoTime();

        JsonValue result = null;
        String error = null;
        try {
            result = scenario.isJoin()
                    ? SQL4Json.queryAsJsonValue(
                    scenario.sql(),
                    Map.of("users", jsonData, "countries", countryLookupJson),
                    LargeTestSettings.INSTANCE)
                    : SQL4Json.queryAsJsonValue(
                    scenario.sql(), jsonData, LargeTestSettings.INSTANCE);
        } catch (Throwable t) {
            error = t.getClass().getSimpleName() + ": " + t.getMessage();
        }

        long wallMs = (System.nanoTime() - wallStart) / 1_000_000;
        long cpuMs = (TMX.getCurrentThreadCpuTime() - cpuBefore) / 1_000_000;
        long userMs = (TMX.getCurrentThreadUserTime() - userBefore) / 1_000_000;

        long peakHeapBytes = HEAP_POOLS.stream()
                .mapToLong(p -> p.getPeakUsage().getUsed())
                .sum();
        long peakHeapMB = peakHeapBytes / (1024 * 1024);

        System.gc();
        long heapAfter = usedHeapMB();

        long rows;
        if (result == null) {
            rows = 0;
        } else if (result.isArray()) {
            rows = result.asArray().get().size();
        } else {
            rows = 1;
        }

        if (error == null && result != null) {
            try {
                scenario.assertion().check(result);
            } catch (AssertionError ae) {
                error = "ASSERTION: " + ae.getMessage();
            }
        }

        RunMetrics m = new RunMetrics(
                scenario.label(), sizeLabel, scenario.sql(), scenario.tier(),
                wallMs, cpuMs, userMs,
                heapBefore, heapAfter, heapAfter - heapBefore, peakHeapMB,
                rows, error);
        printLine(m);
        return m;
    }

    private void warmup(String jsonData) {
        List<String> warmupQueries = List.of(
                "SELECT * FROM $r LIMIT 10",
                "SELECT COUNT(*) FROM $r",
                "SELECT address.country, COUNT(*) FROM $r GROUP BY address.country");
        for (String q : warmupQueries) {
            try {
                SQL4Json.query(q, jsonData, LargeTestSettings.INSTANCE);
            } catch (Throwable ignored) {
                // warmup errors are non-fatal
            }
        }
    }

    private void printLine(RunMetrics m) {
        if (m.error() != null) {
            System.out.printf("[%-38s] FAIL  %s%n", m.label(), m.error());
            return;
        }
        System.out.printf(
                "[%-38s] %,6d ms  cpu %,6d  user %,6d  heap %d→%d MB (%+d, peak %d)  rows: %,d%n",
                m.label(), m.wallMs(), m.cpuMs(), m.userMs(),
                m.heapBeforeMB(), m.heapAfterMB(), m.heapDeltaMB(), m.peakHeapMB(), m.rows());
    }

    private static long usedHeapMB() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
    }

    // ──────────────────────────────────────────────────────────────
    //  Scenario list
    // ──────────────────────────────────────────────────────────────

    private static List<QueryScenario> buildScenarios() {
        List<QueryScenario> s = new ArrayList<>();

        // ── 1. Baseline — full scan ─────────────────────────────
        s.add(new QueryScenario("p01 SELECT-ALL",
                "SELECT * FROM $r",
                Tier.CURATED, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p02 SELECT-ALL-LIMIT-100",
                "SELECT * FROM $r LIMIT 100",
                Tier.CURATED, false,
                r -> assertEquals(100, r.asArray().get().size())));

        s.add(new QueryScenario("p03 LIMIT-1000-OFFSET-5000",
                "SELECT * FROM $r LIMIT 1000 OFFSET 5000",
                Tier.FULL, false,
                r -> assertEquals(1000, r.asArray().get().size())));

        // ── 2. Projection ───────────────────────────────────────
        s.add(new QueryScenario("p04 PROJECT-TOP-LEVEL",
                "SELECT id, is_active, score FROM $r",
                Tier.FULL, false,
                r -> {
                    assertTrue(r.isArray());
                    var first = r.asArray().get().getFirst().asObject().get();
                    assertTrue(first.containsKey("id"));
                    assertTrue(first.containsKey("score"));
                    assertFalse(first.containsKey("bio"));
                }));

        s.add(new QueryScenario("p05 PROJECT-NESTED",
                "SELECT id, address.city, address.geo.lat, metadata.audit.review_status FROM $r",
                Tier.FULL, false,
                r -> {
                    assertTrue(r.isArray());
                    assertFalse(r.asArray().get().isEmpty());
                }));

        s.add(new QueryScenario("p06 PROJECT-ALIAS",
                "SELECT id, address.city AS city, score AS user_score FROM $r",
                Tier.FULL, false,
                r -> {
                    var first = r.asArray().get().getFirst().asObject().get();
                    assertTrue(first.containsKey("city"));
                    assertTrue(first.containsKey("user_score"));
                }));

        // ── 3. GROUP BY + aggregation ───────────────────────────
        s.add(new QueryScenario("p07 GROUP-BY-CITY-COUNT",
                "SELECT address.city AS city, COUNT(*) AS cnt FROM $r GROUP BY address.city",
                Tier.CURATED, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p08 GROUP-BY-COUNTRY-MULTI-AGG",
                "SELECT address.country AS country, COUNT(*) AS cnt, AVG(score) AS avg_score, " +
                        "MIN(score) AS min_score, MAX(score) AS max_score FROM $r GROUP BY address.country",
                Tier.CURATED, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p09 GROUP-BY-THEME-HAVING",
                "SELECT preferences.theme AS theme, COUNT(*) AS cnt, AVG(score) AS avg_score " +
                        "FROM $r GROUP BY preferences.theme HAVING cnt >= 100",
                Tier.FULL, false,
                r -> {
                    assertTrue(r.isArray());
                    for (var row : r.asArray().get()) {
                        long cnt = row.asObject().get().get("cnt").asNumber().orElseThrow().longValue();
                        assertTrue(cnt >= 100);
                    }
                }));

        s.add(new QueryScenario("p10 GROUP-BY-REVIEW-STATUS",
                "SELECT metadata.audit.review_status AS status, COUNT(*) AS cnt, " +
                        "SUM(metadata.audit.comments_count) AS total_comments " +
                        "FROM $r GROUP BY metadata.audit.review_status",
                Tier.FULL, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        // ── 4. WHERE filtering ──────────────────────────────────
        s.add(new QueryScenario("p11 WHERE-BOOL-EQ",
                "SELECT id, score FROM $r WHERE is_active = true",
                Tier.FULL, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p12 WHERE-RANGE-SCORE",
                "SELECT id, score FROM $r WHERE score >= 80 AND score <= 90",
                Tier.CURATED, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p13 WHERE-STRING-EQ",
                "SELECT id, address.city FROM $r WHERE address.city = 'Tokyo'",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p14 WHERE-LIKE",
                "SELECT id, bio FROM $r WHERE bio LIKE '%database%'",
                Tier.CURATED, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p15 WHERE-DEEP-NESTED",
                "SELECT id, metadata.audit.review_status FROM $r " +
                        "WHERE metadata.audit.review_status = 'approved' AND metadata.audit.comments_count > 5",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p16 WHERE-OR-COMPOUND",
                "SELECT id, address.country, score FROM $r " +
                        "WHERE (address.country = 'UK' OR address.country = 'US') AND score > 50",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p17 WHERE-IN",
                "SELECT id, address.city FROM $r WHERE address.city IN ('Tokyo', 'London', 'Berlin', 'Paris')",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p18 WHERE-BETWEEN",
                "SELECT id, score FROM $r WHERE score BETWEEN 40 AND 60",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p19 WHERE-IS-NULL",
                "SELECT id FROM $r WHERE address.state IS NULL",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p20 WHERE-NOT-LIKE",
                "SELECT id FROM $r WHERE bio NOT LIKE '%networking%'",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        // ── 5. ORDER BY ─────────────────────────────────────────
        s.add(new QueryScenario("p21 ORDER-BY-SCORE-ASC",
                "SELECT id, score FROM $r ORDER BY score ASC",
                Tier.CURATED, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p22 ORDER-BY-CITY-DESC",
                "SELECT id, address.city FROM $r ORDER BY address.city DESC",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p23 ORDER-BY-MULTI",
                "SELECT id, address.country, score FROM $r ORDER BY address.country ASC, score DESC",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p24 ORDER-BY-SCORE-TOP50",
                "SELECT id, score FROM $r ORDER BY score DESC LIMIT 50",
                Tier.CURATED, false,
                r -> assertEquals(50, r.asArray().get().size())));

        // ── 6. DISTINCT ─────────────────────────────────────────
        s.add(new QueryScenario("p25 DISTINCT-CITY",
                "SELECT DISTINCT address.city FROM $r",
                Tier.CURATED, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p26 DISTINCT-COUNTRY-THEME",
                "SELECT DISTINCT address.country, preferences.theme FROM $r",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        // ── 7. Functions ────────────────────────────────────────
        s.add(new QueryScenario("p27 FUNC-UPPER",
                "SELECT id, UPPER(address.city) AS city_upper FROM $r LIMIT 10000",
                Tier.FULL, false,
                r -> assertEquals(10_000, r.asArray().get().size())));

        s.add(new QueryScenario("p28 FUNC-CONCAT",
                "SELECT CONCAT(address.city, ' - ', address.country) AS location FROM $r LIMIT 10000",
                Tier.FULL, false,
                r -> assertEquals(10_000, r.asArray().get().size())));

        s.add(new QueryScenario("p29 FUNC-LENGTH-FILTER",
                "SELECT id, bio FROM $r WHERE LENGTH(bio) > 50",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p30 FUNC-ROUND",
                "SELECT id, ROUND(score, 1) AS rounded_score FROM $r LIMIT 10000",
                Tier.FULL, false,
                r -> assertEquals(10_000, r.asArray().get().size())));

        s.add(new QueryScenario("p31 FUNC-ABS-FILTER",
                "SELECT id, address.geo.lat FROM $r WHERE ABS(address.geo.lat) > 60",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p32 FUNC-COALESCE",
                "SELECT id, COALESCE(address.state, 'N/A') AS state FROM $r LIMIT 10000",
                Tier.FULL, false,
                r -> assertEquals(10_000, r.asArray().get().size())));

        s.add(new QueryScenario("p33 FUNC-NESTED-ROUND-AVG",
                "SELECT address.country AS country, ROUND(AVG(score), 2) AS avg_score " +
                        "FROM $r GROUP BY address.country",
                Tier.FULL, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p34 FUNC-SUBSTRING",
                "SELECT id, SUBSTRING(bio, 1, 10) AS bio_prefix FROM $r LIMIT 10000",
                Tier.FULL, false,
                r -> assertEquals(10_000, r.asArray().get().size())));

        s.add(new QueryScenario("p35 FUNC-TRIM-UPPER",
                "SELECT id, UPPER(TRIM(address.city)) AS clean_city FROM $r LIMIT 10000",
                Tier.FULL, false,
                r -> assertEquals(10_000, r.asArray().get().size())));

        // ── 8. Complex / combined ───────────────────────────────
        s.add(new QueryScenario("p36 COMPLEX-FILTER-GROUP-SORT",
                "SELECT address.country AS country, COUNT(*) AS cnt, ROUND(AVG(score), 2) AS avg_score " +
                        "FROM $r WHERE is_active = true AND score > 50 " +
                        "GROUP BY address.country HAVING cnt >= 10 " +
                        "ORDER BY avg_score DESC",
                Tier.CURATED, false,
                r -> {
                    assertTrue(r.isArray());
                    for (var row : r.asArray().get()) {
                        long cnt = row.asObject().get().get("cnt").asNumber().orElseThrow().longValue();
                        assertTrue(cnt >= 10);
                    }
                }));

        s.add(new QueryScenario("p37 COMPLEX-NESTED-GROUP-HAVING",
                "SELECT metadata.source AS source, preferences.theme AS theme, " +
                        "COUNT(*) AS cnt, MAX(score) AS top_score " +
                        "FROM $r WHERE metadata.version >= 3 " +
                        "GROUP BY metadata.source, preferences.theme HAVING cnt >= 5 " +
                        "ORDER BY top_score DESC LIMIT 20",
                Tier.FULL, false,
                r -> assertTrue(r.asArray().get().size() <= 20)));

        s.add(new QueryScenario("p38 COMPLEX-MULTI-COND-PROJ",
                "SELECT id, address.city, score, metadata.audit.review_status AS status " +
                        "FROM $r WHERE score >= 70 AND address.country = 'UK' " +
                        "AND metadata.audit.review_status != 'rejected' " +
                        "ORDER BY score DESC LIMIT 500",
                Tier.FULL, false,
                r -> assertTrue(r.asArray().get().size() <= 500)));

        // ── 9. Subqueries ───────────────────────────────────────
        s.add(new QueryScenario("p39 SUBQUERY-SIMPLE",
                "SELECT id, score FROM (SELECT * FROM $r WHERE is_active = true) WHERE score > 80",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p40 SUBQUERY-GROUP-AFTER-FILTER",
                "SELECT country, COUNT(*) AS cnt, AVG(score) AS avg_score " +
                        "FROM (SELECT address.country AS country, score FROM $r WHERE score >= 60) " +
                        "GROUP BY country ORDER BY avg_score DESC",
                Tier.FULL, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        // ── 10. Prepared / engine (simplified — no special timing here) ──
        s.add(new QueryScenario("p41 PREPARED-GROUPBY",
                "SELECT address.country AS country, COUNT(*) AS cnt FROM $r GROUP BY address.country",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p42 PREPARED-REUSE",
                "SELECT id, score FROM $r WHERE score > 90 LIMIT 100",
                Tier.FULL, false,
                r -> assertTrue(r.asArray().get().size() <= 100)));

        s.add(new QueryScenario("p43 ENGINE-NO-CACHE",
                "SELECT address.city AS city, COUNT(*) AS cnt FROM $r GROUP BY address.city " +
                        "ORDER BY cnt DESC LIMIT 10",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p44 ENGINE-CACHE",
                "SELECT address.country AS country, COUNT(*) AS cnt FROM $r GROUP BY address.country " +
                        "ORDER BY cnt DESC",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p45 ENGINE-MIXED",
                "SELECT id, score FROM $r WHERE score > 95",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        // ── 11. Stress / worst-case ─────────────────────────────
        s.add(new QueryScenario("p46 STRESS-FULL-SORT",
                "SELECT id, score FROM $r ORDER BY score DESC",
                Tier.FULL, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p47 STRESS-HIGH-CARDINALITY-GROUP",
                "SELECT id, COUNT(*) AS cnt FROM $r GROUP BY id",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p48 STRESS-DISTINCT-ID",
                "SELECT DISTINCT id FROM $r",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p49 STRESS-WIDE-PROJECTION",
                "SELECT id, is_active, score, bio, registered_at, " +
                        "address.street, address.city, address.state, address.zip_code, address.country, " +
                        "address.geo.lat, address.geo.lng, address.geo.accuracy_m, " +
                        "metadata.created_at, metadata.updated_at, metadata.version, metadata.source, " +
                        "metadata.audit.last_reviewed_by, metadata.audit.review_status, metadata.audit.comments_count, " +
                        "preferences.theme, preferences.language, " +
                        "preferences.notifications.email, preferences.notifications.sms, " +
                        "preferences.notifications.push, preferences.notifications.frequency " +
                        "FROM $r LIMIT 50000",
                Tier.FULL, false,
                r -> {
                    int n = r.asArray().get().size();
                    assertTrue(n > 0 && n <= 50_000, "expected 1..50000 rows, got " + n);
                }));

        s.add(new QueryScenario("p50 STRESS-COMPLEX-SUBQUERY",
                "SELECT city, cnt, avg_score FROM (" +
                        "SELECT address.city AS city, COUNT(*) AS cnt, ROUND(AVG(score), 2) AS avg_score " +
                        "FROM $r WHERE is_active = true AND score > 30 " +
                        "GROUP BY address.city HAVING cnt >= 10" +
                        ") ORDER BY avg_score DESC LIMIT 20",
                Tier.CURATED, false,
                r -> assertTrue(r.asArray().get().size() <= 20)));

        // ── 12. JOIN — main × synthetic country lookup ──────────
        s.add(new QueryScenario("p51 JOIN-INNER",
                "SELECT u.id, u.address.country, c.name, c.region " +
                        "FROM users u JOIN countries c ON u.address.country = c.code",
                Tier.CURATED, true,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p52 JOIN-LEFT",
                "SELECT u.id, u.address.country, c.name " +
                        "FROM users u LEFT JOIN countries c ON u.address.country = c.code",
                Tier.FULL, true,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p53 JOIN-INNER-FILTER",
                "SELECT u.id, u.score, c.region " +
                        "FROM users u JOIN countries c ON u.address.country = c.code " +
                        "WHERE u.score > 70",
                Tier.FULL, true,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p54 JOIN-INNER-GROUPBY",
                "SELECT c.region AS region, COUNT(*) AS cnt, AVG(u.score) AS avg_score " +
                        "FROM users u JOIN countries c ON u.address.country = c.code " +
                        "GROUP BY c.region",
                Tier.CURATED, true,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p55 JOIN-INNER-ORDERBY-LIMIT",
                "SELECT u.id, u.score, c.name " +
                        "FROM users u JOIN countries c ON u.address.country = c.code " +
                        "ORDER BY u.score DESC LIMIT 100",
                Tier.FULL, true,
                r -> assertTrue(r.asArray().get().size() <= 100)));

        s.add(new QueryScenario("p56 JOIN-LEFT-ANTI",
                "SELECT u.id, u.address.country " +
                        "FROM users u LEFT JOIN countries c ON u.address.country = c.code " +
                        "WHERE c.code IS NULL",
                Tier.FULL, true,
                r -> assertTrue(r.isArray())));

        // ── 13. Window functions ────────────────────────────────
        s.add(new QueryScenario("p57 WINDOW-ROW-NUMBER-GLOBAL",
                "SELECT id, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM $r LIMIT 100",
                Tier.FULL, false,
                r -> assertTrue(r.asArray().get().size() <= 100)));

        s.add(new QueryScenario("p58 WINDOW-ROW-NUMBER-PART",
                "SELECT id, address.country, ROW_NUMBER() OVER (PARTITION BY address.country ORDER BY score DESC) AS rn FROM $r",
                Tier.CURATED, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p59 WINDOW-RANK-PART",
                "SELECT id, address.country, RANK() OVER (PARTITION BY address.country ORDER BY score DESC) AS rnk FROM $r",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p60 WINDOW-DENSE-RANK-PART",
                "SELECT id, preferences.theme, DENSE_RANK() OVER (PARTITION BY preferences.theme ORDER BY score DESC) AS drnk FROM $r",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p61 WINDOW-NTILE-4",
                "SELECT id, NTILE(4) OVER (ORDER BY score DESC) AS quartile FROM $r",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p62 WINDOW-LAG",
                "SELECT id, address.country, LAG(score, 1) OVER (PARTITION BY address.country ORDER BY registered_at) AS prev_score FROM $r",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p63 WINDOW-LEAD",
                "SELECT id, address.country, LEAD(score, 1) OVER (PARTITION BY address.country ORDER BY registered_at) AS next_score FROM $r",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p64 WINDOW-AVG-OVER-PART",
                "SELECT id, address.country, score, AVG(score) OVER (PARTITION BY address.country) AS country_avg FROM $r",
                Tier.CURATED, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        // ── 14. Nested / date / cast functions ──────────────────
        s.add(new QueryScenario("p65 FUNC-NESTED-LPAD-TRIM-COALESCE",
                "SELECT id, LPAD(TRIM(COALESCE(address.city, '')), 12, '*') AS padded FROM $r LIMIT 10000",
                Tier.FULL, false,
                r -> assertEquals(10_000, r.asArray().get().size())));

        s.add(new QueryScenario("p66 FUNC-YEAR-GROUPBY",
                "SELECT YEAR(registered_at) AS yr, COUNT(*) AS cnt FROM $r GROUP BY YEAR(registered_at)",
                Tier.FULL, false,
                r -> assertTrue(r.isArray() && !r.asArray().get().isEmpty())));

        s.add(new QueryScenario("p67 FUNC-DATE-DIFF-FILTER",
                "SELECT id FROM $r WHERE DATE_DIFF(NOW(), registered_at, 'DAY') > 1000",
                Tier.FULL, false,
                r -> assertTrue(r.isArray())));

        s.add(new QueryScenario("p68 FUNC-CAST-INT",
                "SELECT id, CAST(score AS INTEGER) AS score_int FROM $r LIMIT 10000",
                Tier.FULL, false,
                r -> assertEquals(10_000, r.asArray().get().size())));

        return s;
    }

    // ──────────────────────────────────────────────────────────────
    //  Country lookup — synthetic side-table for JOIN profiling
    // ──────────────────────────────────────────────────────────────

    private String buildCountryLookup() {
        String[][] rows = {
                {"US", "United States", "Americas"},
                {"CA", "Canada", "Americas"},
                {"MX", "Mexico", "Americas"},
                {"BR", "Brazil", "Americas"},
                {"AR", "Argentina", "Americas"},
                {"CL", "Chile", "Americas"},
                {"CO", "Colombia", "Americas"},
                {"PE", "Peru", "Americas"},
                {"GB", "United Kingdom", "Europe"},
                {"DE", "Germany", "Europe"},
                {"FR", "France", "Europe"},
                {"IT", "Italy", "Europe"},
                {"ES", "Spain", "Europe"},
                {"NL", "Netherlands", "Europe"},
                {"BE", "Belgium", "Europe"},
                {"CH", "Switzerland", "Europe"},
                {"AT", "Austria", "Europe"},
                {"SE", "Sweden", "Europe"},
                {"NO", "Norway", "Europe"},
                {"DK", "Denmark", "Europe"},
                {"FI", "Finland", "Europe"},
                {"PL", "Poland", "Europe"},
                {"CZ", "Czechia", "Europe"},
                {"PT", "Portugal", "Europe"},
                {"IE", "Ireland", "Europe"},
                {"GR", "Greece", "Europe"},
                {"HU", "Hungary", "Europe"},
                {"RO", "Romania", "Europe"},
                {"RU", "Russia", "Europe"},
                {"UA", "Ukraine", "Europe"},
                {"TR", "Turkey", "Europe"},
                {"JP", "Japan", "Asia"},
                {"CN", "China", "Asia"},
                {"KR", "South Korea", "Asia"},
                {"IN", "India", "Asia"},
                {"ID", "Indonesia", "Asia"},
                {"TH", "Thailand", "Asia"},
                {"VN", "Vietnam", "Asia"},
                {"PH", "Philippines", "Asia"},
                {"MY", "Malaysia", "Asia"},
                {"SG", "Singapore", "Asia"},
                {"PK", "Pakistan", "Asia"},
                {"BD", "Bangladesh", "Asia"},
                {"IL", "Israel", "Asia"},
                {"AE", "United Arab Em.", "Asia"},
                {"SA", "Saudi Arabia", "Asia"},
                {"IR", "Iran", "Asia"},
                {"IQ", "Iraq", "Asia"},
                {"AU", "Australia", "Oceania"},
                {"NZ", "New Zealand", "Oceania"},
                {"ZA", "South Africa", "Africa"},
                {"EG", "Egypt", "Africa"},
                {"NG", "Nigeria", "Africa"},
                {"KE", "Kenya", "Africa"},
                {"MA", "Morocco", "Africa"},
                {"DZ", "Algeria", "Africa"},
                {"TN", "Tunisia", "Africa"},
                {"ET", "Ethiopia", "Africa"},
                {"GH", "Ghana", "Africa"},
        };

        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < rows.length; i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"code\":\"").append(rows[i][0])
                    .append("\",\"name\":\"").append(rows[i][1])
                    .append("\",\"region\":\"").append(rows[i][2]).append("\"}");
        }
        int padCount = 250 - rows.length;
        for (int i = 0; i < padCount; i++) {
            char a = (char) ('A' + i / 26);
            char b = (char) ('A' + i % 26);
            sb.append(",{\"code\":\"").append(a).append(b)
                    .append("\",\"name\":\"Pad").append(a).append(b)
                    .append("\",\"region\":\"Other\"}");
        }
        sb.append(']');
        return sb.toString();
    }

    // ──────────────────────────────────────────────────────────────
    //  Markdown report
    // ──────────────────────────────────────────────────────────────

    private void writeReport() throws IOException {
        if (allRuns.isEmpty()) {
            System.out.println("No runs recorded — skipping report.");
            return;
        }

        Path outDir = Path.of("target", "profiling");
        Files.createDirectories(outDir);
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        Path outFile = outDir.resolve("profiling-report-" + ts + ".md");

        StringBuilder md = new StringBuilder();
        md.append("# SQL4Json Profiling Report — ")
                .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .append("\n\n");

        Runtime rt = Runtime.getRuntime();
        md.append("## Environment\n\n");
        md.append("- JVM: ").append(System.getProperty("java.vm.name"))
                .append(' ').append(System.getProperty("java.vm.version"))
                .append(" (").append(System.getProperty("java.vendor")).append(")\n");
        md.append("- OS:  ").append(System.getProperty("os.name"))
                .append(' ').append(System.getProperty("os.version"))
                .append(" (").append(System.getProperty("os.arch")).append(")\n");
        md.append("- Cores: ").append(rt.availableProcessors()).append('\n');
        md.append("- Max heap: ").append(rt.maxMemory() / (1024 * 1024)).append(" MB\n\n");

        md.append("## File sizes & load time\n\n");
        md.append("| Size | Bytes | Load ms |\n");
        md.append("|------|------:|--------:|\n");
        for (SizeRun sr : allRuns) {
            md.append("| ").append(sr.sizeLabel)
                    .append(" | ").append(String.format("%,d", sr.bytes))
                    .append(" | ").append(String.format("%,d", sr.loadMs))
                    .append(" |\n");
        }
        md.append('\n');

        List<SizeRun> fullTier = new ArrayList<>();
        List<SizeRun> curatedTier = new ArrayList<>();
        for (SizeRun sr : allRuns) {
            if (sr.bytes <= 80L * 1024 * 1024) fullTier.add(sr);
            else curatedTier.add(sr);
        }

        if (!fullTier.isEmpty()) {
            md.append("## Full-suite tier (8 / 16 / 32 / 64 MB)\n\n");
            appendMetricTable(md, "Wall time (ms)", fullTier, RunMetrics::wallMs);
            appendMetricTable(md, "CPU time (ms)", fullTier, RunMetrics::cpuMs);
            appendMetricTable(md, "Peak heap (MB)", fullTier, RunMetrics::peakHeapMB);
        }

        if (!curatedTier.isEmpty()) {
            md.append("## Curated tier (128 / 256 / 512 MB)\n\n");
            appendMetricTable(md, "Wall time (ms)", curatedTier, RunMetrics::wallMs);
            appendMetricTable(md, "CPU time (ms)", curatedTier, RunMetrics::cpuMs);
            appendMetricTable(md, "Peak heap (MB)", curatedTier, RunMetrics::peakHeapMB);
        }

        md.append("## Analysis\n\n");
        md.append("_To be filled in after reading the numbers._\n");

        Files.writeString(outFile, md.toString(),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("Report: " + outFile.toAbsolutePath());
    }

    private void appendMetricTable(
            StringBuilder md,
            String title,
            List<SizeRun> tier,
            ToLongFunction<RunMetrics> metric) {

        Set<String> labels = new LinkedHashSet<>();
        for (SizeRun sr : tier) {
            for (RunMetrics m : sr.results) labels.add(m.label());
        }

        md.append("### ").append(title).append("\n\n");
        md.append("| Label |");
        for (SizeRun sr : tier) md.append(' ').append(sr.sizeLabel).append(" |");
        md.append("\n|-------|");
        for (int i = 0; i < tier.size(); i++) md.append("------:|");
        md.append('\n');

        for (String label : labels) {
            md.append("| ").append(label).append(" |");
            for (SizeRun sr : tier) {
                String cell = "—";
                for (RunMetrics m : sr.results) {
                    if (m.label().equals(label)) {
                        cell = m.error() != null ? "FAIL" : String.format("%,d", metric.applyAsLong(m));
                        break;
                    }
                }
                md.append(' ').append(cell).append(" |");
            }
            md.append('\n');
        }
        md.append('\n');
    }
}
