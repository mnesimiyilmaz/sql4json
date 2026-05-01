package io.github.mnesimiyilmaz.sql4json.internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method or constructor as intentionally excluded from JaCoCo branch /
 * line coverage analysis. The {@code Generated} suffix is what JaCoCo (0.8.2+)
 * keys on for automatic exclusion — the {@code SkipCoverage} prefix records
 * the actual intent for human readers: this is hand-written, perf-critical
 * code whose branches are legitimately exercised at runtime but where adding
 * a synthetic unit test for every {@code instanceof} false-branch would
 * require more boilerplate than the optimization is worth.
 *
 * <p>Use sparingly. Default expectation is full coverage.</p>
 *
 * @since 1.2.0
 */
@Retention(RetentionPolicy.CLASS)
@Target({ ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE })
public @interface SkipCoverageGenerated {
}
