package ch.usi.inf.confidentialstorm.host.profiling;

/**
 * Build-time profiling configuration. All fields are compile-time constants (literal values
 * substituted by {@code templating-maven-plugin} before {@code javac} runs), so the Java
 * compiler performs constant folding and dead-code elimination on every {@code if (ENABLED)}
 * guard.
 * <p>
 * Override at build time via Maven properties:
 * <pre>
 *   mvn compile -Dprofiler.enabled=true -Dprofiler.sample.rate=0.05
 * </pre>
 * Or simply activate the "profiling" Maven profile to use the default profiling configuration:
 * <pre>
 *   mvn compile -Pprofiling
 * </pre>
 */
public final class ProfilerConfig {

    /** Master kill-switch. When {@code false}, javac eliminates all profiling code. */
    public static final boolean ENABLED = ${profiler.enabled};

    /** Fraction of fast ECALLs to time (e.g. 0.01 = 1%). */
    public static final double SAMPLE_RATE = ${profiler.sample.rate};

    /** Ticks between periodic log dumps (for tick-based bolts). */
    public static final int REPORT_INTERVAL_TICKS = ${profiler.report.interval.ticks};

    /** Tuples between periodic log dumps (for non-tick bolts). */
    public static final int REPORT_INTERVAL_TUPLES = ${profiler.report.interval.tuples};

    /** Directory for CSV reports written at bolt cleanup. */
    public static final String OUTPUT_DIR = "${profiler.output.dir}";

    /** Whether expensive operations (snapshots) are always timed when profiling is enabled. */
    public static final boolean ALWAYS_SAMPLE_SNAPSHOTS = ${profiler.always.sample.snapshots};

    private ProfilerConfig() {}
}
