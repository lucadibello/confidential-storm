package ch.usi.inf.examples.synthetic_baseline.profiling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Profiler for bolts (one instance per bolt, created as a transient field
 * in {@code prepare()}).
 * <p>
 * CSV snapshots are appended to disk at every reporting interval (tick- or tuple-based),
 * so data is available continuously for long-running topologies.
 * <p>
 * All public methods are safe to call from multiple threads (Storm executor + background snapshot).
 */
public final class BoltProfiler {

    private static final Logger LOG = LoggerFactory.getLogger(BoltProfiler.class);

    private final String componentId;
    private final int taskId;
    private final Map<String, EcallStats> ecallStats = new LinkedHashMap<>();
    private final Map<String, LongAdder> counters = new LinkedHashMap<>();
    private final Map<String, AtomicLong> gauges = new LinkedHashMap<>();
    private int ticksSinceLastReport = 0;
    private int tuplesSinceLastReport = 0;

    /** Lazily opened CSV writer — appends one snapshot per reporting interval. */
    private PrintWriter csvWriter;

    public BoltProfiler(String componentId, int taskId) {
        this.componentId = componentId;
        this.taskId = taskId;
    }

    /**
     * Returns true if this invocation should be timed (random sampling).
     */
    public boolean shouldSample() {
        return ThreadLocalRandom.current().nextDouble() < ProfilerConfig.SAMPLE_RATE;
    }

    /**
     * Returns true for expensive operations that should always be timed when profiling is enabled.
     */
    public boolean shouldAlwaysSample() {
        return ProfilerConfig.ALWAYS_SAMPLE_SNAPSHOTS;
    }

    /**
     * Records a sampled operation timing measurement.
     */
    public void recordEcall(String ecallName, long durationNanos) {
        getOrCreateStats(ecallName).record(durationNanos);
    }

    /**
     * Increments the total (unsampled) invocation count for an operation.
     */
    public void incrementEcallTotal(String ecallName) {
        getOrCreateStats(ecallName).incrementTotal();
    }

    /**
     * Increments a named counter (e.g., "dropped", "dummy_emissions").
     */
    public void incrementCounter(String name) {
        counters.computeIfAbsent(name, k -> new LongAdder()).increment();
    }

    /**
     * Records a gauge value (last-write-wins).
     */
    public void recordGauge(String name, long value) {
        gauges.computeIfAbsent(name, k -> new AtomicLong()).set(value);
    }

    /**
     * Called on each tick tuple. Triggers periodic log dump every {@link ProfilerConfig#REPORT_INTERVAL_TICKS}.
     */
    public void onTick() {
        ticksSinceLastReport++;
        if (ticksSinceLastReport >= ProfilerConfig.REPORT_INTERVAL_TICKS) {
            dumpToLog();
            ticksSinceLastReport = 0;
        }
    }

    /**
     * Called on each processed tuple for non-tick bolts.
     * Triggers periodic log dump every {@link ProfilerConfig#REPORT_INTERVAL_TUPLES}.
     */
    public void onTupleProcessed() {
        tuplesSinceLastReport++;
        if (tuplesSinceLastReport >= ProfilerConfig.REPORT_INTERVAL_TUPLES) {
            dumpToLog();
            tuplesSinceLastReport = 0;
        }
    }

    /**
     * Records a lifecycle event (e.g., COMPONENT_STARTED, COMPONENT_STOPPING).
     * Written immediately to both log and CSV (not buffered like regular metrics).
     */
    public void recordLifecycleEvent(LifecycleEvent event) {
        String eventName = event.name();
        LOG.info(ProfilerReport.toLifecycleLogLine(componentId, taskId, eventName));
        synchronized (this) {
            PrintWriter writer = getCsvWriter();
            if (writer != null) {
                ProfilerReport.writeLifecycleCsvRow(writer, componentId, taskId, eventName);
            }
        }
    }

    /**
     * Records a lifecycle event with an associated epoch number (e.g., EPOCH_ADVANCED).
     * The epoch is stored in the CSV {@code total} column for analysis.
     */
    public void recordLifecycleEvent(LifecycleEvent event, int epoch) {
        String eventName = event.name();
        LOG.info(ProfilerReport.toLifecycleLogLine(componentId, taskId, eventName, epoch));
        synchronized (this) {
            PrintWriter writer = getCsvWriter();
            if (writer != null) {
                ProfilerReport.writeLifecycleCsvRow(writer, componentId, taskId, eventName, epoch);
            }
        }
    }

    /**
     * Flushes the final CSV snapshot, closes the writer, and logs a final summary.
     * Called from {@code cleanup()}.
     */
    public void writeReport() {
        dumpToLog();
        closeCsvWriter();
    }

    private void dumpToLog() {
        LOG.info(ProfilerReport.toHumanReadable(componentId, taskId, ecallStats, counters, gauges));
        LOG.info(ProfilerReport.toJson(componentId, taskId, ecallStats, counters, gauges));
        appendCsvSnapshot();
        resetWindow();
    }

    /**
     * Appends the current window's stats as CSV rows to the report file.
     * The file is opened lazily on first call and kept open for the bolt's lifetime.
     */
    private synchronized void appendCsvSnapshot() {
        PrintWriter writer = getCsvWriter();
        if (writer == null) return;

        ProfilerReport.writeCsvRows(writer, componentId, taskId, ecallStats, counters, gauges);
        writer.flush();
    }

    /**
     * Opens the CSV writer lazily. If the file doesn't exist, it is created and a header row is written.
     */
    private PrintWriter getCsvWriter() {
        if (csvWriter != null) return csvWriter;

        File dir = new File(ProfilerConfig.OUTPUT_DIR);
        if (!dir.exists() && !dir.mkdirs()) {
            LOG.warn("[PROFILER] Failed to create output directory: {}", dir.getAbsolutePath());
            return null;
        }

        File file = new File(dir, String.format("profiler-%s-task%d.csv", componentId, taskId));
        try {
            boolean needsHeader = !file.exists() || file.length() == 0;
            csvWriter = new PrintWriter(new FileWriter(file, true));
            if (needsHeader) {
                ProfilerReport.writeCsvHeader(csvWriter);
                csvWriter.flush();
            }
            LOG.info("[PROFILER] CSV report file opened: {}", file.getAbsolutePath());
        } catch (IOException e) {
            LOG.warn("[PROFILER] Failed to open report file: {}", file.getAbsolutePath(), e);
        }
        return csvWriter;
    }

    private void closeCsvWriter() {
        if (csvWriter != null) {
            File dir = new File(ProfilerConfig.OUTPUT_DIR);
            File file = new File(dir, String.format("profiler-%s-task%d.csv", componentId, taskId));
            LOG.info("[PROFILER] CSV report finalized: {}", file.getAbsolutePath());
            csvWriter.close();
            csvWriter = null;
        }
    }

    private void resetWindow() {
        for (EcallStats stats : ecallStats.values()) {
            stats.reset();
        }
    }

    private EcallStats getOrCreateStats(String name) {
        return ecallStats.computeIfAbsent(name, EcallStats::new);
    }
}
