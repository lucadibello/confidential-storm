package ch.usi.inf.confidentialstorm.host.profiling;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Serialization helper for profiler output: human-readable log lines, JSON lines, and CSV.
 */
public final class ProfilerReport {

    private ProfilerReport() {}

    /**
     * Produces a multi-line human-readable summary for SLF4J logging.
     */
    public static String toHumanReadable(String componentId, int taskId,
                                         Map<String, EcallStats> ecallStats,
                                         Map<String, LongAdder> counters,
                                         Map<String, AtomicLong> gauges) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("[PROFILER] %s (task %d) @ %s", componentId, taskId, Instant.now()));
        for (EcallStats stats : ecallStats.values()) {
            if (stats.getSampledCount() > 0) {
                sb.append("\n  ").append(stats.toSummaryLine());
            }
        }
        if (!counters.isEmpty()) {
            sb.append("\n  counters: {");
            boolean first = true;
            for (Map.Entry<String, LongAdder> e : counters.entrySet()) {
                if (!first) sb.append(", ");
                sb.append(e.getKey()).append('=').append(e.getValue().sum());
                first = false;
            }
            sb.append('}');
        }
        if (!gauges.isEmpty()) {
            sb.append("\n  gauges: {");
            boolean first = true;
            for (Map.Entry<String, AtomicLong> e : gauges.entrySet()) {
                if (!first) sb.append(", ");
                sb.append(e.getKey()).append('=').append(e.getValue().get());
                first = false;
            }
            sb.append('}');
        }
        return sb.toString();
    }

    /**
     * Produces a single-line JSON object for programmatic ingestion.
     * Marked with [PROFILER-JSON] for easy grep.
     */
    public static String toJson(String componentId, int taskId,
                                Map<String, EcallStats> ecallStats,
                                Map<String, LongAdder> counters,
                                Map<String, AtomicLong> gauges) {
        StringBuilder sb = new StringBuilder();
        sb.append("[PROFILER-JSON] {");
        sb.append("\"component\":\"").append(escapeJson(componentId)).append("\",");
        sb.append("\"taskId\":").append(taskId).append(',');
        sb.append("\"timestamp\":\"").append(Instant.now()).append("\",");

        // ECALL stats
        sb.append("\"ecalls\":{");
        boolean first = true;
        for (EcallStats stats : ecallStats.values()) {
            if (stats.getSampledCount() == 0 && stats.getTotalCount() == 0) continue;
            if (!first) sb.append(',');
            sb.append('"').append(escapeJson(stats.getName())).append("\":{");
            sb.append("\"total\":").append(stats.getTotalCount()).append(',');
            sb.append("\"sampled\":").append(stats.getSampledCount()).append(',');
            sb.append("\"avgMs\":").append(String.format("%.3f", stats.getAvgNanos() / 1_000_000.0)).append(',');
            sb.append("\"p50Ms\":").append(String.format("%.3f", stats.getPercentile(0.50) / 1_000_000.0)).append(',');
            sb.append("\"p95Ms\":").append(String.format("%.3f", stats.getPercentile(0.95) / 1_000_000.0)).append(',');
            sb.append("\"p99Ms\":").append(String.format("%.3f", stats.getPercentile(0.99) / 1_000_000.0)).append(',');
            sb.append("\"minMs\":").append(String.format("%.3f", stats.getMinNanos() / 1_000_000.0)).append(',');
            sb.append("\"maxMs\":").append(String.format("%.3f", stats.getMaxNanos() / 1_000_000.0));
            sb.append('}');
            first = false;
        }
        sb.append("},");

        // Counters
        sb.append("\"counters\":{");
        first = true;
        for (Map.Entry<String, LongAdder> e : counters.entrySet()) {
            if (!first) sb.append(',');
            sb.append('"').append(escapeJson(e.getKey())).append("\":").append(e.getValue().sum());
            first = false;
        }
        sb.append("},");

        // Gauges
        sb.append("\"gauges\":{");
        first = true;
        for (Map.Entry<String, AtomicLong> e : gauges.entrySet()) {
            if (!first) sb.append(',');
            sb.append('"').append(escapeJson(e.getKey())).append("\":").append(e.getValue().get());
            first = false;
        }
        sb.append("}}");

        return sb.toString();
    }

    /**
     * Writes the CSV header row.
     */
    public static void writeCsvHeader(PrintWriter writer) {
        writer.println("timestamp,component,taskId,type,name,total,sampled,avgMs,minMs,maxMs,p50Ms,p95Ms,p99Ms");
    }

    /**
     * Writes CSV data rows for the current reporting window (no header).
     * Called once per reporting interval so long-running topologies produce continuous output.
     */
    public static void writeCsvRows(PrintWriter writer, String componentId, int taskId,
                                    Map<String, EcallStats> ecallStats,
                                    Map<String, LongAdder> counters,
                                    Map<String, AtomicLong> gauges) {
        String ts = Instant.now().toString();

        // ECALL stats
        for (EcallStats stats : ecallStats.values()) {
            writer.printf("%s,%s,%d,ecall,%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f%n",
                    ts, componentId, taskId, stats.getName(),
                    stats.getTotalCount(), stats.getSampledCount(),
                    stats.getAvgNanos() / 1_000_000.0,
                    stats.getMinNanos() / 1_000_000.0,
                    stats.getMaxNanos() / 1_000_000.0,
                    stats.getPercentile(0.50) / 1_000_000.0,
                    stats.getPercentile(0.95) / 1_000_000.0,
                    stats.getPercentile(0.99) / 1_000_000.0);
        }

        // Counters
        for (Map.Entry<String, LongAdder> e : counters.entrySet()) {
            writer.printf("%s,%s,%d,counter,%s,%d,,,,,,%n",
                    ts, componentId, taskId, e.getKey(), e.getValue().sum());
        }

        // Gauges
        for (Map.Entry<String, AtomicLong> e : gauges.entrySet()) {
            writer.printf("%s,%s,%d,gauge,%s,%d,,,,,,%n",
                    ts, componentId, taskId, e.getKey(), e.getValue().get());
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
