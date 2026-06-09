package ch.usi.inf.examples.microbatch_dp.host.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runtime configuration for the confidential micro-batch benchmark.
 *
 * <p>The encrypted-record payload is materially larger than the baseline
 * plaintext tuple, so {@link #DEFAULT_BYTES_PER_TUPLE} is calibrated upwards
 * accordingly. Override via {@code --bytes-per-tuple} on the topology CLI
 * when you have measured the actual per-tuple cost.
 */
public final class MicroBatchConfig {

    public static final String CONF_BATCH_SIZES_GB = "microbatch.sizes.gb";
    public static final String CONF_RUNS_PER_SIZE = "microbatch.runs.per.size";
    public static final String CONF_BYTES_PER_TUPLE = "microbatch.bytes.per.tuple";
    public static final String CONF_COMPLETION_TIMEOUT_MS = "microbatch.completion.timeout.ms";

    /**
     * Serialized footprint of one <em>baseline</em> data tuple as measured by
     * the baseline-side {@code TupleSizeProbe} (mean 30.93 B, p99 33 B).
     * <p>The confidential variant intentionally uses the same value as the
     * baseline so that the GB&rarr;records conversion produces identical
     * record counts in both pipelines &mdash; the comparison is matched on
     * logical workload (tuples), not on on-wire byte budget. The encrypted
     * record is materially larger than 31 B on the wire, but that overhead is
     * what the benchmark measures, not what gates the batch size.
     */
    public static final long DEFAULT_BYTES_PER_TUPLE = 31L;
    public static final double[] DEFAULT_SIZES_GB = { 1.0, 2.0, 5.0 };
    public static final int DEFAULT_RUNS_PER_SIZE = 3;
    public static final long DEFAULT_COMPLETION_TIMEOUT_MS = 30L * 60L * 1000L;

    private MicroBatchConfig() {}

    public static double[] parseSizesGb(String csv) {
        String[] parts = csv.split(",");
        double[] out = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Double.parseDouble(parts[i].trim());
        }
        return out;
    }

    public static long recordCountForBytes(double sizeGb, long bytesPerTuple) {
        double totalBytes = sizeGb * 1024.0 * 1024.0 * 1024.0;
        return Math.max(1L, (long) Math.ceil(totalBytes / (double) bytesPerTuple));
    }

    @SuppressWarnings("unchecked")
    public static double[] sizesGb(Map<String, Object> conf) {
        Object v = conf.get(CONF_BATCH_SIZES_GB);
        if (v == null) return DEFAULT_SIZES_GB;
        if (v instanceof List) {
            List<Object> l = (List<Object>) v;
            double[] out = new double[l.size()];
            for (int i = 0; i < l.size(); i++) out[i] = ((Number) l.get(i)).doubleValue();
            return out;
        }
        return parseSizesGb(v.toString());
    }

    public static int runsPerSize(Map<String, Object> conf) {
        Object v = conf.get(CONF_RUNS_PER_SIZE);
        return v == null ? DEFAULT_RUNS_PER_SIZE : ((Number) v).intValue();
    }

    public static long bytesPerTuple(Map<String, Object> conf) {
        Object v = conf.get(CONF_BYTES_PER_TUPLE);
        return v == null ? DEFAULT_BYTES_PER_TUPLE : ((Number) v).longValue();
    }

    public static long completionTimeoutMs(Map<String, Object> conf) {
        Object v = conf.get(CONF_COMPLETION_TIMEOUT_MS);
        return v == null ? DEFAULT_COMPLETION_TIMEOUT_MS : ((Number) v).longValue();
    }

    public static List<BatchPlan> buildPlan(double[] sizesGb, int runsPerSize, long bytesPerTuple) {
        List<BatchPlan> plan = new ArrayList<>();
        int batchId = 0;
        for (double sizeGb : sizesGb) {
            long records = recordCountForBytes(sizeGb, bytesPerTuple);
            for (int r = 0; r < runsPerSize; r++) {
                plan.add(new BatchPlan(batchId++, sizeGb, records, r));
            }
        }
        return plan;
    }

    public record BatchPlan(int batchId, double sizeGb, long recordCount, int runIndex) {}
}
