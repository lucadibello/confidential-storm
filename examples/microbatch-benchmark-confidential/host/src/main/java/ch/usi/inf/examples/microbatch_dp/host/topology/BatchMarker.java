package ch.usi.inf.examples.microbatch_dp.host.topology;

import org.apache.storm.tuple.Fields;

/**
 * Field layout and helpers for control-stream tuples used by the micro-batch
 * benchmark.
 *
 * <p>A control tuple is one of two types:
 * <ul>
 *   <li>{@link #BEGIN}: emitted by the spout when it starts streaming a batch.
 *       Carries the spout-side wall-clock start timestamp.</li>
 *   <li>{@link #END}: emitted by the spout once the entire batch has been
 *       handed to Storm. Triggers the DP snapshot and the aggregator's stop
 *       timestamp.</li>
 * </ul>
 *
 * <p>Control tuples flow on a separate {@link ComponentConstants#CONTROL_STREAM}
 * stream subscribed via allGrouping, so every replica of every downstream bolt
 * sees every marker exactly once per upstream task.
 */
public final class BatchMarker {

    public static final String BEGIN = "BEGIN";
    public static final String END = "END";

    public static final String F_TYPE = "type";
    public static final String F_BATCH_ID = "batchId";
    public static final String F_SIZE_GB = "sizeGB";
    public static final String F_RECORD_COUNT = "recordCount";
    public static final String F_BYTES_PER_TUPLE = "bytesPerTuple";
    public static final String F_T_NANOS = "tNanos";
    public static final String F_T_EPOCH_MS = "tEpochMs";

    public static Fields fields() {
        return new Fields(F_TYPE, F_BATCH_ID, F_SIZE_GB,
                F_RECORD_COUNT, F_BYTES_PER_TUPLE,
                F_T_NANOS, F_T_EPOCH_MS);
    }

    private BatchMarker() {}
}
