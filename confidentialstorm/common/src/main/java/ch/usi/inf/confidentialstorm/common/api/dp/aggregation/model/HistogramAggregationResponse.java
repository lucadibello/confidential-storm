package ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model;

import javax.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * Response from the histogram aggregation service.
 * When all expected partial histograms have been received, {@code complete} is true
 * and {@code mergedHistogram} contains the global aggregated result.
 *
 * @param complete        true if all expected partials have been received
 * @param mergedHistogram the global aggregated histogram (only if complete is true)
 * @param receivedCount   the number of partials received so far
 * @param expectedCount   the total number of partials expected
 * @param completedEpoch  the epoch number that was completed (-1 if pending)
 * @param isDummy         true if the partial was a dummy and was silently discarded
 */
public record HistogramAggregationResponse(
        boolean complete,
        @Nullable Map<String, Long> mergedHistogram,
        int receivedCount,
        int expectedCount,
        int completedEpoch,
        boolean isDummy
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    /**
     * Creates a pending response (still waiting for more partials).
     *
     * @param received the number of partials received
     * @param expected the total number of partials expected
     * @return the pending response
     */
    public static HistogramAggregationResponse pending(int received, int expected) {
        return new HistogramAggregationResponse(false, null, received, expected, -1, false);
    }

    /**
     * Creates a complete response with the merged histogram.
     *
     * @param histogram the merged histogram
     * @param expected  the total number of partials expected
     * @param epoch     the epoch number that was completed
     * @return the complete response
     */
    public static HistogramAggregationResponse complete(Map<String, Long> histogram, int expected, int epoch) {
        return new HistogramAggregationResponse(true, histogram, expected, expected, epoch, false);
    }

    /**
     * Creates a dummy response indicating the partial was a dummy and was discarded
     * by the enclave aggregator. No data was merged.
     *
     * @param epoch the epoch the dummy was tagged with
     * @return a dummy response
     */
    public static HistogramAggregationResponse dummy(int epoch) {
        return new HistogramAggregationResponse(false, null, 0, 0, epoch, true);
    }
}
