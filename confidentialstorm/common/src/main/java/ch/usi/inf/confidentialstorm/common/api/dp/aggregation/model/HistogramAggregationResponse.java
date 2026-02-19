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
 */
public record HistogramAggregationResponse(
        boolean complete,
        @Nullable Map<String, Long> mergedHistogram,
        int receivedCount,
        int expectedCount
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Creates a pending response (still waiting for more partials).
     *
     * @param received the number of partials received
     * @param expected the total number of partials expected
     * @return the pending response
     */
    public static HistogramAggregationResponse pending(int received, int expected) {
        return new HistogramAggregationResponse(false, null, received, expected);
    }

    /**
     * Creates a complete response with the merged histogram.
     *
     * @param histogram the merged histogram
     * @param expected  the total number of partials expected
     * @return the complete response
     */
    public static HistogramAggregationResponse complete(Map<String, Long> histogram, int expected) {
        return new HistogramAggregationResponse(true, histogram, expected, expected);
    }
}
