package ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model;

import javax.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * Response from the histogram aggregation service.
 * When all expected partial histograms have been received, {@code complete} is true
 * and {@code mergedHistogram} contains the global aggregated result.
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
     */
    public static HistogramAggregationResponse pending(int received, int expected) {
        return new HistogramAggregationResponse(false, null, received, expected);
    }

    /**
     * Creates a complete response with the merged histogram.
     */
    public static HistogramAggregationResponse complete(Map<String, Long> histogram, int expected) {
        return new HistogramAggregationResponse(true, histogram, expected, expected);
    }
}
