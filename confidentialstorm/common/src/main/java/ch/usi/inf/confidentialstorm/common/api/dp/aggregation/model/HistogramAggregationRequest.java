package ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Request to merge a single encrypted partial histogram into the aggregation round.
 */
public record HistogramAggregationRequest(EncryptedValue encryptedPartialHistogram) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public HistogramAggregationRequest {
        Objects.requireNonNull(encryptedPartialHistogram, "encryptedPartialHistogram cannot be null");
    }
}
