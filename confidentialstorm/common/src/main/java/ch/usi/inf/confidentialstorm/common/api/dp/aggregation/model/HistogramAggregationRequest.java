package ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Request to merge a single encrypted partial histogram into the aggregation round.
 *
 * @param encryptedPartialHistogram the encrypted partial histogram snapshot
 */
public record HistogramAggregationRequest(EncryptedValue encryptedPartialHistogram) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new HistogramAggregationRequest.
     *
     * @param encryptedPartialHistogram the encrypted partial histogram snapshot
     * @throws NullPointerException if encryptedPartialHistogram is null
     */
    public HistogramAggregationRequest {
        Objects.requireNonNull(encryptedPartialHistogram, "encryptedPartialHistogram cannot be null");
    }
}
