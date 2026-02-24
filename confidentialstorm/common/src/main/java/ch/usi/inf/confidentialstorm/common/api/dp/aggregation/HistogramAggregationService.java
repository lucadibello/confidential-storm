package ch.usi.inf.confidentialstorm.common.api.dp.aggregation;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

/**
 * Service for aggregating partial DP histogram snapshots from multiple DataPerturbation replicas.
 * Each replica produces an encrypted partial histogram on tick; this service decrypts, merges, and
 * returns the complete histogram once all expected partials have been received.
 * <p>
 * The host-side bolt must call {@link #setExpectedReplicaCount(int)} during initialization
 * before any partials are submitted.
 */
@EnclaveService
public interface HistogramAggregationService {

    /**
     * Sets the number of DataPerturbation replicas whose partials must be merged
     * before a complete histogram is produced.
     * <p>
     * Must be called once during bolt initialization (before any {@link #mergePartial} calls).
     *
     * @param count the expected replica count (must be &gt; 0)
     */
    void setExpectedReplicaCount(int count);

    HistogramAggregationResponse mergePartial(HistogramAggregationRequest request) throws EnclaveServiceException;
}
