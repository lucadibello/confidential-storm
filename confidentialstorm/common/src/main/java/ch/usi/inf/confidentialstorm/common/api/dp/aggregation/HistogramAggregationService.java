package ch.usi.inf.confidentialstorm.common.api.dp.aggregation;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

/**
 * Service for aggregating partial DP histogram snapshots from multiple DataPerturbation replicas.
 * Each replica produces an encrypted partial histogram on tick; this service decrypts, merges, and
 * returns the complete histogram once all expected partials have been received.
 */
@EnclaveService
public interface HistogramAggregationService {
    HistogramAggregationResponse mergePartial(HistogramAggregationRequest request) throws EnclaveServiceException;
}
