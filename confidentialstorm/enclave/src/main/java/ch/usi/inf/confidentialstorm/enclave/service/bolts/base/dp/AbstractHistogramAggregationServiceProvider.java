package ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.HistogramAggregationService;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

import java.util.*;

/**
 * Enclave service that aggregates encrypted partial histograms from multiple DataPerturbation replicas.
 * <p>
 * Each round collects one partial histogram per replica (identified by producerId from AAD).
 * Once all expected replicas have contributed, the merged histogram is returned and state is reset.
 */
public abstract class AbstractHistogramAggregationServiceProvider
        extends ConfidentialBoltService<HistogramAggregationRequest>
        implements HistogramAggregationService {

    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(AbstractHistogramAggregationServiceProvider.class);

    private final Map<String, Long> currentMergedHistogram = new HashMap<>();
    private final Set<String> currentRoundContributors = new HashSet<>();
    private int roundNumber = 0;

    /**
     * Return the number of DataPerturbation replicas whose partials must be merged
     * before a complete histogram is produced.
     */
    protected abstract int getExpectedReplicaCount();

    @Override
    public HistogramAggregationResponse mergePartial(HistogramAggregationRequest request) throws EnclaveServiceException {
        try {
            // Validate route but skip replay protection (multiple producers with independent sequences)
            verify(request);

            EncryptedValue partial = request.encryptedPartialHistogram();

            // Extract producer ID from the AAD of the encrypted partial
            DecodedAAD aad = DecodedAAD.fromBytes(partial.associatedData());
            String senderId = aad.producerId().orElseThrow(
                    () -> new SecurityException("Missing producer_id in partial histogram AAD"));

            // Reject duplicate contributions from the same producer in the current round
            if (currentRoundContributors.contains(senderId)) {
                log.warn("[AGGREGATION] Duplicate partial from producer {} in round {}, ignoring", senderId, roundNumber);
                return HistogramAggregationResponse.pending(currentRoundContributors.size(), getExpectedReplicaCount());
            }

            // Decrypt partial histogram inside enclave
            Map<String, Object> partialMap = decryptToMap(partial);

            // Merge into accumulator
            for (Map.Entry<String, Object> entry : partialMap.entrySet()) {
                long value = ((Number) entry.getValue()).longValue();
                currentMergedHistogram.merge(entry.getKey(), value, Long::sum);
            }
            currentRoundContributors.add(senderId);

            log.debug("[AGGREGATION] Received partial from producer {} ({}/{} in round {})",
                    senderId, currentRoundContributors.size(), getExpectedReplicaCount(), roundNumber);

            // Check if all replicas have contributed
            if (currentRoundContributors.size() >= getExpectedReplicaCount()) {
                // Capture the completed histogram
                Map<String, Long> result = new HashMap<>(currentMergedHistogram);

                // Reset state for next round
                currentMergedHistogram.clear();
                currentRoundContributors.clear();
                roundNumber++;

                log.debug("[AGGREGATION] Round {} complete with {} keys", roundNumber - 1, result.size());
                return HistogramAggregationResponse.complete(result, getExpectedReplicaCount());
            }

            return HistogramAggregationResponse.pending(currentRoundContributors.size(), getExpectedReplicaCount());
        } catch (Throwable t) {
            exceptionCtx.handleException(t);
            return null;
        }
    }
}
