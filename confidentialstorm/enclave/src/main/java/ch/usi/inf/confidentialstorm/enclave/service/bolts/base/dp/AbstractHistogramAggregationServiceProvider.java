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
 * Each epoch collects one partial histogram per replica (identified by producerId from AAD).
 * Partials are tagged with an epoch number, allowing the aggregator to handle out-of-order
 * arrivals from replicas that process tick tuples at different rates.
 * <p>
 * Once all expected replicas have contributed for a given epoch, the merged histogram is returned
 * and that epoch's state is cleaned up.
 * <p>
 * The host-side bolt must call {@link #setExpectedReplicaCount(int)} during initialization
 * before any partials are submitted.
 */
public abstract class AbstractHistogramAggregationServiceProvider
    extends ConfidentialBoltService<HistogramAggregationRequest>
    implements HistogramAggregationService
{

    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(
        AbstractHistogramAggregationServiceProvider.class
    );

    /**
     * Maximum number of concurrent epochs to track.
     * Epochs beyond this limit trigger eviction of the oldest epoch with a warning,
     * rather than crashing, to tolerate transient timing skew during startup.
     */
    private static final int MAX_PENDING_EPOCHS = 8;

    /**
     * Per-epoch aggregation state.
     */
    private static final class EpochState {

        final Map<String, Long> mergedHistogram = new HashMap<>();
        final Set<String> contributors = new HashSet<>();
    }

    /**
     * Map of epoch number to its aggregation state.
     * Uses TreeMap so the lowest (oldest) epoch can be efficiently found for eviction.
     */
    private final TreeMap<Integer, EpochState> epochStates = new TreeMap<>();

    /**
     * The number of DataPerturbation replicas whose partials must be merged.
     * Set at runtime via {@link #setExpectedReplicaCount(int)}.
     */
    private int expectedReplicaCount = -1;

    @Override
    public void setExpectedReplicaCount(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException(
                "expectedReplicaCount must be > 0, got: " + count
            );
        }
        this.expectedReplicaCount = count;
        log.info("[AGGREGATION] Expected replica count set to {}", count);
    }

    /**
     * Return the number of DataPerturbation replicas whose partials must be merged
     * before a complete histogram is produced.
     *
     * @return the expected replica count
     */
    protected int getExpectedReplicaCount() {
        if (expectedReplicaCount <= 0) {
            throw new IllegalStateException(
                "expectedReplicaCount not set — setExpectedReplicaCount() must be called before mergePartial()"
            );
        }
        return expectedReplicaCount;
    }

    /**
     * Merges a partial histogram into the aggregation epoch identified by the AAD.
     *
     * @param request the request containing the encrypted partial histogram
     * @return a response indicating if the aggregation is complete or pending
     * @throws EnclaveServiceException if an error occurs during merging
     */
    @Override
    public HistogramAggregationResponse mergePartial(
        HistogramAggregationRequest request
    ) throws EnclaveServiceException {
        try {
            // Validate request
            verify(request);

            // Extract encrypted partial histogram from request
            EncryptedValue partial = request.encryptedPartialHistogram();

            // Extract producer ID and epoch from the AAD of the encrypted partial
            DecodedAAD aad = DecodedAAD.fromBytes(partial.associatedData());
            String senderId = aad
                .producerId()
                .orElseThrow(() ->
                    new SecurityException(
                        "Missing producer_id in partial histogram AAD"
                    )
                );
            int epoch = aad
                .epoch()
                .orElseThrow(() ->
                    new SecurityException(
                        "Missing epoch in partial histogram AAD"
                    )
                );

            // decrypt partial histogram
            Map<String, Object> partialMap = decryptToMap(partial);

            // check if the partial is a dummy (contains the dummy marker key)
            if (partialMap.containsKey(AbstractDataPerturbationServiceProvider.DUMMY_MARKER_KEY)) {
                log.debug(
                    "[AGGREGATION] Dummy partial from producer {} in epoch {} — discarded",
                    senderId,
                    epoch
                );

                // return a dummy response with the same epoch
                return HistogramAggregationResponse.dummy(epoch);
            }

            // Get or create the epoch state (only for real partials)
            EpochState state = epochStates.computeIfAbsent(epoch, k ->
                new EpochState()
            );

            // Evict the oldest epochs if we exceed the pending limit (warn, don't crash)
            while (epochStates.size() > MAX_PENDING_EPOCHS) {
                int oldestEpoch = epochStates.firstKey();
                epochStates.remove(oldestEpoch);
                log.warn(
                    "[AGGREGATION] Evicted stale epoch {} (pending epochs exceeded {}). "
                    + "Data for that epoch is lost, but the system continues.",
                    oldestEpoch,
                    MAX_PENDING_EPOCHS
                );
            }

            // Reject duplicate contributions from the same producer in the same epoch
            if (state.contributors.contains(senderId)) {
                log.warn(
                    "[AGGREGATION] Duplicate partial from producer {} in epoch {}, ignoring",
                    senderId,
                    epoch
                );

                // Return pending response without merging to avoid double-counting
                return HistogramAggregationResponse.pending(
                    state.contributors.size(),
                    getExpectedReplicaCount()
                );
            }

            // Merge real partial into epoch accumulator
            for (Map.Entry<String, Object> entry : partialMap.entrySet()) {
                long value = ((Number) entry.getValue()).longValue();
                state.mergedHistogram.merge(entry.getKey(), value, Long::sum);
            }
            state.contributors.add(senderId);

            log.info(
                "[AGGREGATION] Received partial from producer {} ({}/{} in epoch {})",
                senderId,
                state.contributors.size(),
                getExpectedReplicaCount(),
                epoch
            );

            // Check if all replicas have contributed for this epoch
            if (state.contributors.size() >= getExpectedReplicaCount()) {
                // Capture the completed histogram
                Map<String, Long> result = new HashMap<>(state.mergedHistogram);

                // Remove completed epoch
                epochStates.remove(epoch);

                log.info(
                    "[AGGREGATION] Epoch {} complete with {} keys",
                    epoch,
                    result.size()
                );

                // Return the complete histogram for this epoch
                return HistogramAggregationResponse.complete(
                    result,
                    getExpectedReplicaCount(),
                    epoch
                );
            }

            return HistogramAggregationResponse.pending(
                state.contributors.size(),
                getExpectedReplicaCount()
            );
        } catch (Throwable t) {
            exceptionCtx.handleException(t);
            return null;
        }
    }
}
