package ch.usi.inf.confidentialstorm.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.api.model.base.IServiceMessage;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.enclave.exception.EnclaveExceptionContext;
import ch.usi.inf.confidentialstorm.enclave.service.model.ReplayWindow;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ConfidentialBoltService<T extends IServiceMessage> {
    /**
     * Zero-depencency logger for the enclave services.
     */
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(ConfidentialBoltService.class);

    protected final EnclaveExceptionContext exceptionCtx = EnclaveExceptionContext.getInstance();

    /**
     * Size of the replay window for sequence number tracking (should be large enough to accommodate out-of-order messages).
     */
    private final int REPLAY_WINDOW_SIZE = 128;

    /**
     * Map of producer IDs to their corresponding replay windows for replay attack prevention.
     * NOTE: we use a map of replay windows as one bolt could ingest data streams from multiple different producers.
     */
    private final Map<String, ReplayWindow> replayWindows = new ConcurrentHashMap<>();
    protected final SealedPayload sealedPayload;

    protected ConfidentialBoltService() {
        this(SealedPayload.fromConfig());
    }

    protected ConfidentialBoltService(SealedPayload sealedPayload) {
        this.sealedPayload = Objects.requireNonNull(sealedPayload, "sealedPayload cannot be null");
    }

    /**
     * Get the expected source component for the sealed values in the request.
     * @return the expected source component
     */
    public abstract TopologySpecification.Component expectedSourceComponent();

    /**
     * Get the expected destination component for the sealed values in the request.
     * @return the expected destination component
     */
    public abstract TopologySpecification.Component expectedDestinationComponent();

    /**
     * Extract all sealed/encrypted values from the request that need to be verified.
     * @param request the request containing the sealed values
     * @return a collection of sealed/encrypted values to verify
     */
    public abstract Collection<EncryptedValue> valuesToVerify(T request);

    /**
     * Verify that all sealed values in the request are valid, come from the expected source to the expected destination,
     * and that their sequence numbers are within the replay window.
     * @param request the request containing the sealed values to verify
     * @throws EnclaveServiceException if any verification step fails
     */
    protected void verify(T request) throws EnclaveServiceException {
        if (!EnclaveConfig.ENABLE_ROUTE_VALIDATION && !EnclaveConfig.ENABLE_REPLAY_PROTECTION) {
            log.debug("Route validation and replay protection disabled; skipping verification.");
            return;
        }

        // extract all critical values from the request
        Collection<EncryptedValue> values = valuesToVerify(request);

        TopologySpecification.Component destination = EnclaveConfig.ENABLE_ROUTE_VALIDATION
                ? Objects.requireNonNull(expectedDestinationComponent(), "Expected destination component cannot be null")
                : null;
        TopologySpecification.Component expectedSource = EnclaveConfig.ENABLE_ROUTE_VALIDATION
                ? expectedSourceComponent()
                : null;

        String producerId = null;
        Long sequence = null;

        // verify each value
        for (EncryptedValue sealedValue : values) {
            if (EnclaveConfig.ENABLE_ROUTE_VALIDATION) {
                try {
                    // NOTE: if the source is null, it means that the value was created outside of ConfidentialStorm
                    // hence, verifyRoute would verify only the destination component
                    sealedPayload.verifyRoute(sealedValue, expectedSource, destination);
                } catch (Exception e) {
                    exceptionCtx.handleException(e);
                    continue;
                }
            }

            if (!EnclaveConfig.ENABLE_REPLAY_PROTECTION) {
                continue;
            }

            try {
                // extract AAD and check producer/sequence consistency
                DecodedAAD aad = DecodedAAD.fromBytes(sealedValue.associatedData());
                String currentProducer = aad.producerId().orElseThrow(() ->
                        new SecurityException("AAD missing producer_id"));
                Long currentSeq = aad.sequenceNumber().orElseThrow(() ->
                        new SecurityException("AAD missing sequence number"));

                // NOTE: we assume all encrypted fields in the same request share the same producer and sequence number

                // get first available producer/sequence tuple and ensure all values have the same
                if (producerId == null && sequence == null) {
                    producerId = currentProducer;
                    sequence = currentSeq;
                } else if (!Objects.equals(producerId, currentProducer) || !Objects.equals(sequence, currentSeq)) {
                    exceptionCtx.handleException(
                            new SecurityException("Mismatch between AAD producer/sequence across encrypted fields")
                    );
                }
            } catch (Exception e) {
                exceptionCtx.handleException(e);
            }
        }

        if (!EnclaveConfig.ENABLE_REPLAY_PROTECTION) {
            return;
        }

        // ensure that we got valid producer/sequence info
        if (producerId == null || sequence == null) {
            exceptionCtx.handleException(new SecurityException("Missing producer/sequence information"));
            return;
        }

        // now, check sequence number for replay attacks -> if the sequence number is outside of the replay window or
        // has already been seen, we reject the request

        // we create or get the existing replay window for this producer (1 replay window per producer)
        ReplayWindow window = replayWindows.computeIfAbsent(producerId, id -> new ReplayWindow(REPLAY_WINDOW_SIZE));
        if (!window.accept(sequence)) {
            exceptionCtx.handleException(new SecurityException("Replay or out-of-window sequence " + sequence + " for producer " + producerId));
        }
    }
}
