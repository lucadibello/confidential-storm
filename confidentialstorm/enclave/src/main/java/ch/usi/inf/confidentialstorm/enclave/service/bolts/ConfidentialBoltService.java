package ch.usi.inf.confidentialstorm.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecificationBuilder;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.enclave.exception.EnclaveExceptionContext;
import ch.usi.inf.confidentialstorm.enclave.security.ReplayWindow;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveJsonUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

import java.util.*;

public abstract class ConfidentialBoltService<T> {

    /**
     * The exception context for handling exceptions within the enclave.
     */
    protected final EnclaveExceptionContext exceptionCtx = EnclaveExceptionContext.getInstance();

    /**
     * The sealed payload handler for encrypting and decrypting data.
     */
    private final SealedPayload sealedPayload;

    /**
     * Unique identifier for this producer instance.
     */
    protected final String producerId = UUID.randomUUID().toString();

    /**
     * The logger for this class.
     */
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(ConfidentialBoltService.class);

    /**
     * Size of the replay window for sequence number tracking (should be large enough to accommodate out-of-order messages).
     * <p>
     * FIXME: is it really possible to have out-of-order messages in Storm? If not, we could reduce this to 1.
     */
    private final int REPLAY_WINDOW_SIZE = 128;

    /**
     * Map of producer IDs to their corresponding replay windows for replay attack prevention.
     * NOTE: we use a map of replay windows as one bolt could ingest data streams from multiple different producers.
     */
    private final Map<String, ReplayWindow> replayWindows = new HashMap<>();

    /**
     * Sequence number generator for this bolt instance.
     */
    private int sequenceNumber = 0;

    /**
     * Get the next sequence number for this bolt instance in a thread-safe manner.
     *
     * @return the next sequence number
     */
    protected synchronized int nextSequenceNumber() {
        return ++sequenceNumber;
    }

    protected ConfidentialBoltService() {
        this(SealedPayload.fromConfig());
    }

    protected ConfidentialBoltService(SealedPayload sealedPayload) {
        this.sealedPayload = Objects.requireNonNull(sealedPayload, "sealedPayload cannot be null");
    }

    /**
     * Get the expected source component for the sealed values in the request.
     *
     * @return the expected source component
     */
    public abstract TopologySpecification.Component expectedSourceComponent();

    /**
     * Get the expected destination component for the sealed values in the request.
     *
     * @return the expected destination component
     */
    public abstract TopologySpecification.Component expectedDestinationComponent();

    /**
     * Get the current component (i.e., this bolt) in the topology.
     *
     * @return the current component
     */
    public abstract TopologySpecification.Component currentComponent();

    /**
     * Extract all sealed/encrypted values from the request that need to be verified.
     *
     * @param request the request containing the sealed values
     * @return a collection of sealed/encrypted values to verify
     */
    public Collection<EncryptedValue> valuesToVerify(T request) throws EnclaveServiceException {
        // Use reflection to find all EncryptedValue fields in the request object
        List<EncryptedValue> values = new ArrayList<>();
        for (var field : request.getClass().getDeclaredFields()) {
            if (EncryptedValue.class.isAssignableFrom(field.getType())) {
                field.setAccessible(true);
                try {
                    EncryptedValue value = (EncryptedValue) field.get(request);
                    if (value != null) {
                        values.add(value);
                    }
                } catch (IllegalAccessException e) {
                    throw new EnclaveServiceException("Failed to access field " + field.getName(), e);
                }
            }
        }
        return values;
    }

    private boolean isTupleRoutedCorrectly(EncryptedValue value) {
        return sealedPayload.isRouteValid(value, expectedSourceComponent(), currentComponent());
    }

    // NOTE: we assume all encrypted fields in the same request share the same producer and sequence number
    private boolean isSequenceNotReplay(String producerId, long sequenceNumber) {
        // we create or get the existing replay window for this producer (1 replay window per producer)
        return replayWindows
                .computeIfAbsent(producerId, id -> new ReplayWindow(REPLAY_WINDOW_SIZE))
                .accept(sequenceNumber);
    }

    protected void verify(T request) throws EnclaveServiceException {
        verify(request, false, false);
    }

    /**
     * Verifies that all sealed values in the request are valid, come from the expected source
     * and go to the expected destination, and that their sequence numbers are within the replay window.
     *
     * @param request the request containing the sealed values to verify
     * @throws EnclaveServiceException if any verification step fails
     */
    protected void verify(T request, boolean skipRouteValidation, boolean skipReplayProtection)
            throws EnclaveServiceException {
        Collection<EncryptedValue> values = valuesToVerify(request);

        if (!values.isEmpty()) {

            // we use these variables to track the producer ID and sequence number across all values in the request to ensure consistency
            boolean set = false;
            String producerId = null;
            long sequenceNumber = -1L;

            for (EncryptedValue value : values) {
                if (!skipRouteValidation) {
                    if (EnclaveConfig.ENABLE_ROUTE_VALIDATION) {
                        if (!isTupleRoutedCorrectly(value)) {
                            DecodedAAD aad = DecodedAAD.fromBytes(value.associatedData());
                            log.error("Tuple routing validation failed on component {} for value with producer_id={}, seq={}",
                                    currentComponent().getName(),
                                    aad.producerId().orElse("unknown"),
                                    aad.sequenceNumber().orElse(-1L));

                            // throw a SecurityException with details about the expected vs actual routing information
                            throw new SecurityException(String.format(
                                    "Tuple routing validation failed on component %s | Expected: [%s -> %s], Actual: [%s -> %s]",
                                    currentComponent(),
                                    expectedSourceComponent(),
                                    currentComponent(),
                                    aad.sourceName().orElse("unknown"),
                                    aad.destinationName().orElse("unknown")
                            ));                        }
                    }
                }
                if (!skipReplayProtection) {
                    if (EnclaveConfig.ENABLE_REPLAY_PROTECTION) {
                        // extract metadata from AAD of encrypted value
                        DecodedAAD aad = DecodedAAD.fromBytes(value.associatedData());
                        Optional<String> optProducerId = Objects.requireNonNull(aad.producerId(), "AAD missing producer_id");
                        Optional<Long> optSequenceNumber = Objects.requireNonNull(aad.sequenceNumber(), "AAD missing sequence number");
                        assert optProducerId.isPresent() && optSequenceNumber.isPresent() : "AAD must contain producer_id and sequence number";

                        // ensure that all values within the same request share the same producer ID and sequence number
                        if (!set) {
                            producerId = optProducerId.get();
                            sequenceNumber = optSequenceNumber.get();

                            // set flag to indicate that we've extracted producer/sequence info from the first value
                            set = true;
                        }
                        // if already set, check consistency with the current value's AAD
                        else {
                            if (!producerId.equals(optProducerId.get()) || sequenceNumber != optSequenceNumber.get()) {
                                log.error("Inconsistent producer_id or sequence number across encrypted values in the same request on component {}. Expected producer_id={}, seq={}; Found producer_id={}, seq={}",
                                        currentComponent().getName(), producerId, sequenceNumber, optProducerId.get(), optSequenceNumber.get());
                                throw new SecurityException(String.format(
                                        "Inconsistent producer_id or sequence number across encrypted values in the same request on component %s. Expected producer_id=%s, seq=%d; Found producer_id=%s, seq=%d",
                                        currentComponent().getName(), producerId, sequenceNumber, optProducerId.get(), optSequenceNumber.get()));
                            }
                        }
                    }
                }
            }

            assert set : "Producer ID and sequence number should have been set from at least one of the encrypted values in the request";

            // if replay protection is enabled, check that the shared sequence number is within the replay window for the producer
            if (!skipReplayProtection) {
                if (EnclaveConfig.ENABLE_REPLAY_PROTECTION && set) {
                    if (!isSequenceNotReplay(producerId, sequenceNumber)) {
                        log.error("Replay attack detected on component {} for producer_id={}, seq={}",
                                currentComponent().getName(), producerId, sequenceNumber);
                        throw new SecurityException(String.format(
                                "Replay attack detected for component %s for producer_id=%s, seq=%d",
                                currentComponent().getName(), producerId, sequenceNumber));
                    }
                }
            }
        }
    }

    private AADSpecification getAADSpecification(int sequenceNumber) {

        AADSpecificationBuilder aadBuilder = AADSpecification.builder();

        // if route validation is enabled, include source and destination components in the AAD
        if (EnclaveConfig.ENABLE_ROUTE_VALIDATION) {
            aadBuilder
                    .sourceComponent(Objects.requireNonNull(currentComponent(), "Current component cannot be null"))
                    .destinationComponent(expectedDestinationComponent());
        }
        // if replay protection is enabled, include producer ID and sequence number in the AAD
        if (EnclaveConfig.ENABLE_REPLAY_PROTECTION) {
            aadBuilder
                    .put("producer_id", producerId)
                    .put("seq", sequenceNumber);
        }

        // build the AAD specification with the required fields
        return aadBuilder.build();
    }

    protected EncryptedValue encrypt(Map<String, Object> plaintext, int sequenceNumber) throws SealedPayloadProcessingException, AADEncodingException, CipherInitializationException {
        byte[] plaintextPayload = EnclaveJsonUtil.serialize(plaintext);
        AADSpecification aadSpec = getAADSpecification(sequenceNumber);
        return sealedPayload.encrypt(plaintextPayload, aadSpec);
    }

    protected EncryptedValue encrypt(double plaintext, int sequenceNumber) throws SealedPayloadProcessingException, AADEncodingException, CipherInitializationException {
        byte[] plaintextPayload = EnclaveJsonUtil.serialize(plaintext);
        AADSpecification aadSpec = getAADSpecification(sequenceNumber);
        return sealedPayload.encrypt(plaintextPayload, aadSpec);
    }

    protected EncryptedValue encrypt(long plaintext, int sequenceNumber) throws SealedPayloadProcessingException, AADEncodingException, CipherInitializationException {
        byte[] plaintextPayload = EnclaveJsonUtil.serialize(plaintext);
        AADSpecification aadSpec = getAADSpecification(sequenceNumber);
        return sealedPayload.encrypt(plaintextPayload, aadSpec);
    }

    protected EncryptedValue encrypt(byte[] plaintext, int sequenceNumber) throws SealedPayloadProcessingException, AADEncodingException, CipherInitializationException {
        AADSpecification aadSpec = getAADSpecification(sequenceNumber);
        return sealedPayload.encrypt(plaintext, aadSpec);
    }

    protected EncryptedValue encrypt(String plaintext, int sequenceNumber) throws SealedPayloadProcessingException, AADEncodingException, CipherInitializationException {
        byte[] plaintextPayload = EnclaveJsonUtil.serialize(plaintext);
        AADSpecification aadSpec = getAADSpecification(sequenceNumber);
        return sealedPayload.encrypt(plaintextPayload, aadSpec);
    }

    protected byte[] decryptToBytes(EncryptedValue sealedValue) throws SealedPayloadProcessingException, CipherInitializationException {
        return sealedValue == null ? null : sealedPayload.decrypt(sealedValue);
    }

    protected String decryptToString(EncryptedValue sealedValue) throws SealedPayloadProcessingException, CipherInitializationException {
        return sealedValue == null ? null :  sealedPayload.decryptToString(sealedValue);
    }

    protected Double decryptToDouble(EncryptedValue sealedValue) throws SealedPayloadProcessingException, CipherInitializationException {
        String json_payload = sealedPayload.decryptToString(sealedValue);
        return Double.valueOf(json_payload);
    }

    protected Long decryptToLong(EncryptedValue sealedValue) throws SealedPayloadProcessingException, CipherInitializationException {
        String json_payload = sealedPayload.decryptToString(sealedValue);
        return Long.valueOf(json_payload);
    }

    protected Map<String, Object> decryptToMap(EncryptedValue sealedValue) throws SealedPayloadProcessingException, CipherInitializationException {
        // decrypt the payload
        String json_payload = sealedPayload.decryptToString(sealedValue);
        // reconstruct the map from the JSON
        return EnclaveJsonUtil.parseJson(json_payload);
    }

}
