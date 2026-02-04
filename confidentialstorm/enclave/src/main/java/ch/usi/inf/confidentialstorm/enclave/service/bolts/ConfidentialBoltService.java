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
    private static int sequenceNumber = 0;

    /**
     * Get the next sequence number for this bolt instance in a thread-safe manner.
     *
     * @return the next sequence number
     */
    protected synchronized int nextSequenceNumber() {
        return sequenceNumber++;
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
    private boolean isTupleNotReplay(EncryptedValue value) {
        // extract AAD and check producer/sequence consistency
        DecodedAAD aad = DecodedAAD.fromBytes(value.associatedData());
        String producerId = aad.producerId().orElseThrow(() ->
                new SecurityException("AAD missing producer_id"));
        Long sequence = aad.sequenceNumber().orElseThrow(() ->
                new SecurityException("AAD missing sequence number"));

        // now, check sequence number for replay attacks -> if the sequence number is outside the replay window or
        // has already been seen, we reject the request

        // we create or get the existing replay window for this producer (1 replay window per producer)
        return replayWindows
                .computeIfAbsent(producerId, id -> new ReplayWindow(REPLAY_WINDOW_SIZE))
                .accept(sequence);
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
        // extract all critical values from the request
        Collection<EncryptedValue> values = valuesToVerify(request);

        // if > 0 values to verify, proceed
        if (!values.isEmpty()) {
            for (EncryptedValue value : values) {
                if (skipRouteValidation) {
                    if (EnclaveConfig.ENABLE_ROUTE_VALIDATION) {
                        if (!isTupleRoutedCorrectly(value)) {
                            log.error("Tuple routing validation failed for value with producer_id={}, seq={}",
                                    DecodedAAD.fromBytes(value.associatedData()).producerId().orElse("unknown"),
                                    DecodedAAD.fromBytes(value.associatedData()).sequenceNumber().orElse(-1L));
                            throw new SecurityException("Tuple routing validation failed");
                        }
                    }
                }
                if (skipReplayProtection) {
                    if (EnclaveConfig.ENABLE_REPLAY_PROTECTION) {
                        if (!isTupleNotReplay(value)) {
                            log.error("Replay attack detected for value with producer_id={}, seq={}",
                                    DecodedAAD.fromBytes(value.associatedData()).producerId().orElse("unknown"),
                                    DecodedAAD.fromBytes(value.associatedData()).sequenceNumber().orElse(-1L));
                            throw new SecurityException("Replay attack detected");
                        }
                    }
                }
            }
        }
    }

    private AADSpecification getAADSpecification(int sequenceNumber) {
        return AADSpecification.builder()
                .sourceComponent(Objects.requireNonNull(currentComponent(), "Current component cannot be null"))
                .destinationComponent(expectedDestinationComponent())
                .put("producer_id", producerId)
                .put("seq", sequenceNumber)
                .build();
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
