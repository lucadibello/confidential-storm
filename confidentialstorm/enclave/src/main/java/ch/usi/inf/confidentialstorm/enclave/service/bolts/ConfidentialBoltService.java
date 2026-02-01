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
                    exceptionCtx.handleException(e);
                }
            }
        }
        return values;
    }

    private void verifyRoute(Collection<EncryptedValue> values) throws EnclaveServiceException {
        // NOTE: the destination component is the current component!
        TopologySpecification.Component currentComponent = EnclaveConfig.ENABLE_ROUTE_VALIDATION
                ? Objects.requireNonNull(currentComponent(), "Expected destination component cannot be null")
                : null;
        TopologySpecification.Component expectedSource = EnclaveConfig.ENABLE_ROUTE_VALIDATION
                ? expectedSourceComponent()
                : null;

        for (EncryptedValue sealedValue : values) {
            try {
                // NOTE: if the source is null, it means that the value was created outside ConfidentialStorm
                // hence, verifyRoute would verify only the destination component
                sealedPayload.verifyRoute(sealedValue, expectedSource, currentComponent);
            } catch (Exception e) {
                exceptionCtx.handleException(e);
            }
        }
    }

    // NOTE: we assume all encrypted fields in the same request share the same producer and sequence number
    private void verifyReplayProtection(Collection<EncryptedValue> values) throws EnclaveServiceException {
        String producerId = null;
        Long sequence = null;

        for (EncryptedValue sealedValue : values) {
            try {
                // extract AAD and check producer/sequence consistency
                DecodedAAD aad = DecodedAAD.fromBytes(sealedValue.associatedData());
                String currentProducer = aad.producerId().orElseThrow(() ->
                        new SecurityException("AAD missing producer_id"));
                Long currentSeq = aad.sequenceNumber().orElseThrow(() ->
                        new SecurityException("AAD missing sequence number"));

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

        // ensure that we got valid producer/sequence info
        if (producerId == null || sequence == null) {
            exceptionCtx.handleException(new SecurityException("Missing producer/sequence information"));
        }

        // now, check sequence number for replay attacks -> if the sequence number is outside the replay window or
        // has already been seen, we reject the request

        // we create or get the existing replay window for this producer (1 replay window per producer)
        ReplayWindow window = replayWindows.computeIfAbsent(producerId, id -> new ReplayWindow(REPLAY_WINDOW_SIZE));
        if (!window.accept(sequence)) {
            exceptionCtx.handleException(new SecurityException("Replay or out-of-window sequence " + sequence + " for producer " + producerId));
        }
    }

    /**
     * Verifies that all sealed values in the request are valid, come from the expected source
     * and go to the expected destination, and that their sequence numbers are within the replay window.
     *
     * @param request the request containing the sealed values to verify
     * @throws EnclaveServiceException if any verification step fails
     */
    protected void verify(T request) throws EnclaveServiceException {
        // extract all critical values from the request
        Collection<EncryptedValue> values = valuesToVerify(request);

        // if > 0 values to verify, proceed
        if (!values.isEmpty()) {
            if (EnclaveConfig.ENABLE_ROUTE_VALIDATION) verifyRoute(values);
            if (EnclaveConfig.ENABLE_REPLAY_PROTECTION) verifyReplayProtection(values);
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
