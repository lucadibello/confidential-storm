package ch.usi.inf.examples.confidential_word_count.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.SpoutPreprocessingService;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SpoutPreprocessingRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SpoutPreprocessingResponse;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

/**
 * Re-encrypts spout data with correct AAD routing information
 * for verification and decryption by downstream components.
 * Ensures routing integrity and data confidentiality.
 */
@AutoService(SpoutPreprocessingService.class)
public final class SpoutPreprocessingServiceProvider
        extends ConfidentialBoltService<SpoutPreprocessingRequest>
        implements SpoutPreprocessingService {

    @Override
    public SpoutPreprocessingResponse setupRoute(SpoutPreprocessingRequest request) throws EnclaveServiceException {
        try {
            // Verify request (replay protection skipped as spout data lacks sequence numbers)
            verify(request, false, true);

            // Decrypt and re-encrypt payload and user ID with new routing AAD
            int seq = nextSequenceNumber();
            EncryptedValue reEncryptedPayload = encrypt(decryptToBytes(request.payload()), seq);
            EncryptedValue reEncryptedUserId = encrypt(decryptToBytes(request.userId()), seq);

            // Return preprocessed response
            return new SpoutPreprocessingResponse(reEncryptedPayload, reEncryptedUserId);
        } catch (Throwable t) {
            exceptionCtx.handleException(t);
            return null;
        }
    }

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        // Data originates from synthetic dataset loader not registered in the topology graph.
        return ComponentConstants._DATASET;
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        // Component is the spout itself
        return ComponentConstants.SPOUT_RANDOM_JOKE;
    }
}
