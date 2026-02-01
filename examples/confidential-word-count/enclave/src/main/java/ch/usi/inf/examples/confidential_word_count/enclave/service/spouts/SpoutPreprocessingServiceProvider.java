package ch.usi.inf.examples.confidential_word_count.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.SpoutPreprocessingService;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SpoutRouterRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SpoutRouterResponse;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

/**
 * The SpoutRouterServiceProvider is responsible to re-encrypt the data coming from the spout with the
 * correct AAD routing information so that it can be correctly verified and decrypted by the downstream component.
 * <p>
 * This implementation allows to ensure routing integrity and confidentiality of the data as it moves through
 * the topology.
 * <p>
 * NOTE: If we disable routing verification, this is not needed.
 * (refer to @link{ch.usi.inf.examples.confidential_word_count.enclave.WordCountEnclaveConfigProvider#isRouteValidationEnabled()}
 */
@AutoService(SpoutPreprocessingService.class)
public final class SpoutPreprocessingServiceProvider
        extends ConfidentialBoltService<SpoutRouterRequest>
        implements SpoutPreprocessingService {

    @Override
    public SpoutRouterResponse setupRoute(SpoutRouterRequest request) throws EnclaveServiceException {
        try {
            // ensure request is valid
            // NOTE: verify(request);

            // Decrypt and re-encrypt payload / user_id with new AAD routing information
            int seq = nextSequenceNumber();
            EncryptedValue reEncryptedPayload = encrypt(decryptToBytes(request.payload()), seq);
            EncryptedValue reEncryptedUserId = encrypt(decryptToBytes(request.userId()), seq);

            // Return tuple format: (payload, userId)
            return new SpoutRouterResponse(reEncryptedPayload, reEncryptedUserId);
        } catch (Throwable t) {
            exceptionCtx.handleException(t);
            return null;
        }
    }

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return ComponentConstants.DATASET; // data comes from the dataset component
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        // the data will be routed to the same spout component, which will then forward it to the correct downstream bolt
        return ComponentConstants.RANDOM_JOKE_SPOUT;
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        // this service is a spout router, so the current component is the spout itself
        return ComponentConstants.RANDOM_JOKE_SPOUT;
    }
}
