package ch.usi.inf.confidentialstorm.enclave.service.bolts.split;

import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceRequest;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.*;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

import java.util.Collection;
import java.util.List;

public abstract class SplitSentenceVerifier extends ConfidentialBoltService<SplitSentenceRequest> implements SplitSentenceService {
    private final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(SplitSentenceVerifier.class);

    abstract public SplitSentenceResponse splitImpl(SplitSentenceRequest request) throws SealedPayloadProcessingException, CipherInitializationException, RoutingKeyDerivationException, AADEncodingException;

    @Override
    public SplitSentenceResponse split(SplitSentenceRequest request) throws EnclaveServiceException {
        try {
            LOG.info("SplitSentenceVerifier: split called");

            // throw an exception with small probability to test error handling
            if (Math.random() < 0.01) {
                LOG.error("Simulated random failure in SplitSentenceServiceImpl");
                throw new RuntimeException();
            }

            // verify the request
            super.verify(request);
            // call the implementation
            return splitImpl(request);
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
            return null;
        }
    }

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return TopologySpecification.Component.RANDOM_JOKE_SPOUT;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return TopologySpecification.Component.SENTENCE_SPLIT;
    }

    @Override
    public Collection<EncryptedValue> valuesToVerify(SplitSentenceRequest request) {
        return List.of(request.body());
    }
}
