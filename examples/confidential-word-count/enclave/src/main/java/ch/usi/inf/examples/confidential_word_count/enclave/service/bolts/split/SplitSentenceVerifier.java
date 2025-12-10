package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.split;

import ch.usi.inf.confidentialstorm.common.crypto.exception.*;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.examples.confidential_word_count.common.api.SplitSentenceService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.SplitSentenceRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.model.SplitSentenceResponse;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;

import java.util.Collection;
import java.util.List;

public abstract sealed class SplitSentenceVerifier extends ConfidentialBoltService<SplitSentenceRequest> implements SplitSentenceService permits SplitSentenceServiceImpl {
    abstract public SplitSentenceResponse splitImpl(SplitSentenceRequest request) throws SealedPayloadProcessingException, CipherInitializationException, RoutingKeyDerivationException, AADEncodingException;

    @Override
    public SplitSentenceResponse split(SplitSentenceRequest request) throws EnclaveServiceException {
        try {
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
        return ComponentConstants.RANDOM_JOKE_SPOUT;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return ComponentConstants.SENTENCE_SPLIT;
    }

    @Override
    public Collection<EncryptedValue> valuesToVerify(SplitSentenceRequest request) {
        return List.of(request.jokeEntry());
    }
}
