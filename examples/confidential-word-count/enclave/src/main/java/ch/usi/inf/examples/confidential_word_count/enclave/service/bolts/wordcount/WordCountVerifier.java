package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.wordcount;

import ch.usi.inf.confidentialstorm.common.crypto.exception.*;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.examples.confidential_word_count.common.api.WordCountService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountAckResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountFlushRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountFlushResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountResponse;

import java.util.Collection;
import java.util.List;

public abstract sealed class WordCountVerifier extends ConfidentialBoltService<WordCountRequest> implements WordCountService permits WordCountServiceImpl{

    @Override
    public WordCountAckResponse count(WordCountRequest request) throws EnclaveServiceException {
        try {
            super.verify(request);
            return countImpl(request);
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
            return null;
        }
    }

    public abstract WordCountAckResponse countImpl(WordCountRequest request) throws SealedPayloadProcessingException, CipherInitializationException, RoutingKeyDerivationException, AADEncodingException;

    @Override
    public WordCountFlushResponse flush(WordCountFlushRequest request) throws EnclaveServiceException {
        try {
            // NOTE: no verification needed (the request is empty)
            return flushImpl(request);
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
            return null;
        }
    }

    public abstract WordCountFlushResponse flushImpl(WordCountFlushRequest request) throws SealedPayloadProcessingException, CipherInitializationException, RoutingKeyDerivationException, AADEncodingException;

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return TopologySpecification.Component.USER_CONTRIBUTION_BOUNDING;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return TopologySpecification.Component.WORD_COUNT;
    }

    @Override
    public Collection<EncryptedValue> valuesToVerify(WordCountRequest request) {
        return List.of(request.word());
    }
}
