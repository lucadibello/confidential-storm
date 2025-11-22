package ch.usi.inf.confidentialstorm.enclave.service.bolts.wordcount;

import ch.usi.inf.confidentialstorm.common.api.WordCountService;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountRequest;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountResponse;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveExceptionUtil;

import java.util.Collection;
import java.util.List;

public abstract class WordCountVerifier extends ConfidentialBoltService<WordCountRequest> implements WordCountService {

    @Override
    public WordCountResponse count(WordCountRequest request) {
        try {
            super.verify(request);
            return countImpl(request);
        } catch (Throwable t) {
            throw EnclaveExceptionUtil.wrap("WordCountService.count", t);
        }
    }
    public abstract WordCountResponse countImpl(WordCountRequest request);

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return TopologySpecification.Component.SENTENCE_SPLIT;
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
