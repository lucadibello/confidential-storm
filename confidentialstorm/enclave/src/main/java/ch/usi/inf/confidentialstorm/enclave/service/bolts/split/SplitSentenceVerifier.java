package ch.usi.inf.confidentialstorm.enclave.service.bolts.split;

import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceRequest;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceResponse;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;

import java.util.Collection;
import java.util.List;

public abstract class SplitSentenceVerifier extends ConfidentialBoltService<SplitSentenceRequest> implements SplitSentenceService {

    abstract public SplitSentenceResponse splitImpl(SplitSentenceRequest request);

    @Override
    public SplitSentenceResponse split(SplitSentenceRequest request) {
        System.out.println("SplitSentenceVerifier: split called");
        // verify the request
        super.verify(request);
        // call the implementation
        return splitImpl(request);
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
