package ch.usi.inf.confidentialstorm.enclave.service.bolts.split;

import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceRequest;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceResponse;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveExceptionUtil;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLoggerFactory;

import java.util.Collection;
import java.util.List;

public abstract class SplitSentenceVerifier extends ConfidentialBoltService<SplitSentenceRequest> implements SplitSentenceService {
    private static final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(SplitSentenceVerifier.class);

    abstract public SplitSentenceResponse splitImpl(SplitSentenceRequest request);

    @Override
    public SplitSentenceResponse split(SplitSentenceRequest request) {
        try {
            LOG.info("SplitSentenceVerifier: split called");
            // verify the request
            super.verify(request);
            // call the implementation
            return splitImpl(request);
        } catch (Throwable t) {
            throw EnclaveExceptionUtil.wrap("SplitSentenceService.split", t);
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
