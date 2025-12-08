package ch.usi.inf.examples.synthetic_dp.enclave.service;

import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticHistogramService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticUpdateRequest;
import java.util.Collection;
import java.util.List;

public abstract sealed class SyntheticHistogramServiceVerifier extends ConfidentialBoltService<SyntheticUpdateRequest>
        implements SyntheticHistogramService permits SyntheticHistogramServiceImpl {

    @Override
    public void update(SyntheticUpdateRequest request) throws EnclaveServiceException {
        try {
            super.verify(request);
            updateImpl(request);
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
        }
    }

    public abstract void updateImpl(SyntheticUpdateRequest update)
            throws SealedPayloadProcessingException, CipherInitializationException;

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return TopologySpecification.Component.RANDOM_JOKE_SPOUT;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return TopologySpecification.Component.HISTOGRAM_GLOBAL;
    }

    @Override
    public Collection<EncryptedValue> valuesToVerify(SyntheticUpdateRequest request) {
        return List.of(request.key(), request.count(), request.userId());
    }
}
