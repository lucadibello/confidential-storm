package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.histogram;

import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.examples.confidential_word_count.common.api.HistogramService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramUpdateRequest;

import java.util.Collection;
import java.util.List;

public abstract sealed class HistogramServiceVerifier extends ConfidentialBoltService<HistogramUpdateRequest> implements HistogramService permits HistogramServiceImpl {
    @Override
    public void update(HistogramUpdateRequest update) throws EnclaveServiceException {
        try {
            super.verify(update);
            updateImpl(update);
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
        }
    }

    public abstract void updateImpl(HistogramUpdateRequest update) throws SealedPayloadProcessingException, CipherInitializationException;

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return TopologySpecification.Component.WORD_COUNT;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return TopologySpecification.Component.HISTOGRAM_GLOBAL;
    }

    @Override
    public Collection<EncryptedValue> valuesToVerify(HistogramUpdateRequest request) {
        return List.of(request.word(), request.count());
    }
}
