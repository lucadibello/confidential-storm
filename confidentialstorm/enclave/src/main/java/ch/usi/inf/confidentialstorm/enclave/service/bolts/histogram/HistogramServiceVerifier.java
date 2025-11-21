package ch.usi.inf.confidentialstorm.enclave.service.bolts.histogram;

import ch.usi.inf.confidentialstorm.common.api.HistogramService;
import ch.usi.inf.confidentialstorm.common.api.model.HistogramUpdateRequest;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;

import java.util.Collection;
import java.util.List;

public abstract class HistogramServiceVerifier extends ConfidentialBoltService<HistogramUpdateRequest> implements HistogramService {
    @Override
    public void update(HistogramUpdateRequest update) {
        super.verify(update);
        updateImpl(update);
    }
    public abstract void updateImpl(HistogramUpdateRequest update);

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
        return List.of();
    }
}
