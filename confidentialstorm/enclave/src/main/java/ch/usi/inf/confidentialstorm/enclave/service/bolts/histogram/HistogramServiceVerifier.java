package ch.usi.inf.confidentialstorm.enclave.service.bolts.histogram;

import ch.usi.inf.confidentialstorm.common.api.HistogramService;
import ch.usi.inf.confidentialstorm.common.api.WordCountService;
import ch.usi.inf.confidentialstorm.common.api.model.HistogramUpdateRequest;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
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
    public Class<?> expectedSourceComponentName() {
        return WordCountService.class;
    }

    @Override
    public Class<?> expectedDestinationComponentName() {
        return HistogramService.class;
    }

    @Override
    public Collection<EncryptedValue> valuesToVerify(HistogramUpdateRequest request) {
        return List.of();
    }
}
