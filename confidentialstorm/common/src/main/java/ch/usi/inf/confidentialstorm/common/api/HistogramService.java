package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.api.model.HistogramSnapshotResponse;
import ch.usi.inf.confidentialstorm.common.api.model.HistogramUpdateRequest;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface HistogramService {
    void update(HistogramUpdateRequest update) throws EnclaveServiceException;

    HistogramSnapshotResponse snapshot();
}
