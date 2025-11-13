package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.model.HistogramSnapshotResponse;
import ch.usi.inf.confidentialstorm.common.model.HistogramUpdateRequest;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface HistogramService {
    void update(HistogramUpdateRequest update);
    HistogramSnapshotResponse snapshot();
}
