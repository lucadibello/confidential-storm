package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.model.HistogramSnapshot;
import ch.usi.inf.confidentialstorm.common.model.HistogramUpdate;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface HistogramService {
    void update(HistogramUpdate update);
    HistogramSnapshot snapshot();
}
