package ch.usi.inf.examples.synthetic_dp.common.api;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticSnapshotResponse;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticUpdateRequest;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface SyntheticHistogramService {
    void configure(int maxTimeSteps, long mu) throws EnclaveServiceException;
    void update(SyntheticUpdateRequest request) throws EnclaveServiceException;
    SyntheticSnapshotResponse snapshot() throws EnclaveServiceException;
}
