package ch.usi.inf.examples.confidential_word_count.common.api;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramSnapshotResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramUpdateAckResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramUpdateRequest;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface HistogramService {
    HistogramUpdateAckResponse update(HistogramUpdateRequest update) throws EnclaveServiceException;
    HistogramSnapshotResponse snapshot() throws EnclaveServiceException;
}
