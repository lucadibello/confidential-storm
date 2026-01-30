package ch.usi.inf.examples.confidential_word_count.common.api.histogram;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.examples.confidential_word_count.common.api.histogram.model.HistogramSnapshotResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.histogram.model.HistogramUpdateAckResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.histogram.model.HistogramUpdateRequest;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface HistogramService {
    HistogramUpdateAckResponse update(HistogramUpdateRequest update) throws EnclaveServiceException;

    HistogramSnapshotResponse snapshot() throws EnclaveServiceException;
}
