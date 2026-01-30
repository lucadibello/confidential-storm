package ch.usi.inf.examples.confidential_word_count.common.api.split;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.examples.confidential_word_count.common.api.split.model.SplitSentenceRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.split.model.SplitSentenceResponse;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface SplitSentenceService {
    SplitSentenceResponse split(SplitSentenceRequest request) throws EnclaveServiceException;
}
