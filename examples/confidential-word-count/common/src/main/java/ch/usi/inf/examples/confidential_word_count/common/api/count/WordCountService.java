package ch.usi.inf.examples.confidential_word_count.common.api.count;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.examples.confidential_word_count.common.api.count.model.WordCountAckResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.count.model.WordCountFlushRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.count.model.WordCountFlushResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.count.model.WordCountRequest;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface WordCountService {
    WordCountAckResponse count(WordCountRequest request) throws EnclaveServiceException;

    WordCountFlushResponse flush(WordCountFlushRequest request) throws EnclaveServiceException;
}
