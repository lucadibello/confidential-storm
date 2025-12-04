package ch.usi.inf.examples.confidential_word_count.common.api;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountFlushRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountFlushResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountResponse;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface WordCountService {
    WordCountResponse count(WordCountRequest request) throws EnclaveServiceException;

    WordCountFlushResponse flush(WordCountFlushRequest request) throws EnclaveServiceException;
}
