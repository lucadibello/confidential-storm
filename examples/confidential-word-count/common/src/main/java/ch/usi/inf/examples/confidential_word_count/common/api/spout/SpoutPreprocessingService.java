package ch.usi.inf.examples.confidential_word_count.common.api.spout;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SpoutPreprocessingRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SpoutPreprocessingResponse;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface SpoutPreprocessingService {
    /**
     * Sets up the routing and AAD information for the encrypted tuple.
     *
     * @param request the request containing the encrypted tuple
     * @return the preprocessed response with updated routing details
     * @throws EnclaveServiceException if routing setup fails
     */
    SpoutPreprocessingResponse setupRoute(SpoutPreprocessingRequest request) throws EnclaveServiceException;
}
