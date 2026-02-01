package ch.usi.inf.examples.confidential_word_count.common.api.spout;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SpoutRouterRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SpoutRouterResponse;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface SpoutPreprocessingService {
    /**
     * Setup the routing information for the given encrypted tuple, ensuring correct AAD information
     * needed for security features implemented by downstream bolts.
     *
     * @param request the request containing the encrypted tuple
     * @return
     * @throws EnclaveServiceException
     */
    SpoutRouterResponse setupRoute(SpoutRouterRequest request) throws EnclaveServiceException;
}
