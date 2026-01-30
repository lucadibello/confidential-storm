package ch.usi.inf.examples.confidential_word_count.common.api.spout;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface SpoutRouterService {
    /**
     * Setup the routing information for the given encrypted tuple, ensuring correct AAD information
     * needed for security features implemented by downstream bolts.
     *
     * @param entry
     * @return
     * @throws EnclaveServiceException
     */
    EncryptedValue setupRoute(EncryptedValue entry) throws EnclaveServiceException;
}
