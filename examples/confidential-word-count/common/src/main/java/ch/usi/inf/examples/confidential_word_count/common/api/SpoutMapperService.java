package ch.usi.inf.examples.confidential_word_count.common.api;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface SpoutMapperService {
    EncryptedValue setupRoute(TopologySpecification.Component component, EncryptedValue entry) throws EnclaveServiceException;
}
