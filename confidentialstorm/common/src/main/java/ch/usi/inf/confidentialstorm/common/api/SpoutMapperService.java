package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface SpoutMapperService {
    EncryptedValue setupRoute(TopologySpecification.Component component, EncryptedValue entry);
}
