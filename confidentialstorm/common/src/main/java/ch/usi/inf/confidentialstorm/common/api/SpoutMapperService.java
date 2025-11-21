package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface SpoutMapperService {
    EncryptedValue setupRoute(String componentId, EncryptedValue entry);
}
