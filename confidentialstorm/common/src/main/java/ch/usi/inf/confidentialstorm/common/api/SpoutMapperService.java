package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

import javax.crypto.NoSuchPaddingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@EnclaveService
public interface SpoutMapperService {
    EncryptedValue testRoute(TopologySpecification.Component component, EncryptedValue entry) throws SealedPayloadProcessingException, CipherInitializationException;
    EncryptedValue setupRoute(TopologySpecification.Component component, EncryptedValue entry) throws SealedPayloadProcessingException, CipherInitializationException, AADEncodingException;
}
