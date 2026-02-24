package ch.usi.inf.examples.synthetic_dp.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.Hash;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.exception.EnclaveExceptionContext;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticDataService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticEncryptedRecord;
import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

import java.security.NoSuchAlgorithmException;
import java.util.UUID;

@AutoService(SyntheticDataService.class)
public final class SyntheticDataServiceProvider implements SyntheticDataService {
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(SyntheticDataServiceProvider.class);
    private final EnclaveExceptionContext exceptionCtx = EnclaveExceptionContext.getInstance();
    private final SealedPayload sealedPayload = SealedPayload.fromConfig();
    private final String producerId = UUID.randomUUID().toString();
    private long seq = 0L;

    @Override
    public SyntheticEncryptedRecord encryptRecord(String key, String count, String userId) throws EnclaveServiceException {
        try {
            // build AAD using metadata
            AADSpecification aad = AADSpecification.builder()
                    .sourceComponent(ComponentConstants.SPOUT)
                    .destinationComponent(ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING)
                    .put("producer_id", producerId)
                    .put("seq", seq++)
                    .put("user_id", userId)
                    .build();

            // encrypt each field separately
            EncryptedValue k = sealedPayload.encryptString(key, aad);
            EncryptedValue c = sealedPayload.encryptString(count, aad);
            EncryptedValue u = sealedPayload.encryptString(userId, aad);

            // generate routing key for user-based partitioning
            byte[] routingKey = Hash.computeHash(("user:" + userId).getBytes());

            // return the encrypted record with routing key
            return new SyntheticEncryptedRecord(k, c, u, routingKey);
        } catch (SealedPayloadProcessingException | CipherInitializationException | AADEncodingException | NoSuchAlgorithmException e) {
            exceptionCtx.handleException(e);
            log.error("Failed to encrypt record for key {}", key);
            return null;
        }
    }
}
