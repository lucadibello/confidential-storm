package ch.usi.inf.examples.synthetic_dp.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.exception.EnclaveExceptionContext;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticDataService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticEncryptedRecord;
import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;
import java.util.Objects;
import java.util.UUID;

@AutoService(SyntheticDataService.class)
public final class SyntheticDataServiceImpl implements SyntheticDataService {
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(SyntheticDataServiceImpl.class);
    private final EnclaveExceptionContext exceptionCtx = EnclaveExceptionContext.getInstance();
    private final SealedPayload sealedPayload;
    private final String producerId = UUID.randomUUID().toString();
    private long seq = 0L;

    public SyntheticDataServiceImpl() {
        this(SealedPayload.fromConfig());
    }

    SyntheticDataServiceImpl(SealedPayload sealedPayload) {
        this.sealedPayload = Objects.requireNonNull(sealedPayload);
    }

    @Override
    public SyntheticEncryptedRecord encryptRecord(String key, String count, String userId) throws EnclaveServiceException {
        try {
            AADSpecification aad = AADSpecification.builder()
                    .sourceComponent(ComponentConstants.SPOUT)
                    .destinationComponent(ComponentConstants.HISTOGRAM_GLOBAL)
                    .put("producer_id", producerId)
                    .put("seq", seq++)
                    .put("user_id", userId)
                    .build();

            EncryptedValue k = sealedPayload.encryptString(key, aad);
            EncryptedValue c = sealedPayload.encryptString(count, aad);
            EncryptedValue u = sealedPayload.encryptString(userId, aad);
            return new SyntheticEncryptedRecord(k, c, u);
        } catch (SealedPayloadProcessingException | CipherInitializationException | AADEncodingException e) {
            exceptionCtx.handleException(e);
            log.error("Failed to encrypt record for key {}", key);
            return null;
        }
    }
}
