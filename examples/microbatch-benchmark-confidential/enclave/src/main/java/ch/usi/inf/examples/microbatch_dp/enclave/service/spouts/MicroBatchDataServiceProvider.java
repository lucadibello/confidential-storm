package ch.usi.inf.examples.microbatch_dp.enclave.service.spouts;

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
import ch.usi.inf.examples.microbatch_dp.common.api.MicroBatchDataService;
import ch.usi.inf.examples.microbatch_dp.common.api.model.MicroBatchEncryptedRecord;
import ch.usi.inf.examples.microbatch_dp.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

@AutoService(MicroBatchDataService.class)
public final class MicroBatchDataServiceProvider
    implements MicroBatchDataService
{

    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(
        MicroBatchDataServiceProvider.class
    );
    private final EnclaveExceptionContext exceptionCtx =
        EnclaveExceptionContext.getInstance();
    private final SealedPayload sealedPayload = SealedPayload.fromConfig();
    private final String producerId = UUID.randomUUID().toString();
    private long seq = 0L;

    @Override
    public MicroBatchEncryptedRecord encryptRecord(
        String key,
        String count,
        String userId
    ) throws EnclaveServiceException {
        try {
            return encryptOne(key, count, userId);
        } catch (
            SealedPayloadProcessingException
            | CipherInitializationException
            | AADEncodingException
            | NoSuchAlgorithmException e
        ) {
            exceptionCtx.handleException(e);
            log.error("Failed to encrypt record for key {}", key);
            return null;
        }
    }

    @Override
    public MicroBatchEncryptedRecord[] encryptRecords(
        String[] keys,
        String[] counts,
        String[] userIds
    ) throws EnclaveServiceException {
        if (keys.length != counts.length || keys.length != userIds.length) {
            log.error("encryptRecords: array length mismatch (keys={}, counts={}, userIds={})",
                keys.length, counts.length, userIds.length);
            return null;
        }
        final int n = keys.length;
        final MicroBatchEncryptedRecord[] out = new MicroBatchEncryptedRecord[n];
        try {
            for (int i = 0; i < n; i++) {
                out[i] = encryptOne(keys[i], counts[i], userIds[i]);
            }
            return out;
        } catch (
            SealedPayloadProcessingException
            | CipherInitializationException
            | AADEncodingException
            | NoSuchAlgorithmException e
        ) {
            exceptionCtx.handleException(e);
            log.error("Failed to encrypt batch of {} records", n);
            return null;
        }
    }

    private MicroBatchEncryptedRecord encryptOne(String key, String count, String userId)
        throws SealedPayloadProcessingException,
               CipherInitializationException,
               AADEncodingException,
               NoSuchAlgorithmException
    {
        AADSpecification aad = AADSpecification.builder()
            .sourceComponent(ComponentConstants.SPOUT)
            .destinationComponent(ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING)
            .put("producer_id", producerId)
            .put("seq", seq++)
            .put("user_id", userId)
            .build();
        EncryptedValue k = sealedPayload.encryptString(key, aad);
        EncryptedValue c = sealedPayload.encryptString(count, aad);
        EncryptedValue u = sealedPayload.encryptString(userId, aad);
        byte[] routingKey = Hash.computeHash(("user:" + userId).getBytes());
        return new MicroBatchEncryptedRecord(k, c, u, routingKey);
    }
}
