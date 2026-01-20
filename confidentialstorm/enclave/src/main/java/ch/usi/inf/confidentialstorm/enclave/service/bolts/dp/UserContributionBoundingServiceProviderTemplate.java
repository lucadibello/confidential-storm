package ch.usi.inf.confidentialstorm.enclave.service.bolts.dp;

import ch.usi.inf.confidentialstorm.common.api.UserContributionBoundingService;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecificationBuilder;
import ch.usi.inf.confidentialstorm.enclave.dp.UserContributionLimiter;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveJsonUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import org.apache.commons.math3.util.FastMath;

import java.util.*;

public abstract class UserContributionBoundingServiceProviderTemplate
        extends ConfidentialBoltService<UserContributionBoundingRequest>
        implements UserContributionBoundingService {

    /**
     * The logger for this class.
     */
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(UserContributionBoundingServiceProviderTemplate.class);

    private final UserContributionLimiter userContributionLimiter = new UserContributionLimiter();


    // template methods to be implemented by subclasses

    /**
     * Get the expected JSON fields in the decrypted payload.
     *
     * @return Set of expected JSON field names in the payload.
     */
    protected abstract Set<String> getExpectedJsonFields();

    /**
     * Get the per-record clamp value (L_m).
     *
     * @return The per-record clamp value.
     */
    protected abstract double getPerRecordClamp();

    /**
     * Get the maximum contributions allowed per user (C).
     *
     * @return The maximum contributions per user.
     */
    protected abstract long getMaxContributionsPerUser();

    /**
     * Get the AAD specification for encrypting the clamped count.
     *
     * @return The AAD specification for count encryption.
     */
    protected abstract AADSpecification getCountEncryptionAADSpecification();

    @Override
    public UserContributionBoundingResponse checkAndClamp(UserContributionBoundingRequest request) throws EnclaveServiceException {
        try {
            // Verify request integrity
            super.verify(request);

            // Decrypt payload to get word and user_id
            String jsonPayload = sealedPayload.decryptToString(request.word());
            Map<String, Object> jsonMap = EnclaveJsonUtil.parseJson(jsonPayload);

            // validate expected fields
            if (!jsonMap.keySet().containsAll(getExpectedJsonFields())) {
                throw new RuntimeException("Invalid payload structure");
            }

            // Extract word from payload
            String word = (String) jsonMap.get("word");
            if (word == null) {
                throw new RuntimeException("Missing 'word' field in payload");
            }

            // Extract user_id from payload
            String userId = String.valueOf(jsonMap.get("user_id")); // long to string
            if (userId == null) {
                throw new RuntimeException("Missing 'user_id' field in payload");
            }

            // Extract count from request
            // double count = Double.parseDouble(sealedPayload.decryptToString(request.count()));

            // FIXME: for word count, each record contributes 1 count BUT depends on application
            double count = 1.0f;

            // Enforce contribution bounding: each user can contribute at most C records
            // NOTE: needed to maintaining L_1 = C * L_m sensitivity assumption
            if (!userContributionLimiter.allow(userId, getMaxContributionsPerUser())) {
                log.debug("[UserContributionLimiter] Rejected contribution from user {} (exceeded C={})",
                        userId, getMaxContributionsPerUser());

                // user exceeded max contributions, drop the record
                return UserContributionBoundingResponse.dropped();
            }

            // Clamp the per-record contribution to L_m
            final double perRecordClamp = getPerRecordClamp();
            double clamped = FastMath.max(-perRecordClamp, FastMath.min(perRecordClamp, count));

            log.debug("[UserContributionBounding] Accepted contribution from user {}: original count = {}, clamped count = {}",
                    userId, count, clamped);

            // encrypt clamped count
            EncryptedValue encryptedClampedCount = sealedPayload.encryptString(
                    String.valueOf(clamped),
                    getCountEncryptionAADSpecification()
            );

            // return authorized response with encrypted clamped count
            return UserContributionBoundingResponse.authorized(encryptedClampedCount);
        } catch (Throwable t) {
            // use exception manager
            exceptionCtx.handleException(t);
            return null; // signal error
        }
    }

    @Override
    public Collection<EncryptedValue> valuesToVerify(UserContributionBoundingRequest request) {
        return List.of(request.word());
    }
}
