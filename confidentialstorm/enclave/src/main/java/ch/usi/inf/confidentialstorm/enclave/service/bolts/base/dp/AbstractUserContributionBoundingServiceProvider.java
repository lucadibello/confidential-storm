package ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.bounding.UserContributionBoundingService;
import ch.usi.inf.confidentialstorm.common.api.dp.bounding.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.bounding.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.Hash;
import ch.usi.inf.confidentialstorm.enclave.dp.UserContributionLimiter;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import org.apache.commons.math3.util.FastMath;

import java.util.Objects;

public abstract class AbstractUserContributionBoundingServiceProvider
        extends ConfidentialBoltService<UserContributionBoundingRequest>
        implements UserContributionBoundingService {

    /**
     * The logger for this class.
     */
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(AbstractUserContributionBoundingServiceProvider.class);

    private final UserContributionLimiter userContributionLimiter = new UserContributionLimiter();


    // template methods to be implemented by subclasses

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


    @Override
    public UserContributionBoundingResponse checkAndClamp(UserContributionBoundingRequest request) throws EnclaveServiceException {
        try {
            // Verify request
            super.verify(request);

            // Decrypt word (now just a plain string, not JSON with user_id embedded)
            String word = Objects.requireNonNull(decryptToString(request.word()),"Missing 'word' field in payload");
            // Decrypt userId
            String userId = Objects.requireNonNull(decryptToString(request.userId()),"Missing 'user_id' field in payload");
            // Extract count from request
            long count = decryptToLong(request.count());

            // Enforce contribution bounding: each user can contribute at most C records
            // NOTE: needed to maintaining L_1 = C * L_m sensitivity assumption
            long accepted_contributions = userContributionLimiter.allow(userId, count, getMaxContributionsPerUser());

            if (accepted_contributions <= 0) {
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

            // Re-encrypt all values with new AAD for next hop
            int seq = nextSequenceNumber();
            EncryptedValue encryptedWord = encrypt(word, seq);
            EncryptedValue encryptedClampedCount = encrypt(clamped, seq);
            EncryptedValue encryptedUserId = encrypt(userId, seq);

            // Generate word-only routing key for data perturbation partitioning
            // (ensures all contributions for the same word land on the same DP replica)
            byte[] dpRoutingKey = Hash.computeHash(("word:" + word).getBytes());

            // Return authorized response with tuple format: (word, clampedCount, userId, routingKey)
            return UserContributionBoundingResponse.authorized(encryptedWord, encryptedClampedCount, encryptedUserId, dpRoutingKey);
        } catch (Throwable t) {
            // use exception manager
            exceptionCtx.handleException(t);
            return null; // signal error
        }
    }
}
