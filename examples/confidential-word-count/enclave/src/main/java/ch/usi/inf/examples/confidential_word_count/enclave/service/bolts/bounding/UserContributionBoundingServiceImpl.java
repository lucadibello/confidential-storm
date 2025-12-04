package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.bounding;

import ch.usi.inf.confidentialstorm.common.api.UserContributionBoundingService;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.*;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecificationBuilder;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.bounding.UserContributionBoundingVerifier;
import ch.usi.inf.confidentialstorm.enclave.dp.ContributionLimiter;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.confidential_word_count.common.config.DPConfig;
import com.google.auto.service.AutoService;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@AutoService(UserContributionBoundingService.class)
public class UserContributionBoundingServiceImpl extends UserContributionBoundingVerifier {
    private final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(UserContributionBoundingServiceImpl.class);
    private final ContributionLimiter limiter = new ContributionLimiter();
    private final String producerId = UUID.randomUUID().toString();
    private final AtomicLong sequenceCounter = new AtomicLong(0);
    private static final long MAX_CONTRIBUTIONS = DPConfig.MAX_CONTRIBUTIONS_PER_USER;

    @Override
    public UserContributionBoundingResponse checkImpl(UserContributionBoundingRequest request) throws SealedPayloadProcessingException, CipherInitializationException, RoutingKeyDerivationException, AADEncodingException {
        // Decrypt word
        String word = sealedPayload.decryptToString(request.word());

        // Extract user_id
        DecodedAAD aad = DecodedAAD.fromBytes(request.word().associatedData());
        Object userId = aad.attributes().get("user_id");
        
        boolean enforceBounding = DPConfig.ENABLE_USER_LEVEL_PRIVACY && userId != null;

        if (enforceBounding) {
            if (!limiter.allow(userId, MAX_CONTRIBUTIONS)) {
                LOG.info("User {} exceeded contribution limit ({}), dropping.", userId, MAX_CONTRIBUTIONS);
                return new UserContributionBoundingResponse(null);
            }
        } else if (DPConfig.ENABLE_USER_LEVEL_PRIVACY) {
            LOG.warn("No user_id in AAD, skipping user-level bounding (event-level privacy).");
        }

        // Re-encrypt with new AAD
        long sequence = sequenceCounter.getAndIncrement();
        AADSpecificationBuilder aadBuilder = AADSpecification.builder()
                .sourceComponent(TopologySpecification.Component.USER_CONTRIBUTION_BOUNDING)
                .destinationComponent(TopologySpecification.Component.WORD_COUNT)
                .put("producer_id", producerId)
                .put("seq", sequence);

        if (enforceBounding) {
            aadBuilder.put("user_id", userId);
        }

        EncryptedValue newPayload = sealedPayload.encryptString(word, aadBuilder.build());
        
        return new UserContributionBoundingResponse(newPayload);
    }
}
