package ch.usi.inf.confidentialstorm.enclave.service.bolts.bounding;

import ch.usi.inf.confidentialstorm.common.api.UserContributionBoundingService;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.*;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

import java.util.Collection;
import java.util.List;

public abstract class UserContributionBoundingVerifier extends ConfidentialBoltService<UserContributionBoundingRequest> implements UserContributionBoundingService {
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(UserContributionBoundingVerifier.class);

    abstract public UserContributionBoundingResponse checkImpl(UserContributionBoundingRequest request) throws SealedPayloadProcessingException, CipherInitializationException, RoutingKeyDerivationException, AADEncodingException;

    @Override
    public UserContributionBoundingResponse check(UserContributionBoundingRequest request) throws EnclaveServiceException {
        try {
            log.debug("check called");
            super.verify(request);
            return checkImpl(request);
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
            return null;
        }
    }

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return TopologySpecification.Component.SENTENCE_SPLIT;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return TopologySpecification.Component.USER_CONTRIBUTION_BOUNDING;
    }

    @Override
    public Collection<EncryptedValue> valuesToVerify(UserContributionBoundingRequest request) {
        return List.of(request.word());
    }
}
