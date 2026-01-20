package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface UserContributionBoundingService {
    UserContributionBoundingResponse checkAndClamp(UserContributionBoundingRequest request) throws EnclaveServiceException;
}