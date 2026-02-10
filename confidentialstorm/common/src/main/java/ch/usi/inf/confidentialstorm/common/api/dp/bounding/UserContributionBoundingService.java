package ch.usi.inf.confidentialstorm.common.api.dp.bounding;

import ch.usi.inf.confidentialstorm.common.api.dp.bounding.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.bounding.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface UserContributionBoundingService {
    UserContributionBoundingResponse checkAndClamp(UserContributionBoundingRequest request) throws EnclaveServiceException;
}