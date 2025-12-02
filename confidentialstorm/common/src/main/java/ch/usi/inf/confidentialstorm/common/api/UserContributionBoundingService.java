package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.*;

public interface UserContributionBoundingService {
    UserContributionBoundingResponse check(UserContributionBoundingRequest request) throws EnclaveServiceException;
}
