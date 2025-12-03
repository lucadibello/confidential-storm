package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.*;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface UserContributionBoundingService {
    UserContributionBoundingResponse check(UserContributionBoundingRequest request) throws EnclaveServiceException;
}