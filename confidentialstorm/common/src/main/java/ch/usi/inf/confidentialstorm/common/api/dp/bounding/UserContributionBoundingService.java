package ch.usi.inf.confidentialstorm.common.api.dp.bounding;

import ch.usi.inf.confidentialstorm.common.api.dp.bounding.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.bounding.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

/**
 * Enclave service for enforcing user-level contribution bounding (C) and per-record clamping (L_m).
 * This service ensures that the total influence of any single user on the resulting histogram
 * is bounded, which is a prerequisite for providing differential privacy guarantees.
 */
@EnclaveService
public interface UserContributionBoundingService {
    /**
     * Checks if a user contribution is within the allowed limits and clamps its value.
     *
     * @param request the request containing the user ID, word, and contribution count
     * @return a response indicating if the contribution was accepted (clamped) or dropped
     * @throws EnclaveServiceException if an error occurs during processing inside the enclave
     */
    UserContributionBoundingResponse checkAndClamp(UserContributionBoundingRequest request) throws EnclaveServiceException;
}
