package ch.usi.inf.confidentialstorm.common.api.dp.perturbation;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationContributionEntryRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationContributionEntryResponse;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.EncryptedDataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

/**
 * Enclave service responsible for maintaining a differentially private histogram.
 * This service implements the core DP mechanism, including streaming key selection (Algorithm 1),
 * hierarchical perturbation (Algorithm 2), and empty key release prediction (Algorithm 3).
 */
@EnclaveService
public interface DataPerturbationService {
    /**
     * Adds a clamped user contribution to the DP mechanism.
     *
     * @param request the request containing the user ID, word, and clamped contribution count
     * @return an acknowledgement response
     * @throws EnclaveServiceException if an error occurs during processing inside the enclave
     */
    @SuppressWarnings("UnusedReturnValue")
    DataPerturbationContributionEntryResponse addContribution(DataPerturbationContributionEntryRequest request) throws EnclaveServiceException;

    /**
     * Produces a snapshot of the current differentially private histogram.
     *
     * @return the noisy histogram snapshot
     * @throws EnclaveServiceException if an error occurs during snapshot production
     */
    DataPerturbationSnapshot getSnapshot() throws EnclaveServiceException;

    /**
     * Returns the current DP histogram snapshot encrypted inside the enclave.
     * The encrypted snapshot can be forwarded to an aggregation bolt for secure merging.
     *
     * @return the encrypted noisy histogram snapshot
     * @throws EnclaveServiceException if an error occurs during snapshot encryption
     */
    EncryptedDataPerturbationSnapshot getEncryptedSnapshot() throws EnclaveServiceException;
}
