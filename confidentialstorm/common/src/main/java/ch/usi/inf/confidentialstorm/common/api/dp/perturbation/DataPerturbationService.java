package ch.usi.inf.confidentialstorm.common.api.dp.perturbation;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationContributionEntryRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationContributionEntryResponse;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.EncryptedDataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface DataPerturbationService {
    @SuppressWarnings("UnusedReturnValue")
    DataPerturbationContributionEntryResponse addContribution(DataPerturbationContributionEntryRequest request) throws EnclaveServiceException;
    DataPerturbationSnapshot getSnapshot() throws EnclaveServiceException;

    /**
     * Returns the current DP histogram snapshot encrypted inside the enclave.
     * The encrypted snapshot can be forwarded to an aggregation bolt for secure merging.
     */
    EncryptedDataPerturbationSnapshot getEncryptedSnapshot() throws EnclaveServiceException;
}