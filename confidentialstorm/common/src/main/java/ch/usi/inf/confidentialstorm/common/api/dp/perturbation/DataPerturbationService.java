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

    /**
     * Produces a plaintext dummy partial histogram containing only a marker key.
     * <p>
     * This method does NOT advance the epoch (e.g. does NOT call the DP mechanism's
     * `snapshot` method), and is computationally cheap. It is used by the host bolt 
     * to emit a tuple on every tick even when the real snapshot computation has not 
     * yet completed, ensuring uniform emission timing across replicas.
     * <p>
     * The dummy contains a {@code __dummy} marker key that the aggregation bolt
     * can detect and silently discard.
     *
     * @return a dummy snapshot with only the marker key
     * @throws EnclaveServiceException if an error occurs
     */
    DataPerturbationSnapshot getDummyPartial() throws EnclaveServiceException;

    /**
     * Produces an encrypted dummy partial histogram that is cryptographically
     * indistinguishable from a real encrypted snapshot to any observer outside the enclave.
     * <p>
     * This method does NOT advance the epoch (e.g. does NOT call the DP mechanism's
     * `snapshot` method), and is computationally cheap.
     * and is computationally cheap. It is used by the host bolt to emit a tuple on every
     * tick even when the real snapshot computation has not yet completed, ensuring uniform
     * emission timing that hides workload imbalance from an honest-but-curious admin.
     * <p>
     * The dummy contains a marker inside the ciphertext that the aggregation enclave
     * can detect after decryption, allowing it to silently discard the dummy.
     *
     * @return an encrypted dummy partial (same type as a real snapshot)
     * @throws EnclaveServiceException if encryption fails
     */
    EncryptedDataPerturbationSnapshot getEncryptedDummyPartial() throws EnclaveServiceException;

    /**
     * Initiates an asynchronous plaintext snapshot computation inside the enclave.
     * The enclave spawns an internal thread to run the DP mechanism's {@code snapshot()}
     * and returns immediately, making this ECALL O(1).
     * <p>
     * The result can be retrieved via {@link #pollSnapshot()}.
     * Calling this while a previous async snapshot is still in progress is a no-op.
     *
     * @return true if a new snapshot computation was started, or false if a previous computation is still in progress
     * @throws EnclaveServiceException if an error occurs
     */
    boolean startSnapshot() throws EnclaveServiceException;

    /**
     * Initiates an asynchronous encrypted snapshot computation inside the enclave.
     * The enclave spawns an internal thread to run the DP mechanism's {@code snapshot()}
     * followed by encryption, and returns immediately, making this ECALL O(1).
     * <p>
     * The result can be retrieved via {@link #pollEncryptedSnapshot()}.
     * Calling this while a previous async snapshot is still in progress is a no-op.
     *
     * @throws EnclaveServiceException if an error occurs
     * @return true if a new encrypted snapshot computation was started, or false if a previous computation is still in progress
     */
    boolean startEncryptedSnapshot() throws EnclaveServiceException;

    /**
     * Polls for the result of a previously started async plaintext snapshot.
     * Returns a snapshot with {@code ready() == true} if the computation has completed,
     * or {@code ready() == false} if it is still in progress.
     * <p>
     * This ECALL is O(1) regardless of whether the result is ready.
     *
     * @return a snapshot (always non-null; check {@code ready()} to determine if data is available)
     * @throws EnclaveServiceException if the background computation failed
     */
    DataPerturbationSnapshot pollSnapshot() throws EnclaveServiceException;

    /**
     * Polls for the result of a previously started async encrypted snapshot.
     * Returns a snapshot with {@code ready() == true} if the computation has completed,
     * or {@code ready() == false} if it is still in progress.
     * <p>
     * This ECALL is O(1) regardless of whether the result is ready.
     *
     * @return an encrypted snapshot (always non-null; check {@code ready()} to determine if data is available)
     * @throws EnclaveServiceException if the background computation failed
     */
    EncryptedDataPerturbationSnapshot pollEncryptedSnapshot() throws EnclaveServiceException;
}
