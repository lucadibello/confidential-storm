package ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.DataPerturbationService;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.*;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.dp.StreamingDPMechanism;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Abstract base implementation of the {@link DataPerturbationService}.
 * This class handles the initialization of the {@link StreamingDPMechanism} and provides
 * template methods for privacy parameters.
 */
public abstract class AbstractDataPerturbationServiceProvider
        extends ConfidentialBoltService<DataPerturbationRequest>
        implements DataPerturbationService
{

    /**
     * Reserved key used inside the encrypted payload to mark a dummy partial.
     * The aggregation enclave checks for this key after decryption.
     * This key is never a valid histogram word because it starts with double underscore.
     */
    static final String DUMMY_MARKER_KEY = "__dummy";


    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(AbstractDataPerturbationServiceProvider.class);
    private final StreamingDPMechanism mechanism;
    private int epoch = 0;

    /**
     * Epoch value captured at {@link #startEncryptedSnapshot()} time for use by the
     * background thread's encrypt call.
     * NOTE: only one async snapshot can be in progress at a time - a single field is sufficient.
     */
    private volatile int asyncEpoch = -1;

    /**
     * Holds the result of the background plaintext snapshot computation.
     */
    private volatile Map<String, Long> completedPlaintextResult;

    /**
     * Holds the result of the background encrypted snapshot computation.
     */
    private volatile EncryptedDataPerturbationSnapshot completedEncryptedResult;

    /**
     * Whether an async snapshot computation is currently in progress.
     */
    private volatile boolean snapshotInProgress;

    /**
     * The background thread running the async snapshot computation.
     */
    private Thread snapshotThread;

    /**
     * Holds an exception thrown by the background snapshot thread, to be
     * propagated on the next {@link #pollSnapshot()} or {@link #pollEncryptedSnapshot()} call.
     */
    private volatile Throwable asyncError;

    // template methods for subclasses to specify privacy parameters and sensitivity

    /**
     * Privacy budget for the (epsilon, delta)-DP guarantee for Key Selection (epsilon_k)
     * @return the epsilon value for key selection
     */
    public abstract double getEpsilonK();

    /**
     * Failure probability (delta_k) for the (epsilon, delta)-DP guarantee for Key Selection
     * @return the delta value
     */
    public abstract double getDeltaK();

    /**
     * Privacy budget for the (epsilon, delta)-DP guarantee for Histogram (epsilon_h)
     * @return the epsilon value for histogram
     */
    public abstract double getEpsilonH();

    /**
     * Failure probability (delta_h) for the (epsilon, delta)-DP guarantee for Histogram
     * @return the delta value for histogram
     */
    public abstract double getDeltaH();

    /**
     * The minimum number of unique user contributions required for releasing a key in the histogram.
     * <p>
     * NOTE: higher mu = fewer keys selected, but higher quality (fewer 0 counts).
     *
     * @return the base threshold mu
     */
    public abstract long getMu();

    /**
     * The maximum number of time steps to consider for the data stream.
     * <p>
     * NOTE: lower maxTimeSteps = less noise, faster prediction, better histogram quality.
     *
     * @return the maximum number of time steps
     */
    public abstract int getMaxTimeSteps();

    /**
     * The maximum absolute value for each individual record contribution.
     * <p>
     * For example, if each record represents a count of events, this could be set to 1 to ensure that each record
     * contributes at most 1 to the histogram.
     *
     * @return the per-record clamp value
     */
    public abstract double getPerRecordClamp();


    /**
     * The maximum number of contributions a single user can make across all time steps.
     * <p>
     * This is used to calculate the l1 sensitivity of the histogram, which in turn determines the amount of noise to
     * add for differential privacy.
     *
     * @return the maximum user contributions
     */
    public abstract long getMaxUserContributions();

    /**
     * Constructs a new AbstractDataPerturbationServiceProvider and initializes the DP mechanism.
     */
    public AbstractDataPerturbationServiceProvider() {
        // Calibrate noise for Key Selection (Sensitivity = 1)
        double rhoK = DPUtil.cdpRho(getEpsilonK(), getDeltaK());
        double sigmaKey = DPUtil.calculateSigma(rhoK, getMaxTimeSteps(), 1.0);

        // Calibrate noise for Histogram (Sensitivity = C * L_m)
        double rhoH = DPUtil.cdpRho(getEpsilonH(), getDeltaH());

        // calculate l1 sensitivity for histogram (to calibrate gaussian noise)
        final double l1Sensitivity = DPUtil.l1Sensitivity(getMaxUserContributions(), getPerRecordClamp());
        double sigmaHist = DPUtil.calculateSigma(rhoH, getMaxTimeSteps(), l1Sensitivity);

        // Initialize the standard mechanism
        this.mechanism = new StreamingDPMechanism(
                sigmaKey,
                sigmaHist,
                getMaxTimeSteps(),
                getMu(),
                getMaxUserContributions()
        );
    }

    @Override
    protected Map<String, Object> getExtraAADAttributes() {
        // When called from the async snapshot thread, use the captured epoch
        int effectiveEpoch = (asyncEpoch >= 0) ? asyncEpoch : epoch;
        return Map.of("epoch", effectiveEpoch);
    }

    private boolean verifyContribution(DataPerturbationContributionEntryRequest update) {
        // verify that all required fields are present
        return Objects.nonNull(update.word()) && Objects.nonNull(update.userId()) && Objects.nonNull(update.clampedCount());
    }

    @Override
    public DataPerturbationContributionEntryResponse addContribution(DataPerturbationContributionEntryRequest update)
            throws EnclaveServiceException {
        try {
            // verify request
            verify(update);

            // decrypt entries
            String word = decryptToString(update.word());
            String userId = decryptToString(update.userId());
            double clamped_count = decryptToDouble(update.clampedCount());

            // ensure contribution is valid (e.g., non-negative count, userId and word are not empty)
            if (!verifyContribution(update)) {
                throw new IllegalArgumentException("Invalid contribution entry: " + update);
            }

            // add contribution to the mechanism
            this.mechanism.addContribution(userId, word, clamped_count);

            // return acknowledgement response (could be extended to include additional info if needed)
            return new DataPerturbationContributionEntryResponse();
        } catch (Throwable t) {
            this.exceptionCtx.handleException(t);
            return null; // signal error if exceptions are disabled
        }
    }

    @Override
    public DataPerturbationSnapshot getSnapshot() throws EnclaveServiceException {
        try {
            this.epoch++;
            return new DataPerturbationSnapshot(this.mechanism.snapshot());
        } catch (Throwable t) {
            this.exceptionCtx.handleException(t);
            return null; // signal error if exceptions are disabled
       }
    }

    @Override
    public EncryptedDataPerturbationSnapshot getEncryptedSnapshot() throws EnclaveServiceException {
        try {
            this.epoch++;

            Map<String, Long> snapshot = this.mechanism.snapshot();

            // Widen Long -> Object for the generic encrypt(Map<String, Object>, seq) method
            Map<String, Object> payload = new LinkedHashMap<>(snapshot.size());
            payload.putAll(snapshot);

            EncryptedValue encrypted = encrypt(payload, nextSequenceNumber());
            return new EncryptedDataPerturbationSnapshot(encrypted);
        } catch (Throwable t) {
            this.exceptionCtx.handleException(t);
            return null;
        }
    }

    @Override
    public DataPerturbationSnapshot getDummyPartial() throws EnclaveServiceException {
        try {
            // Does NOT call mechanism.snapshot(), does NOT increment epoch.
            // Returns a plaintext snapshot with only the dummy marker key.
            Map<String, Long> dummy = new LinkedHashMap<>();
            dummy.put(DUMMY_MARKER_KEY, 0L);
            return new DataPerturbationSnapshot(dummy);
        } catch (Throwable t) {
            this.exceptionCtx.handleException(t);
            return null;
        }
    }

    @Override
    public EncryptedDataPerturbationSnapshot getEncryptedDummyPartial() throws EnclaveServiceException {
        try {
            // Does NOT call mechanism.snapshot(), does NOT increment epoch.
            // The payload contains only the dummy marker — encrypted with the same
            // AEAD scheme, AAD structure (producerId, epoch, seq), and key as real
            // partials, making it indistinguishable to any observer without the key.
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put(DUMMY_MARKER_KEY, true);

            EncryptedValue encrypted = encrypt(payload, nextSequenceNumber());
            return new EncryptedDataPerturbationSnapshot(encrypted);
        } catch (Throwable t) {
            this.exceptionCtx.handleException(t);
            return null;
        }
    }

    // ---- Async snapshot methods ----

    @Override
    public boolean startSnapshot() throws EnclaveServiceException {
        try {
            if (snapshotInProgress) {
                log.warn("Attempted to start a new snapshot while another is still in progress");
                return false; // already computing
            }
            snapshotInProgress = true;
            asyncError = null;
            this.epoch++;

            log.info("Starting async snapshot computation for epoch " + this.epoch);
            snapshotThread = new Thread(() -> {
                try {
                    log.info("[BG THREAD] Computing snapshot for epoch " + this.epoch);
                    completedPlaintextResult = mechanism.snapshot();
                    log.info("[BG THREAD] Snapshot computation completed for epoch " + this.epoch);
                } catch (Throwable t) {
                    asyncError = t;
                    log.error("[BG THREAD] Snapshot computation failed for epoch " + this.epoch, t);
                }
            }, "enclave-snapshot");
            snapshotThread.setDaemon(true);
            snapshotThread.start();
            log.info("Async snapshot thread started successfully for epoch " + this.epoch);
        } catch (Throwable t) {
            snapshotInProgress = false;
            this.exceptionCtx.handleException(t);
            return false;
        }
        // signal that snapshot computation has started successfully
        return true;
    }

    @Override
    public boolean startEncryptedSnapshot() throws EnclaveServiceException {
        try {
            if (snapshotInProgress) {
                log.warn("Attempted to start a new encrypted snapshot while another is still in progress");
                return false; // already computing
            }
            snapshotInProgress = true;
            asyncError = null;
            this.epoch++;

            // Capture epoch and sequence number now (in the ECALL thread) to guarantee
            // correct ordering and AAD consistency. The background thread will use these
            // captured values for encryption.
            final int capturedEpoch = this.epoch;
            final int capturedSeqNum = nextSequenceNumber();

            log.info("Starting async encrypted snapshot computation for epoch " + capturedEpoch + ", seqNum " + capturedSeqNum);
            snapshotThread = new Thread(() -> {
                try {
                    log.info("[BG THREAD] Computing encrypted snapshot for epoch " + capturedEpoch + ", seqNum " + capturedSeqNum);
                    Map<String, Long> snapshot = mechanism.snapshot();
                    log.info("[BG THREAD] Snapshot computation completed for epoch " + capturedEpoch + ", seqNum " + capturedSeqNum);

                    // Widen Long -> Object for the generic encrypt method
                    Map<String, Object> payload = new LinkedHashMap<>(snapshot.size());
                    payload.putAll(snapshot);

                    // Set asyncEpoch so getExtraAADAttributes() uses the captured value
                    asyncEpoch = capturedEpoch;
                    EncryptedValue encrypted = encrypt(payload, capturedSeqNum);
                    asyncEpoch = -1; // reset

                    completedEncryptedResult = new EncryptedDataPerturbationSnapshot(encrypted);
                } catch (Throwable t) {
                    asyncEpoch = -1; // reset on error
                    asyncError = t;
                    log.error("[BG THREAD] Encrypted snapshot computation failed for epoch " + capturedEpoch + ", seqNum " + capturedSeqNum, t);
                }
            }, "enclave-snapshot");
            snapshotThread.setDaemon(true);
            snapshotThread.start();
            log.info("Async encrypted snapshot thread started successfully for epoch " + capturedEpoch + ", seqNum " + capturedSeqNum);
        } catch (Throwable t) {
            snapshotInProgress = false;
            this.exceptionCtx.handleException(t);
            return false;
        }
        // signal that snapshot computation has started successfully
        return true;
    }

    @Override
    public DataPerturbationSnapshot pollSnapshot() throws EnclaveServiceException {
        try {
            // Check for errors from the background thread
            if (asyncError != null) {
                Throwable error = asyncError;
                asyncError = null;
                snapshotInProgress = false;
                throw new EnclaveServiceException("Async snapshot computation failed", error);
            }

            Map<String, Long> result = completedPlaintextResult;
            if (result != null) {
                completedPlaintextResult = null;
                snapshotInProgress = false;
                return new DataPerturbationSnapshot(result);
            }
            return DataPerturbationSnapshot.notReady();
        } catch (EnclaveServiceException e) {
            throw e;
        } catch (Throwable t) {
            this.exceptionCtx.handleException(t);
            return null; // signal error if exceptions are disabled
        }
    }

    @Override
    public EncryptedDataPerturbationSnapshot pollEncryptedSnapshot() throws EnclaveServiceException {
        try {
            // Check for errors from the background thread
            if (asyncError != null) {
                Throwable error = asyncError;
                asyncError = null;
                snapshotInProgress = false;
                throw new EnclaveServiceException("Async encrypted snapshot computation failed", error);
            }

            EncryptedDataPerturbationSnapshot result = completedEncryptedResult;
            if (result != null) {
                completedEncryptedResult = null;
                snapshotInProgress = false;
                return result;
            }
            return EncryptedDataPerturbationSnapshot.notReady();
        } catch (EnclaveServiceException e) {
            throw e;
        } catch (Throwable t) {
            this.exceptionCtx.handleException(t);
            return null; // signal error if exceptions are disabled
        }
    }
}
