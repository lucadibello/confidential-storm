package ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.DataPerturbationService;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.*;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.enclave.dp.StreamingDPMechanism;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;

import java.util.Objects;

public abstract class AbstractDataPerturbationServiceProvider
        extends ConfidentialBoltService<DataPerturbationRequest>
        implements DataPerturbationService
{

    private final StreamingDPMechanism mechanism;

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
     */
    public abstract long getMu();

    /**
     * The maximum number of time steps to consider for the data stream.
     * <p>
     * NOTE: lower maxTimeSteps = less noise, faster prediction, better histogram quality.
     */
    public abstract int getMaxTimeSteps();

    /**
     * The maximum absolute value for each individual record contribution.
     * <p>
     * For example, if each record represents a count of events, this could be set to 1 to ensure that each record
     * contributes at most 1 to the histogram.
     */
    public abstract double getPerRecordClamp();


    /**
     * The maximum number of contributions a single user can make across all time steps.
     * <p>
     * This is used to calculate the l1 sensitivity of the histogram, which in turn determines the amount of noise to
     * add for differential privacy.
     */
    public abstract long getMaxUserContributions();

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
                getMaxUserContributions(),
                getPerRecordClamp()
        );
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
            return new DataPerturbationSnapshot(this.mechanism.snapshot());
        } catch (Throwable t) {
            this.exceptionCtx.handleException(t);
            return null; // signal error if exceptions are disabled
       }
    }
}
