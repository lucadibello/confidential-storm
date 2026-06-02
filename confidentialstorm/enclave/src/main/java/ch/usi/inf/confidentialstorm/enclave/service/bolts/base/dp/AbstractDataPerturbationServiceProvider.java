package ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.DataPerturbationService;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.*;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.dp.CompositionMode;
import ch.usi.inf.confidentialstorm.enclave.dp.StreamingDPMechanism;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract base implementation of the {@link DataPerturbationService}.
 * This class handles the initialization of the {@link StreamingDPMechanism} and
 * provides
 * template methods for privacy parameters.
 */
public abstract class AbstractDataPerturbationServiceProvider
    extends ConfidentialBoltService<DataPerturbationRequest>
    implements DataPerturbationService {

  /**
   * Reserved key used inside the encrypted payload to mark a dummy partial.
   * The aggregation enclave checks for this key after decryption.
   * This key is never a valid histogram word because it starts with double
   * underscore.
   */
  static final String DUMMY_MARKER_KEY = "__dummy";

  private final StreamingDPMechanism mechanism;
  private int epoch = 0;

  // template methods for subclasses to specify privacy parameters and sensitivity

  /**
   * Privacy budget for the (epsilon, delta)-DP guarantee for Key Selection
   * (epsilon_k)
   * 
   * @return the epsilon value for key selection
   */
  public abstract double getEpsilonK();

  /**
   * Failure probability (delta_k) for the (epsilon, delta)-DP guarantee for Key
   * Selection
   * 
   * @return the delta value
   */
  public abstract double getDeltaK();

  /**
   * Privacy budget for the (epsilon, delta)-DP guarantee for Histogram
   * (epsilon_h)
   * 
   * @return the epsilon value for histogram
   */
  public abstract double getEpsilonH();

  /**
   * Failure probability (delta_h) for the (epsilon, delta)-DP guarantee for
   * Histogram
   * 
   * @return the delta value for histogram
   */
  public abstract double getDeltaH();

  /**
   * The minimum number of unique user contributions required for releasing a key
   * in the histogram.
   * <p>
   * NOTE: higher mu = fewer keys selected, but higher quality (fewer 0 counts).
   *
   * @return the base threshold mu
   */
  public abstract long getMu();

  /**
   * The maximum number of time steps to consider for the data stream.
   * <p>
   * NOTE: lower maxTimeSteps = less noise, faster prediction, better histogram
   * quality.
   *
   * @return the maximum number of time steps
   */
  public abstract int getMaxTimeSteps();

  /**
   * The maximum absolute value for each individual record contribution.
   * <p>
   * For example, if each record represents a count of events, this could be set
   * to 1 to ensure that each record
   * contributes at most 1 to the histogram.
   *
   * @return the per-record clamp value
   */
  public abstract double getPerRecordClamp();

  /**
   * The maximum number of contributions a single user can make across all time
   * steps.
   * <p>
   * This is used to calculate the l1 sensitivity of the histogram, which in turn
   * determines the amount of noise to add for differential privacy.
   *
   * @return the maximum user contributions
   */
  public abstract long getMaxUserContributions();

  /**
   * Fraction of the per-round key-selection delta budget reserved for the
   * threshold-failure cost (e^eps + 1) * beta of Algorithm 1, as in the
   * pre-allocation approach described in the thesis background chapter ("Choosing
   * the Accuracy Parameter beta"). The remaining (1 - alpha) share calibrates the
   * DP-Tree Gaussian noise.
   * <p>
   * The default value 0.5 mirrors the DP-SQLP paper's calibration: the authors
   * clarified (May 2026) that the 2*delta/3 budget allocated to key selection is
   * split equally between the Gaussian-noise share and the threshold-failure
   * share, which corresponds to alpha=0.5.
   * <p>
   * Refer to the thesis background chapter for a detailed discussion of the
   * trade-offs in chosing alpha.
   *
   * @return alpha in (0, 1)
   */
  public double getThresholdFailureFraction() {
    return 0.5;
  }

  /**
   * The C-fold composition theorem used to derive the per-round key-selection
   * budget from the total (epsilon_k, delta_k) budget.
   * <p>
   * Defaults to {@link CompositionMode#ZCDP_LINEAR}, which is the tightest of the
   * three theorems (it avoids the lossy (epsilon, delta) -> rho conversion that
   * the advanced-composition variants incur) and therefore yields the lowest
   * Gaussian noise / best utility. Subclasses may override to select the
   * Dwork-Rothblum-Vadhan or Kairouz-Oh-Viswanath advanced-composition variants.
   *
   * @return the composition mode to use for key-selection budget derivation
   */
  public CompositionMode getCompositionMode() {
    return CompositionMode.ZCDP_LINEAR;
  }

  /**
   * Constructs a new AbstractDataPerturbationServiceProvider and initializes the
   * DP mechanism.
   * <p>
   * The full DP-SQLP Section 4.4 calibration (composing the key-selection budget
   * over C rounds via {@link #getCompositionMode()}, splitting the per-round
   * delta into Gaussian-noise and threshold-failure shares, deriving beta and the
   * threshold quantile, and calibrating sigma_h against (epsilon_h, delta_h) with
   * sensitivity C * L_m) is delegated to
   * {@link StreamingDPMechanism#StreamingDPMechanism(CompositionMode, double,
   * double, double, double, int, long, long, double, double)}.
   * <p>
   * The caller is responsible for choosing the split (epsilon_k, delta_k) and
   * (epsilon_h, delta_h) so that their composition stays within the desired total
   * (epsilon, delta) -- this class does not enforce a global budget.
   */
  public AbstractDataPerturbationServiceProvider() {
    this.mechanism = new StreamingDPMechanism(
        getCompositionMode(),
        getEpsilonK(),
        getDeltaK(),
        getEpsilonH(),
        getDeltaH(),
        getMaxTimeSteps(),
        getMu(),
        getMaxUserContributions(),
        getPerRecordClamp(),
        getThresholdFailureFraction());
  }

  @Override
  protected Map<String, Object> getExtraAADAttributes() {
    return Map.of("epoch", epoch);
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

      // ensure contribution is valid (e.g., non-negative count, userId and word are
      // not empty)
      if (!verifyContribution(update)) {
        throw new IllegalArgumentException("Invalid contribution entry: " + update);
      }

      // add contribution to the mechanism
      this.mechanism.addContribution(userId, word, clamped_count);

      // return acknowledgement response (could be extended to include additional info
      // if needed)
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
      // The payload contains only the dummy marker -- encrypted with the same
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
}
