package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.dp.StreamingDPMechanism;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.confidential_word_count.common.api.histogram.HistogramService;
import ch.usi.inf.examples.confidential_word_count.common.api.histogram.model.HistogramSnapshotResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.histogram.model.HistogramUpdateAckResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.histogram.model.HistogramUpdateRequest;
import ch.usi.inf.examples.confidential_word_count.common.config.DPConfig;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

import java.util.Map;
import java.util.Objects;

@AutoService(HistogramService.class)
public final class HistogramServiceProvider extends ConfidentialBoltService<HistogramUpdateRequest> implements HistogramService {
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(HistogramService.class);
    private final StreamingDPMechanism mechanism;

    @SuppressWarnings("unused")
    public HistogramServiceProvider() {
        // Calibrate noise for Key Selection (Sensitivity = 1)
        double rhoK = DPUtil.cdpRho(DPConfig.EPSILON_K, DPConfig.DELTA_K);
        double sigmaKey = DPUtil.calculateSigma(rhoK, DPConfig.MAX_TIME_STEPS, 1.0);

        // Calibrate noise for Histogram (Sensitivity = C * L_m)
        double rhoH = DPUtil.cdpRho(DPConfig.EPSILON_H, DPConfig.DELTA_H);
        double l1Sensitivity = DPConfig.l1Sensitivity();
        double sigmaHist = DPUtil.calculateSigma(rhoH, DPConfig.MAX_TIME_STEPS, l1Sensitivity);

        // Initialize the standard mechanism
        this.mechanism = new StreamingDPMechanism(
                sigmaKey,
                sigmaHist,
                DPConfig.MAX_TIME_STEPS,
                DPConfig.MU,
                DPConfig.MAX_CONTRIBUTIONS_PER_USER,
                DPConfig.PER_RECORD_CLAMP
        );
    }

    @Override
    public HistogramUpdateAckResponse update(HistogramUpdateRequest update) throws EnclaveServiceException {
        try {
            // Decrypt word
            String word = Objects.requireNonNull(decryptToString(update.word()), "Missing 'word' field in payload");
            // Decrypt userId
            String userId = Objects.requireNonNull(decryptToString(update.userId()), "Missing 'userId' field in payload");
            // Decrypt count (which is already pre-aggregated and clamped)
            double clamped_count = decryptToDouble(update.count());

            // Register contribution from user to the DP mechanism
            mechanism.addContribution(word, clamped_count, userId);

            // Return acknowledgment to indicate successful processing
            return new HistogramUpdateAckResponse();
        } catch (Throwable t) {
            this.exceptionCtx.handleException(t);
            return null; // signal error if using the default exception handler
        }
    }

    @Override
    public HistogramSnapshotResponse snapshot() throws EnclaveServiceException {
        // Advance time step and get the latest noisy histogram
        try {
            Map<String, Long> histogram = mechanism.snapshot();
            return new HistogramSnapshotResponse(histogram);
        } catch (Throwable t) {
            this.exceptionCtx.handleException(t);
            return null; // signal error if using the default exception handler
        }
    }

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return ComponentConstants.USER_CONTRIBUTION_BOUNDING;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return ComponentConstants.HISTOGRAM_GLOBAL; // ownership remains here
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.HISTOGRAM_GLOBAL;
    }
}
