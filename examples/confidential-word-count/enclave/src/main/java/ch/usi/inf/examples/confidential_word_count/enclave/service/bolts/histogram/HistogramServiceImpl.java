package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.histogram;

import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.enclave.dp.StreamingDPMechanism;
import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveJsonUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.confidential_word_count.common.api.HistogramService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramSnapshotResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramUpdateAckResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramUpdateRequest;
import ch.usi.inf.examples.confidential_word_count.common.config.DPConfig;
import com.google.auto.service.AutoService;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@AutoService(HistogramService.class)
public final class HistogramServiceImpl extends HistogramServiceVerifier {
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(HistogramService.class);
    private final StreamingDPMechanism mechanism;

    // for development purposes, we define the expected JSON fields here to validate the input
    private final Set<String> expectedJsonFields = new HashSet<>(List.of("word", "user_id"));

    @SuppressWarnings("unused")
    public HistogramServiceImpl() {
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
    public HistogramUpdateAckResponse updateImpl(HistogramUpdateRequest update) throws SealedPayloadProcessingException, CipherInitializationException {
        // Decrypt payload to get word and user_id
        String jsonPayload = sealedPayload.decryptToString(update.word());
        Map<String, Object> jsonMap = EnclaveJsonUtil.parseJson(jsonPayload);

        // Validate expected fields
        if (!jsonMap.keySet().containsAll(expectedJsonFields)) {
            log.warn("Invalid payload structure: {}", jsonPayload);
        }

        // Extract word from payload
        String word = (String) jsonMap.get("word");
        if (word == null) {
            log.warn("Missing 'word' field in payload: {}", jsonPayload);
            throw new RuntimeException("Missing 'word' field in payload");
        }

        // Extract user_id from payload
        String userId = String.valueOf(jsonMap.get("user_id")); // long to string
        if (userId == null) {
            log.warn("Missing 'user_id' field in payload: {}", jsonPayload);
            throw new RuntimeException("Missing 'user_id' field in payload");
        }

        // Decrypt count (which is already pre-aggregated -> count >= 1)
        long count = Long.parseLong(sealedPayload.decryptToString(update.count()));
        if (count < 1) {
            log.warn("Invalid count value: {} in payload: {}", count, jsonPayload);
            throw new RuntimeException("Invalid count value in payload");
        }

        // Handle aggregated counts by treating them as multiple unit contributions (ensure contribution bounding is applied)
        for (int i = 0; i < count; i++) {
            // record unit contribution
            if (!mechanism.addContribution(word, 1.0, userId)) {
                log.warn("Contribution for word '{}' from user '{}' was not added (contribution bounding has been exceeded)", word, userId);
            }
        }

        // return acknowledgment to indicate successful processing
        return new HistogramUpdateAckResponse();
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
}
