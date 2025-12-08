package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.histogram;

import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.enclave.dp.StreamingDPMechanism;
import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;
import ch.usi.inf.examples.confidential_word_count.common.api.HistogramService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramSnapshotResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramUpdateRequest;
import ch.usi.inf.examples.confidential_word_count.common.config.DPConfig;
import com.google.auto.service.AutoService;

import java.util.Map;
import org.apache.commons.math3.util.FastMath;

@AutoService(HistogramService.class)
public final class HistogramServiceImpl extends HistogramServiceVerifier {
    private final StreamingDPMechanism mechanism;

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
                DPConfig.MU
        );
    }

    @Override
    public void updateImpl(HistogramUpdateRequest update) throws SealedPayloadProcessingException, CipherInitializationException {
        String word = sealedPayload.decryptToString(update.word());
        double rawCount = Double.parseDouble(sealedPayload.decryptToString(update.count()));

        // clamp the contribution to [-C, C]
        double clamp = DPConfig.PER_RECORD_CLAMP;
        double count = FastMath.max(-clamp, FastMath.min(rawCount, clamp));

        // Extract user_id from AAD
        DecodedAAD aad = DecodedAAD.fromBytes(update.word().associatedData());
        Object userIdObj = aad.attributes().get("user_id");
        String userId = userIdObj != null ? userIdObj.toString() : "unknown";

        // Buffer the contribution
        mechanism.addContribution(word, count, userId);
    }

    @Override
    public HistogramSnapshotResponse snapshot() {
        // Advance time step and get the latest noisy histogram
        Map<String, Long> histogram = mechanism.snapshot();
        return new HistogramSnapshotResponse(histogram);
    }
}
