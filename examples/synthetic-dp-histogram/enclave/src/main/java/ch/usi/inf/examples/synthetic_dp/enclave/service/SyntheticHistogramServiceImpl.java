package ch.usi.inf.examples.synthetic_dp.enclave.service;

import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.enclave.dp.ContributionLimiter;
import ch.usi.inf.confidentialstorm.enclave.dp.StreamingDPMechanism;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticHistogramService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticSnapshotResponse;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticUpdateRequest;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@AutoService(SyntheticHistogramService.class)
public final class SyntheticHistogramServiceImpl extends SyntheticHistogramServiceVerifier
        implements SyntheticHistogramService {

    private final StreamingDPMechanism mechanism;
    private final ContributionLimiter limiter = new ContributionLimiter();
    private final Map<String, Set<String>> windowUsers = new ConcurrentHashMap<>();
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(SyntheticHistogramServiceImpl.class);

    public SyntheticHistogramServiceImpl() {
        log.info("Initializing SyntheticHistogramServiceImpl with DPConfig: {}", DPConfig.describe());
        double rhoK = DPUtil.cdpRho(DPConfig.EPSILON_K, DPConfig.DELTA_K);
        double sigmaKey = DPUtil.calculateSigma(rhoK, DPConfig.MAX_TIME_STEPS, 1.0);

        double rhoH = DPUtil.cdpRho(DPConfig.EPSILON_H, DPConfig.DELTA_H);
        double sigmaHist = DPUtil.calculateSigma(rhoH, DPConfig.MAX_TIME_STEPS, DPConfig.l1Sensitivity());

        this.mechanism = new StreamingDPMechanism(
                sigmaKey,
                sigmaHist,
                DPConfig.MAX_TIME_STEPS,
                DPConfig.MU
        );
    }

    @Override
    public void updateImpl(SyntheticUpdateRequest request) throws SealedPayloadProcessingException, CipherInitializationException {
        String key = sealedPayload.decryptToString(request.key());
        double raw = Double.parseDouble(sealedPayload.decryptToString(request.count()));
        String userId = sealedPayload.decryptToString(request.userId());

        double clamped = Math.max(-DPConfig.PER_RECORD_CLAMP, Math.min(raw, DPConfig.PER_RECORD_CLAMP));
        if (!limiter.allow(userId, DPConfig.MAX_CONTRIBUTIONS_PER_USER)) {
            log.info("Dropping contribution for user {} due to bounding", userId);
            return;
        }
        log.debug("Adding contribution key={}, user={}, value={}", key, userId, clamped);
        mechanism.addContribution(key, clamped, userId);
    }

    @Override
    public SyntheticSnapshotResponse snapshot() {
        Map<String, Long> snap = mechanism.snapshot();
        log.info("Emitted DP snapshot with {} keys", snap.size());
        return new SyntheticSnapshotResponse(snap);
    }

}
