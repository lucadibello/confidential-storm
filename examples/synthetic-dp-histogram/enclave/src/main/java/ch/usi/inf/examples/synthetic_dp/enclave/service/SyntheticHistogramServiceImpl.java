package ch.usi.inf.examples.synthetic_dp.enclave.service;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.dp.StreamingDPMechanism;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticHistogramService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticSnapshotResponse;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticUpdateRequest;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;

import com.google.auto.service.AutoService;
import java.util.Map;

@AutoService(SyntheticHistogramService.class)
public final class SyntheticHistogramServiceImpl
        extends ConfidentialBoltService<SyntheticUpdateRequest>
        implements SyntheticHistogramService {

    private StreamingDPMechanism mechanism;
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(SyntheticHistogramServiceImpl.class);

    public SyntheticHistogramServiceImpl() {
        log.debug("Initializing SyntheticHistogramServiceImpl with default DPConfig: {}", DPConfig.describe());
        // Initialize with defaults from DPConfig (these might be overridden by configure)
        initMechanism(DPConfig.MAX_TIME_STEPS, DPConfig.MU);
    }

    @Override
    public void configure(int maxTimeSteps, long mu) {
        log.info("Configuring SyntheticHistogramServiceImpl: maxTimeSteps={}, mu={}", maxTimeSteps, mu);
        initMechanism(maxTimeSteps, mu);
    }

    private void initMechanism(int maxTimeSteps, long mu) {
        // get privacy parameters for keys
        double rhoK = DPUtil.cdpRho(DPConfig.EPSILON_K, DPConfig.DELTA_K);
        double sigmaKey = DPUtil.calculateSigma(rhoK, maxTimeSteps, 1.0);

        // get privacy parameters for histogram
        double rhoH = DPUtil.cdpRho(DPConfig.EPSILON_H, DPConfig.DELTA_H);
        double sigmaHist = DPUtil.calculateSigma(rhoH, maxTimeSteps, DPConfig.l1Sensitivity());

        // initialize DP mechanism
        this.mechanism = new StreamingDPMechanism(
                sigmaKey,
                sigmaHist,
                maxTimeSteps,
                mu,
                DPConfig.MAX_CONTRIBUTIONS_PER_USER,
                DPConfig.PER_RECORD_CLAMP
        );
    }


    @Override
    public void update(SyntheticUpdateRequest request) throws EnclaveServiceException {
        try {
            // verify request
            super.verify(request);

            String key = decryptToString(request.key());
            double count = decryptToDouble(request.count());
            String userId = decryptToString(request.userId());

            // record contribution to DP mechanism
            // NOTE: we assume that the count has already been clamped per-record if needed!
            mechanism.addContribution(key, count, userId);
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
        }
    }

    @Override
    public SyntheticSnapshotResponse snapshot() {
        log.debug("[ENCLAVE] snapshot() called - entering StreamingDPMechanism.snapshot()");
        Map<String, Long> snap = mechanism.snapshot();
        log.debug("[ENCLAVE] snapshot() completed - returning {} keys", snap.size());

        // return response
        return new SyntheticSnapshotResponse(snap);
    }

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return ComponentConstants.SPOUT;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return ComponentConstants.HISTOGRAM_GLOBAL;
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.HISTOGRAM_GLOBAL;
    }
}
