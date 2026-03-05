package ch.usi.inf.confidentialstorm.host.bolts.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.HistogramAggregationService;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Host-side bolt that receives encrypted partial histograms from DataPerturbation replicas
 * and delegates merging to an enclave-based {@link HistogramAggregationService}.
 * <p>
 * This bolt is a pure merging sink: it receives partial histograms, merges them via the
 * enclave service, and releases complete histograms on a tick schedule.
 * <p>
 * Epoch synchronization is handled out-of-band by {@link EpochBarrierCoordinator} in the
 * upstream DataPerturbation bolts — no back-edge streams are needed.
 */
public abstract class AbstractHistogramAggregationBolt extends ConfidentialBolt<HistogramAggregationService> {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractHistogramAggregationBolt.class);

    private static final int DEFAULT_TICK_INTERVAL_SECS = 5;

    /** The number of upstream DP tasks (set during prepare). */
    private int expectedUpstreamTasks = -1;

    /** The latest complete merged histogram, buffered for tick-based release. */
    private Map<String, Long> lastCompleteHistogram = null;

    /** The epoch of the latest complete histogram. */
    private int lastCompletedEpoch = -1;

    /** Whether a new histogram has been buffered since the last release. */
    private boolean hasNewHistogram = false;

    public AbstractHistogramAggregationBolt() {
        super(HistogramAggregationService.class);
    }

    protected int getTickIntervalSecs() {
        return DEFAULT_TICK_INTERVAL_SECS;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> config = Objects.requireNonNullElse(super.getComponentConfiguration(), new HashMap<>());
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, getTickIntervalSecs());
        return config;
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        super.afterPrepare(topoConf, context);
        configureService(state.getEnclaveManager().getService(), context);

        expectedUpstreamTasks = getExpectedUpstreamTaskCount(context);
        LOG.info("[HistogramAggregation] Expecting {} upstream producers", expectedUpstreamTasks);
    }

    protected abstract int getExpectedUpstreamTaskCount(TopologyContext context);

    protected void configureService(HistogramAggregationService service, TopologyContext context) {
        // hook for subclasses
    }

    protected abstract EncryptedValue getEncryptedPartialHistogram(Tuple input);

    protected abstract void processCompleteHistogram(Map<String, Long> mergedHistogram) throws EnclaveServiceException;

    protected void processStaleHistogram(Map<String, Long> staleHistogram) {
        // hook for subclasses
    }

    @Override
    protected void processTuple(Tuple input, HistogramAggregationService service) throws EnclaveServiceException {
        if (isTickTuple(input)) {
            handleTick();
        } else {
            handlePartialHistogram(input, service);
        }
    }

    private void handleTick() throws EnclaveServiceException {
        if (hasNewHistogram && lastCompleteHistogram != null) {
            LOG.info("[HistogramAggregation] Releasing histogram for epoch {} ({} keys)",
                    lastCompletedEpoch, lastCompleteHistogram.size());
            processCompleteHistogram(lastCompleteHistogram);
            hasNewHistogram = false;
        } else if (lastCompleteHistogram != null) {
            LOG.info("[HistogramAggregation] No new histogram at release tick (last was epoch {})",
                    lastCompletedEpoch);
            processStaleHistogram(lastCompleteHistogram);
        } else {
            LOG.info("[HistogramAggregation] No histogram available yet at release tick");
        }
    }

    private void handlePartialHistogram(Tuple input, HistogramAggregationService service) throws EnclaveServiceException {
        EncryptedValue partial = getEncryptedPartialHistogram(input);
        HistogramAggregationResponse response = service.mergePartial(new HistogramAggregationRequest(partial));

        if (response.isDummy()) {
            LOG.info("[HistogramAggregation] Dummy partial discarded (epoch {})",
                    response.completedEpoch());
        } else if (response.complete()) {
            lastCompleteHistogram = response.mergedHistogram();
            lastCompletedEpoch = response.completedEpoch();
            hasNewHistogram = true;
            LOG.info("[HistogramAggregation] Epoch {} complete ({} keys), buffered for next release tick",
                    lastCompletedEpoch, lastCompleteHistogram.size());
        } else {
            LOG.info("[HistogramAggregation] Partial received ({}/{})",
                    response.receivedCount(), response.expectedCount());
        }

        getCollector().ack(input);
    }
}
