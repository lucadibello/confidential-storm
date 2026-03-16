package ch.usi.inf.confidentialstorm.host.bolts.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.HistogramAggregationService;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import ch.usi.inf.confidentialstorm.host.profiling.ProfilerConfig;
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
 * upstream DataPerturbation bolts -- no back-edge streams are needed.
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

    /** Set to true when the bolt has reached {@link #getMaxEpochs()} and should stop processing. */
    private boolean finished = false;

    private transient int ticksSinceLastCompletion;
    private transient int dummyMergesThisEpoch;
    private transient int realMergesThisEpoch;

    public AbstractHistogramAggregationBolt() {
        super(HistogramAggregationService.class);
    }

    protected int getTickIntervalSecs() {
        return DEFAULT_TICK_INTERVAL_SECS;
    }

    /**
     * Returns the maximum number of epochs to process before deactivating.
     * After this many epochs, the bolt stops processing and flushes the profiler.
     * Subclasses may override to set a finite limit (e.g., from DP configuration).
     *
     * @return the max epoch count, or {@code -1} (default) to run indefinitely.
     */
    protected int getMaxEpochs() {
        return -1;
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

        if (ProfilerConfig.ENABLED) {
            getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.TICK_INTERVAL_SECS, getTickIntervalSecs());
            getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.MAX_EPOCHS_CONFIGURED, getMaxEpochs());
        }
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
        // check if user has configured a max epoch limit and we've already reached it
        if (finished) {
            if (!isTickTuple(input)) getCollector().ack(input);
            return;
        }

        // distinguish between tick tuples (for releasing histograms) and regular tuples (partial histograms to merge)
        if (isTickTuple(input)) {
            handleTick();
        } else {
            handlePartialHistogram(input, service);
        }
    }

    private void handleTick() throws EnclaveServiceException {
        if (ProfilerConfig.ENABLED) {
            ticksSinceLastCompletion++;
            getProfiler().onTick();
        }

        if (hasNewHistogram && lastCompleteHistogram != null) {
            LOG.info("[HistogramAggregation] Releasing histogram for epoch {} ({} keys)",
                    lastCompletedEpoch, lastCompleteHistogram.size());
            processCompleteHistogram(lastCompleteHistogram);
            hasNewHistogram = false;
            if (ProfilerConfig.ENABLED) getProfiler().incrementCounter("histograms_released");
        } else if (lastCompleteHistogram != null) {
            LOG.info("[HistogramAggregation] No new histogram at release tick (last was epoch {})",
                    lastCompletedEpoch);
            processStaleHistogram(lastCompleteHistogram);
            if (ProfilerConfig.ENABLED) getProfiler().incrementCounter("stale_releases");
        } else {
            LOG.info("[HistogramAggregation] No histogram available yet at release tick");
        }
    }

    private void handlePartialHistogram(Tuple input, HistogramAggregationService service) throws EnclaveServiceException {
        EncryptedValue partial = getEncryptedPartialHistogram(input);

        long t0 = ProfilerConfig.ENABLED && getProfiler().shouldSample() ? System.nanoTime() : 0;
        HistogramAggregationResponse response = service.mergePartial(new HistogramAggregationRequest(partial));
        if (t0 != 0) getProfiler().recordEcall("mergePartial", System.nanoTime() - t0);
        if (ProfilerConfig.ENABLED) getProfiler().incrementEcallTotal("mergePartial");

        if (response.isDummy()) {
            LOG.info("[HistogramAggregation] Dummy partial discarded (epoch {})",
                    response.completedEpoch());
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementCounter("dummies_received");
                dummyMergesThisEpoch++;
            }
        } else if (response.complete()) {
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementCounter("real_partials_received");
                realMergesThisEpoch++; // the latest partial completed the histogram
            }
            lastCompleteHistogram = response.mergedHistogram();
            lastCompletedEpoch = response.completedEpoch();
            hasNewHistogram = true;
            LOG.info("[HistogramAggregation] Epoch {} complete ({} keys), buffered for next release tick",
                    lastCompletedEpoch, lastCompleteHistogram.size());

            if (ProfilerConfig.ENABLED) {
                getProfiler().recordGauge("ticks_to_completion", ticksSinceLastCompletion);
                getProfiler().recordGauge("dummy_merges_this_epoch", dummyMergesThisEpoch);
                getProfiler().recordGauge("real_merges_this_epoch", realMergesThisEpoch);
                getProfiler().recordGauge("total_merges_this_epoch", dummyMergesThisEpoch + realMergesThisEpoch);
                getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.EPOCH_ADVANCED, lastCompletedEpoch);

                ticksSinceLastCompletion = 0;
                realMergesThisEpoch = 0;
                dummyMergesThisEpoch = 0;
            }

            // if the user has configured a max epoch limit, check if we've reached it and deactivate the bolt if so
            if (getMaxEpochs() > 0 && lastCompletedEpoch >= getMaxEpochs()) {
                finished = true;
                LOG.info("[HistogramAggregation] Reached max epochs ({}), deactivating", getMaxEpochs());
                if (ProfilerConfig.ENABLED) {
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.MAX_EPOCH_REACHED, lastCompletedEpoch);
                    getProfiler().writeReport();
                }
            }
        } else {
            LOG.info("[HistogramAggregation] Partial received ({}/{})",
                    response.receivedCount(), response.expectedCount());
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementCounter("real_partials_received");
                realMergesThisEpoch++;
            }
        }

        getCollector().ack(input);
    }
}
