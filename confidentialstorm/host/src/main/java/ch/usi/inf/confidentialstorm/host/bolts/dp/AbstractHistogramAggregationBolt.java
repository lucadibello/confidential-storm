package ch.usi.inf.confidentialstorm.host.bolts.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.HistogramAggregationService;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Host-side bolt that receives encrypted partial histograms from DataPerturbation replicas
 * and delegates merging to an enclave-based {@link HistogramAggregationService}.
 * <p>
 * This bolt is the <b>single source of truth for epoch timing</b>. It:
 * <ol>
 *   <li>Runs a tick timer (the only tick in the topology).</li>
 *   <li>On each tick, broadcasts a "take snapshot" signal to all DP bolts via
 *       {@link #TAKE_SNAPSHOT_STREAM} (allGrouping).</li>
 *   <li>Releases the latest complete histogram on a fixed schedule.</li>
 * </ol>
 * <p>
 * Additionally uses a one-time {@link #TOPOLOGY_READY_STREAM} to signal all DP bolts
 * that all producers have been discovered during the startup probe phase.
 */
public abstract class AbstractHistogramAggregationBolt extends ConfidentialBolt<HistogramAggregationService> {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractHistogramAggregationBolt.class);

    /**
     * Named stream for the one-time "topology ready" signal (all producers discovered).
     * DP bolts subscribe via allGrouping.
     */
    public static final String TOPOLOGY_READY_STREAM = "topology-ready";

    /**
     * Named stream for periodic "take snapshot" signals broadcast to all DP bolts.
     * Fired on every tick after topology is ready. DP bolts subscribe via allGrouping.
     * This is the single epoch clock for the entire topology.
     */
    public static final String TAKE_SNAPSHOT_STREAM = "take-snapshot";

    /**
     * Default tick interval in seconds
     */
    private static final int DEFAULT_TICK_INTERVAL_SECS = 5;

    /** Set of Storm task IDs from upstream DP bolts that have sent at least one partial. */
    private final Set<Integer> knownProducerIds = new HashSet<>();

    /** Whether all upstream producers have been discovered. */
    private boolean topologyReady = false;

    /** The number of upstream DP tasks (set during prepare). */
    private int expectedUpstreamTasks = -1;

    /** The latest complete merged histogram, buffered for tick-based release. */
    private Map<String, Long> lastCompleteHistogram = null;

    /** The epoch of the latest complete histogram. */
    private int lastCompletedEpoch = -1;

    /** Whether a new histogram has been buffered since the last release. */
    private boolean hasNewHistogram = false;

    /** Monotonically increasing snapshot signal counter. */
    private int snapshotSignalEpoch = -1;

    /**
     * Whether the aggregator is waiting for the current epoch to complete before
     * broadcasting the next take-snapshot signal. After the priming signal (epoch -1),
     * a new signal is only broadcast once the previous epoch's histogram is complete.
     * This prevents fast DP bolts from racing ahead of slow ones.
     */
    private boolean awaitingEpochCompletion = false;

    /**
     * Constructs a new AbstractHistogramAggregationBolt.
     */
    public AbstractHistogramAggregationBolt() {
        super(HistogramAggregationService.class);
    }

    /**
     * Returns the tick interval in seconds. Subclasses may override.
     */
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
        LOG.info("[HistogramAggregation] Waiting for {} upstream producers to come online",
                expectedUpstreamTasks);
    }

    /**
     * Returns the number of upstream DP tasks. Subclasses must implement this.
     */
    protected abstract int getExpectedUpstreamTaskCount(TopologyContext context);

    /**
     * Template method for subclass configuration after enclave init.
     */
    protected void configureService(HistogramAggregationService service, TopologyContext context) {
        // hook for subclasses
    }

    /**
     * Extract the encrypted partial histogram from the input tuple.
     */
    protected abstract EncryptedValue getEncryptedPartialHistogram(Tuple input);

    /**
     * Called when a complete histogram is released on the tick schedule.
     */
    protected abstract void processCompleteHistogram(Map<String, Long> mergedHistogram) throws EnclaveServiceException;

    /**
     * Called on a release tick when no new histogram has arrived since the last release.
     * Default: no-op. Subclasses can override for constant-rate output.
     */
    protected void processStaleHistogram(Map<String, Long> staleHistogram) {
        // hook for subclasses
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // topology-ready stream (one-time): signals all DP bolts that all producers have been discovered and topology is ready for snapshotting
        declarer.declareStream(TOPOLOGY_READY_STREAM, new Fields("ready"));
        // take-snapshot stream (periodic): signals all DP bolts to take a snapshot of their current data for the next epoch
        declarer.declareStream(TAKE_SNAPSHOT_STREAM, new Fields("epoch"));
    }

    @Override
    protected void processTuple(Tuple input, HistogramAggregationService service) throws EnclaveServiceException {
        // handle control tuples
        if (isTickTuple(input)) {
            handleTick();
        }
        // handle data tuples from DataPerturbationBolt replicas
        else {
            handlePartialHistogram(input, service);
        }
    }

    /**
     * Tick handler: release histogram AND broadcast snapshot signal to DP bolts.
     * This is the single epoch clock for the topology.
     */
    private void handleTick() throws EnclaveServiceException {
        // 1. Release confidential histogram
        // A. If a new complete histogram has arrived since the last release, emit it and reset flag
        if (hasNewHistogram && lastCompleteHistogram != null) {
            LOG.info("[HistogramAggregation] Releasing histogram for epoch {} ({} keys)",
                    lastCompletedEpoch, lastCompleteHistogram.size());
            processCompleteHistogram(lastCompleteHistogram);
            hasNewHistogram = false;
        }
        // B. If histogram is stale (no new complete since last release), let subclass decide how to handle (e.g. re-emit last histogram for constant-rate output)
        else if (lastCompleteHistogram != null) {
            LOG.debug("[HistogramAggregation] No new histogram at release tick (last was epoch {})",
                    lastCompletedEpoch);
            processStaleHistogram(lastCompleteHistogram);
        }
        // C. No histogram available yet (e.g. during startup)
        else {
            LOG.debug("[HistogramAggregation] No histogram available yet at release tick");
        }

        // 2. Broadcast "take snapshot" signal to all DP bolts (only after topology is ready)
        //    Gate: after the priming signal, only broadcast a new signal once the previous
        //    epoch's histogram is complete. This prevents fast bolts from racing ahead —
        //    all replicas stay on the same epoch because the aggregator only requests the
        //    next snapshot once it has received all partials for the current epoch.
        if (topologyReady && !awaitingEpochCompletion) {
            LOG.debug("[HistogramAggregation] Broadcasting take-snapshot signal (epoch {})", snapshotSignalEpoch);
            getCollector().emit(TAKE_SNAPSHOT_STREAM, new Values(snapshotSignalEpoch));
            snapshotSignalEpoch++;
            // After the priming signal (epoch -1), wait for completion before sending the next
            if (snapshotSignalEpoch > 0) {
                awaitingEpochCompletion = true;
            }
        }
    }

    /**
     * Process an incoming partial histogram.
     */
    private void handlePartialHistogram(Tuple input, HistogramAggregationService service) throws EnclaveServiceException {
        // get partial histogram from tuple and merge via service
        EncryptedValue partial = getEncryptedPartialHistogram(input);
        HistogramAggregationResponse response = service.mergePartial(new HistogramAggregationRequest(partial));

        // Check whether the topology is not yet ready
        if (!topologyReady) {
            int sourceTaskId = input.getSourceTask();
            knownProducerIds.add(sourceTaskId);

            if (knownProducerIds.size() >= expectedUpstreamTasks) {
                topologyReady = true;
                LOG.info("[HistogramAggregation] All {} producers online, emitting topology-ready signal",
                        expectedUpstreamTasks);
                getCollector().emit(TOPOLOGY_READY_STREAM, new Values(true));
            } else {
                LOG.info("[HistogramAggregation] Producer discovery: {}/{} (new: task {})",
                        knownProducerIds.size(), expectedUpstreamTasks, sourceTaskId);
            }
        }


        // now, handle the response from merging this partial:
        // A) if response is dummy, log and discard
        if (response.isDummy()) {
            LOG.debug("[HistogramAggregation] Dummy partial discarded (epoch {})",
                    response.completedEpoch());
        }
        // B) if response is complete, buffer histogram for next release tick
        else if (response.complete()) {
            lastCompleteHistogram = response.mergedHistogram();
            lastCompletedEpoch = response.completedEpoch();
            hasNewHistogram = true;
            // Ungate: allow the next take-snapshot signal to be broadcast on the next tick
            awaitingEpochCompletion = false;
            LOG.info("[HistogramAggregation] Epoch {} complete ({} keys), buffered for next release tick",
                    lastCompletedEpoch, lastCompleteHistogram.size());
        }
        // C) otherwise, it's a partial response that has been merged but is not yet complete - log and wait for more
        else {
            LOG.debug("[HistogramAggregation] Partial received ({}/{})",
                    response.receivedCount(), response.expectedCount());
        }

        getCollector().ack(input);
    }
}
