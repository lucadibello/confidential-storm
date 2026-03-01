package ch.usi.inf.confidentialstorm.host.bolts.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.DataPerturbationService;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationContributionEntryRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.EncryptedDataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base implementation for a bolt that performs data perturbation (DP).
 * This bolt delegates the DP mechanism to an enclave-based {@link DataPerturbationService}.
 * <p>
 * <b>Epoch timing</b> is driven by the downstream {@link AbstractHistogramAggregationBolt},
 * which broadcasts "take snapshot" signals on a fixed tick schedule. All DP bolt instances
 * receive the signal at the same time (via allGrouping), ensuring synchronized emission.
 * <p>
 * <b>Background computation</b>: The expensive {@code snapshot()} call runs on a dedicated
 * background thread. When the "take snapshot" signal arrives:
 * <ul>
 *   <li>If the background computation has finished → emit the real encrypted partial.</li>
 *   <li>If it's still running → emit an encrypted dummy partial (indistinguishable from
 *       a real one to the honest-but-curious admin).</li>
 * </ul>
 * After emitting, the next computation is kicked off on the background thread.
 * <p>
 * <b>Startup protocol</b>:
 * <ol>
 *   <li><b>Probe phase</b>: On the first tick (before topology-ready), a blocking probe
 *       snapshot is taken so the aggregator can discover this producer.</li>
 *   <li><b>Steady state</b>: After receiving "topology ready", all further snapshots use
 *       the background thread + signal-driven emission.</li>
 * </ol>
 */
public abstract class AbstractDataPerturbationBolt extends ConfidentialBolt<DataPerturbationService> {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractDataPerturbationBolt.class);

    /**
     * Tick frequency used only for the probe phase during startup.
     * After topology-ready, the aggregator's TAKE_SNAPSHOT_STREAM drives all timing.
     */
    private static final int PROBE_TICK_FREQ_SECS = 5;

    /**
     * Whether the topology-ready signal has been received from the aggregator.
     */
    private boolean topologyReady = false;

    /**
     * Whether the one-time probe snapshot has been sent for producer discovery.
     */
    private boolean probeSent = false;


    /**
     * Whether the next snapshot to emit is the first one since topology-ready.
     */
    private boolean isFirstSnapshot = true;

    /**
     * The last signal epoch processed by this bolt.
     * Used to deduplicate queued take-snapshot signals that Storm may deliver in burst,
     * preventing fast bolts from advancing their enclave epoch multiple times per tick cycle.
     */
    private int lastProcessedSignalEpoch = Integer.MIN_VALUE;

    /**
     * Holds the result of the background snapshot computation.
     * Set by the background thread, consumed by the main bolt thread on the next signal.
     * null means the computation is still in progress (or hasn't started).
     * Transient: initialized in {@link #afterPrepare} (not serializable by Storm).
     */
    private transient AtomicReference<EncryptedDataPerturbationSnapshot> completedSnapshot;

    /**
     * The background thread running the snapshot computation.
     * Only one computation runs at a time.
     */
    private transient Thread snapshotThread;

    /**
     * Lock to serialize access to the enclave service.
     * The StreamingDPMechanism inside the enclave is NOT thread-safe — addContribution()
     * and snapshot() must not run concurrently. This lock is acquired by the main bolt
     * thread (for addContribution and getEncryptedDummyPartial) and by the background
     * snapshot thread (for getEncryptedSnapshot).
     * Transient: initialized in {@link #afterPrepare} (plain Object is not serializable).
     */
    private transient Object serviceLock;

    /**
     * Constructs a new AbstractDataPerturbationBolt.
     */
    public AbstractDataPerturbationBolt() {
        super(DataPerturbationService.class);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        super.afterPrepare(topoConf, context);
        this.completedSnapshot = new AtomicReference<>(null);
        this.snapshotThread = null;
        this.serviceLock = new Object();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> config = Objects.requireNonNullElse(super.getComponentConfiguration(), new HashMap<>());
        // Tick is only used for the probe phase during startup.
        // After topology-ready, the aggregator's TAKE_SNAPSHOT_STREAM drives all timing.
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, PROBE_TICK_FREQ_SECS);
        return config;
    }

    /**
     * Override to return true to use encrypted snapshots instead of plaintext.
     * When enabled, the background-thread + dummy model is activated.
     *
     * @return true to use encrypted snapshots
     */
    protected boolean useEncryptedSnapshots() {
        return false;
    }

    /**
     * Template method to process an encrypted histogram snapshot from the data perturbation service.
     * Only called when {@link #useEncryptedSnapshots()} returns true.
     *
     * @param snapshot the encrypted snapshot (real or dummy — indistinguishable to the host)
     * @throws EnclaveServiceException if there is an error processing the snapshot
     */
    protected void processEncryptedSnapshot(EncryptedDataPerturbationSnapshot snapshot) throws EnclaveServiceException {
        throw new UnsupportedOperationException("Override processEncryptedSnapshot() when useEncryptedSnapshots() is true");
    }

    /**
     * Checks if the tuple is a topology-ready signal from the histogram aggregation bolt.
     */
    private boolean isTopologyReadyTuple(Tuple tuple) {
        return AbstractHistogramAggregationBolt.TOPOLOGY_READY_STREAM
                .equals(tuple.getSourceStreamId());
    }

    /**
     * Checks if the tuple is a "take snapshot" signal from the histogram aggregation bolt.
     */
    private boolean isTakeSnapshotTuple(Tuple tuple) {
        return AbstractHistogramAggregationBolt.TAKE_SNAPSHOT_STREAM
                .equals(tuple.getSourceStreamId());
    }

    @Override
    protected void processTuple(Tuple input, DataPerturbationService service) throws EnclaveServiceException {

        /*
        Control tuple - Tick tuple during probe phase:
            This mechanism allows to signal the presence of this DP bolt to the aggregator before the topology is fully ready.
            The bolt sends a probe snapshot on the first tick tuple it receives (before topology-ready), allowing the aggregator
            to discover this producer. If the probe request gets lost, the bolt will keep sending it on every tick until
            the topology is ready.

            -> These ticks are ignored as soon as the topology-ready signal is received.
         */
        if (isTickTuple(input)) {
            handleProbePhase(input, service);
        }

        /*
        Control tuple - Topology-ready signal:
            HistogramAggregationBolt signals that all DataPerturbationBolts are ready -> produce snapshots next tick
         */
        else if (isTopologyReadyTuple(input)) {
            handleTopologyReady(input);
        }

        /*
        Control tuple - Take-snapshot signal:
            HistogramAggregationBolt signals to produce a snapshot for the current epoch -> emit real or dummy,
            then kick off next computation
         */
        else if (isTakeSnapshotTuple(input)) {
            handleTakeSnapshotSignal(input, service);
        }

        /*
        Normal data tuple: add contribution to the service.
         */
        else {
            // Normal data tuple — must synchronize with background snapshot thread
            synchronized (serviceLock) {
                service.addContribution(new DataPerturbationContributionEntryRequest(
                        getUserIdEntry(input),
                        getWordEntry(input),
                        getClampedCountEntry(input)
                ));
            }
            getCollector().ack(input);
        }
    }

    private void handleProbePhase(Tuple input, DataPerturbationService service) throws EnclaveServiceException {
        if (!topologyReady && !probeSent) {
            LOG.info("[DataPerturbation] Received tick tuple during probe phase - sending probe snapshot for producer discovery");
            if (useEncryptedSnapshots()) {
                EncryptedDataPerturbationSnapshot probeSnapshot = service.getEncryptedSnapshot();
                processEncryptedSnapshot(probeSnapshot);
            } else {
                DataPerturbationSnapshot snapshot = service.getSnapshot();
                processSnapshot(snapshot.histogramSnapshot());
            }
            probeSent = true;
        }
        getCollector().ack(input);
    }

    /**
     * Topology-ready signal: enable the signal-driven emission cycle.
     */
    private void handleTopologyReady(Tuple input) {
        topologyReady = true;
        LOG.info("[DataPerturbation] Topology ready signal received — awaiting first take-snapshot signal");
        getCollector().ack(input);
    }

    /**
     * "Take snapshot" signal from aggregator: emit real or dummy, then kick off next computation.
     */
    private void handleTakeSnapshotSignal(Tuple input, DataPerturbationService service) throws EnclaveServiceException {
        // Ignore signals before topology is ready (shouldn't happen, but be safe)
        if (!topologyReady) {
            getCollector().ack(input);
            return;
        }

        // Deduplicate queued signals: Storm may deliver multiple take-snapshot tuples
        // in burst (e.g., if the bolt was busy with data tuples). Without this guard,
        // fast bolts process all queued signals back-to-back, each advancing the enclave
        // epoch, causing epoch drift relative to slow bolts.
        int signalEpoch = input.getIntegerByField("epoch");
        if (signalEpoch <= lastProcessedSignalEpoch) {
            LOG.debug("[DataPerturbation] Ignoring stale take-snapshot signal (epoch {} <= {})",
                    signalEpoch, lastProcessedSignalEpoch);
            getCollector().ack(input);
            return;
        }
        lastProcessedSignalEpoch = signalEpoch;

        // NOTE: if this code reaches here, means that every bolt has received this signal at more or less the same time
        // (via `allGrouping` from the aggregator) - so we are synchronized!
        if (isFirstSnapshot) {
            // trigger the first snapshot computation - no result will be emitted on this first signal
            LOG.info("[DataPerturbation] First take-snapshot signal received — starting background snapshot cycle");
            isFirstSnapshot = false;
            completedSnapshot.set(null); // ensure no dummy is emitted on the first signal
            startBackgroundSnapshot(service);
            getCollector().ack(input);
            return;
        }

        // Encrypted mode: non-blocking emit with background computation + dummy fallback
        if (useEncryptedSnapshots()) {
            // Collect the result from the background thread (if finished)
            EncryptedDataPerturbationSnapshot result = completedSnapshot.getAndSet(null);

            if (result != null) {
                // Background computation finished — emit the real result and start the next computation
                processEncryptedSnapshot(result);
                startBackgroundSnapshot(service);
            } else if (snapshotThread != null && snapshotThread.isAlive()) {
                // Background computation still running — emit a dummy partial
                LOG.debug("[DataPerturbation] Background snapshot still computing on take-snapshot signal");
                synchronized (serviceLock) {
                    result = service.getEncryptedDummyPartial();
                }
                processEncryptedSnapshot(result);
                // Don't start a new computation — the current one will finish and be picked up on the next signal
            } else {
                // No result and no running thread — this can happen if the previous thread finished
                // but completedSnapshot was already consumed, or on edge cases. Start a new computation.
                LOG.debug("[DataPerturbation] No pending result and no running thread — starting computation");
                startBackgroundSnapshot(service);
                // Emit a dummy for this signal since computation just started
                synchronized (serviceLock) {
                    result = service.getEncryptedDummyPartial();
                }
                processEncryptedSnapshot(result);
            }
        }
        // Plaintext mode: blocking compute-then-emit
        else {
            DataPerturbationSnapshot snapshot = service.getSnapshot();
            processSnapshot(snapshot.histogramSnapshot());
        }

        getCollector().ack(input);
    }

    /**
     * Starts the snapshot computation on a background thread.
     * If a previous computation is still running, this is a no-op — the current
     * computation will eventually finish and its result will be picked up by a future
     * take-snapshot signal. This prevents the main bolt thread from blocking on join()
     * and avoids cascading epoch advancement when queued signals are processed in burst.
     */
    private void startBackgroundSnapshot(DataPerturbationService service) {
        if (snapshotThread != null && snapshotThread.isAlive()) {
            LOG.debug("[DataPerturbation] Previous snapshot still computing — skipping new computation");
            return;
        }

        snapshotThread = new Thread(() -> {
            try {
                EncryptedDataPerturbationSnapshot result;
                synchronized (serviceLock) {
                    result = service.getEncryptedSnapshot();
                }
                completedSnapshot.set(result);
                LOG.debug("[DataPerturbation] Background snapshot computation complete");
            } catch (EnclaveServiceException e) {
                LOG.error("[DataPerturbation] Background snapshot computation failed", e);
                // completedSnapshot remains null — next signal will emit a dummy
            }
        }, "dp-snapshot-" + getTaskId());
        snapshotThread.setDaemon(true);
        snapshotThread.start();
    }

    /**
     * Template method to extract the user ID entry from the input tuple.
     */
    protected abstract EncryptedValue getUserIdEntry(Tuple input);

    /**
     * Template method to extract the word entry from the input tuple.
     */
    protected abstract EncryptedValue getWordEntry(Tuple input);

    /**
     * Template method to extract the (clamped) count entry from the input tuple.
     */
    protected abstract EncryptedValue getClampedCountEntry(Tuple input);

    /**
     * Template method to process the histogram snapshot obtained from the data perturbation service.
     */
    protected abstract void processSnapshot(Map<String, Long> histogramSnapshot) throws EnclaveServiceException;
}
