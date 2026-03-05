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
 * <b>Epoch synchronization</b> uses a ZK shared epoch counter via {@link EpochBarrierCoordinator}.
 * The global counter ({@code /current-epoch}) only advances when ALL replicas have completed
 * the current epoch, ensuring all replicas call {@code getEncryptedSnapshot()} the same number
 * of times — keeping enclave timeSteps perfectly in sync.
 * <p>
 * <b>Non-blocking tick handler</b> (3-phase state machine):
 * <ol>
 *   <li><b>Phase 1 — Emit:</b> If the background thread has finished, emit the real partial,
 *       increment {@code localEpoch}, and register completion in ZK. Otherwise emit a dummy.</li>
 *   <li><b>Phase 2 — Start:</b> If {@code targetEpoch > localEpoch} and no background thread
 *       is running, start a new background snapshot computation.</li>
 *   <li><b>Phase 3 — Advance:</b> Leader checks if all replicas completed, advances global epoch.</li>
 * </ol>
 */
public abstract class AbstractDataPerturbationBolt extends ConfidentialBolt<DataPerturbationService> {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractDataPerturbationBolt.class);

    private static final int DEFAULT_TICK_INTERVAL_SECS = 5;
    private static final int DEFAULT_EPOCH_TIMEOUT_SECS = 30;

    /**
     * How many times this replica has called getEncryptedSnapshot() and emitted the result.
     * Must stay in sync with the enclave's internal epoch/timeStep.
     */
    private int localEpoch = 0;

    /**
     * Holds the result of the background snapshot computation.
     * Set by the background thread, consumed by the main bolt thread on the next tick.
     * null means the computation is still in progress (or hasn't started).
     */
    private transient AtomicReference<EncryptedDataPerturbationSnapshot> completedSnapshot;

    /**
     * The background thread running the snapshot computation.
     */
    private transient Thread snapshotThread;

    /**
     * Lock to serialize access to the enclave service.
     * The StreamingDPMechanism inside the enclave is NOT thread-safe — addContribution()
     * and snapshot() must not run concurrently.
     */
    private transient Object serviceLock;

    private transient EpochBarrierCoordinator coordinator;

    public AbstractDataPerturbationBolt() {
        super(DataPerturbationService.class);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        super.afterPrepare(topoConf, context);
        this.completedSnapshot = new AtomicReference<>(null);
        this.snapshotThread = null;
        this.serviceLock = new Object();

        int totalReplicas = context.getComponentTasks(context.getThisComponentId()).size();
        int minTaskId = context.getComponentTasks(context.getThisComponentId()).stream()
                .mapToInt(Integer::intValue).min().orElse(getTaskId());
        boolean isLeader = getTaskId() == minTaskId;

        this.coordinator = new EpochBarrierCoordinator(
                topoConf, context.getStormId(), getTaskId(),
                totalReplicas, DEFAULT_EPOCH_TIMEOUT_SECS, isLeader);

        coordinator.awaitStartup(() ->
                LOG.info("[DataPerturbation] Task {} ready - starting epoch processing", getTaskId()));
    }

    @Override
    protected void beforeCleanup() {
        if (coordinator != null) {
            try {
                coordinator.close();
            } catch (Exception e) {
                LOG.warn("[DataPerturbation] Error closing EpochBarrierCoordinator", e);
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> config = Objects.requireNonNullElse(super.getComponentConfiguration(), new HashMap<>());
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, getTickIntervalSecs());
        return config;
    }

    /**
     * Returns the tick interval in seconds. Subclasses may override.
     */
    protected int getTickIntervalSecs() {
        return DEFAULT_TICK_INTERVAL_SECS;
    }

    /**
     * Override to return true to use encrypted snapshots instead of plaintext.
     */
    protected boolean useEncryptedSnapshots() {
        return false;
    }

    /**
     * Template method to process an encrypted histogram snapshot from the data perturbation service.
     * Only called when {@link #useEncryptedSnapshots()} returns true.
     */
    protected void processEncryptedSnapshot(EncryptedDataPerturbationSnapshot snapshot) throws EnclaveServiceException {
        throw new UnsupportedOperationException("Override processEncryptedSnapshot() when useEncryptedSnapshots() is true");
    }

    @Override
    protected void processTuple(Tuple input, DataPerturbationService service) throws EnclaveServiceException {
        // Tick tuple: handle epoch synchronization and snapshot emission
        if (isTickTuple(input)) {
            handleEpochTick(service);
        }
        // Data tuple: add contribution to the service
        else {
            synchronized (serviceLock) {
                service.addContribution(new DataPerturbationContributionEntryRequest(
                        getUserIdEntry(input),
                        getWordEntry(input),
                        getClampedCountEntry(input)
                ));
            }
        }

        // ack input
        getCollector().ack(input);
    }

    /**
     * Non-blocking 3-phase tick handler using ZK shared epoch counter.
     */
    private void handleEpochTick(DataPerturbationService service) throws EnclaveServiceException {
        // if startup not complete, do nothing (startup phase)
        if (!coordinator.isReady()) {
            LOG.debug("[DataPerturbation] Task {} ignoring tick - startup not complete", getTaskId());
            return;
        }

        // get the current global epoch target from ZK (all replicas should be in sync on this)
        int targetEpoch = coordinator.getTargetEpoch();

        if (useEncryptedSnapshots()) {
            EncryptedDataPerturbationSnapshot result = completedSnapshot.getAndSet(null);

            // PHASE 1: emit snapshot for the current epoch (real if ready, dummy if still computing)
            if (result != null) {
                // Background thread finished - emit real partial
                processEncryptedSnapshot(result);
                localEpoch++;

                // record that this replica was able to produce the snapshot in time for this epoch
                coordinator.registerCompletion(localEpoch);
                LOG.info("[DataPerturbation] Task {} emitted real partial for epoch {}", getTaskId(), localEpoch);
            } else if (targetEpoch > localEpoch) {
                // Background thread still computing — emit dummy to keep aggregator fed
                synchronized (serviceLock) {
                    processEncryptedSnapshot(service.getEncryptedDummyPartial());
                }
                LOG.debug("[DataPerturbation] Task {} emitted dummy (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, targetEpoch);
            }

            // PHASE 2: Start a new background computation only if we didn't already produce a real snapshot for this
            // epoch (localEpoch < targetEpoch) AND we are not computing it right now (snapshotThread not alive)
            if (targetEpoch > localEpoch && (snapshotThread == null || !snapshotThread.isAlive())) {
                LOG.info("[DataPerturbation] Task {} starting bg snapshot (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, targetEpoch);
                startBackgroundSnapshot(service);
            }

            // PHASE 3: Leader tries to advance global epoch
            // NOTE: when everyone has registered completion for the current epoch, the leader will advance the global
            // epoch, which will trigger the next round of snapshot computation on the next tick. If some replicas are
            // slow and miss the current epoch (i.e. they haven't registered completion), the leader will not advance the epoch,
            // and those replicas will keep emitting dummies until they can complete the snapshot and register completion for the current epoch.
            coordinator.tryAdvanceEpoch(targetEpoch);
        } else {
            // Plaintext mode: blocking compute-then-emit (no parallelism concerns in plaintext)
            if (targetEpoch > localEpoch) {
                synchronized (serviceLock) {
                    DataPerturbationSnapshot snapshot = service.getSnapshot();
                    if (snapshot != null) {
                        processSnapshot(snapshot.histogramSnapshot());
                    }
                }
                localEpoch++;
                coordinator.registerCompletion(localEpoch);
                coordinator.tryAdvanceEpoch(targetEpoch);
            }
        }
    }

    /**
     * Starts the snapshot computation on a background thread.
     */
    private void startBackgroundSnapshot(DataPerturbationService service) {
        if (snapshotThread != null && snapshotThread.isAlive()) {
            LOG.info("[DataPerturbation] Previous snapshot still computing - skipping new computation");
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
            }
        }, "dp-snapshot-" + getTaskId());
        snapshotThread.setDaemon(true);
        snapshotThread.start();
    }

    protected abstract EncryptedValue getUserIdEntry(Tuple input);
    protected abstract EncryptedValue getWordEntry(Tuple input);
    protected abstract EncryptedValue getClampedCountEntry(Tuple input);
    protected abstract void processSnapshot(Map<String, Long> histogramSnapshot) throws EnclaveServiceException;
}
