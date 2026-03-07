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
 * <b>Epoch synchronization</b> uses a Curator {@code SharedCount} via {@link EpochBarrierCoordinator}.
 * The global counter ({@code /current-epoch}) only advances when ALL replicas have completed
 * the current epoch, ensuring all replicas call {@code getEncryptedSnapshot()} the same number
 * of times, keeping enclave timeSteps perfectly in sync.
 * <p>
 * <b>Tick-driven emission</b>: Every replica emits exactly once per tick tuple -- either the
 * real partial (if the background computation finished) or a dummy (if still computing).
 * <p>
 * <b>Listener-driven computation start</b>: A {@code SharedCountListener} on the epoch counter
 * notifies all replicas immediately when the epoch advances. The listener callback starts the
 * next background computation without waiting for the next tick, eliminating wasted ticks.
 * <p>
 * <b>Tick lifecycle</b>:
 * <ol>
 *   <li>Tick arrives -> read targetEpoch (locally cached by Curator's {@code SharedCount})</li>
 *   <li>Fallback: if behind target and no bg thread running (listener missed), start bg thread</li>
 *   <li>If bg thread finished: emit real partial, advance localEpoch, register completion</li>
 *   <li>If bg thread still running or just started: emit dummy (except on the very first tick)</li>
 *   <li>Try to advance global epoch via versioned CAS</li>
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
     * The StreamingDPMechanism inside the enclave is NOT thread-safe -- addContribution()
     * and snapshot() must not run concurrently.
     */
    private transient Object serviceLock;

    /**
     * Guards against epoch chaining: set to true when a real partial is emitted
     * during a tick, reset at the start of each tick. Prevents the SharedCountListener
     * from starting a new background snapshot if we already advanced this tick.
     */
    private volatile boolean advancedThisTick = false;

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

        // Register the SharedCountListener callback: when the global epoch advances,
        // start the next background computation immediately (from the Curator event thread).
        DataPerturbationService service = state.getEnclaveManager().getService();
        coordinator.setOnEpochAdvanced(() -> {
            if (!advancedThisTick
                    && coordinator.getTargetEpoch() > localEpoch
                    && (snapshotThread == null || !snapshotThread.isAlive())) {
                LOG.info("[DataPerturbation] Task {} starting bg snapshot from watch (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, coordinator.getTargetEpoch());
                startBackgroundSnapshot(service);
            }
        });

        coordinator.awaitStartup(() ->
                LOG.info("[DataPerturbation] Task {} ready -- starting epoch processing", getTaskId()));
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
        if (isTickTuple(input)) {
            handleEpochTick(service);
        } else {
            synchronized (serviceLock) {
                service.addContribution(new DataPerturbationContributionEntryRequest(
                        getUserIdEntry(input),
                        getWordEntry(input),
                        getClampedCountEntry(input)
                ));
            }
        }
        getCollector().ack(input);
    }

    /**
     * Tick-driven epoch handler. Every tick, each replica either:
     * - Emits a real partial (bg thread finished), OR
     * - Emits a dummy (bg thread still running or just started by listener), OR
     * - Does nothing (very first tick, localEpoch==0 and no result yet).
     *
     * Background computation is started reactively by the {@code SharedCountListener}
     * callback when the epoch advances. The tick handler only starts a computation as
     * a fallback if the listener notification was missed.
     */
    private void handleEpochTick(DataPerturbationService service) throws EnclaveServiceException {
        if (!coordinator.isReady()) {
            LOG.debug("[DataPerturbation] Task {} ignoring tick -- startup not complete", getTaskId());
            return;
        }

        int targetEpoch = coordinator.getTargetEpoch();

        if (useEncryptedSnapshots()) {
            handleEncryptedTick(service, targetEpoch);
        } else {
            handlePlaintextTick(service, targetEpoch);
        }
    }

    private void handleEncryptedTick(DataPerturbationService service, int targetEpoch) throws EnclaveServiceException {
        advancedThisTick = false;

        // 1. Check if a previously started bg computation finished.
        //    IMPORTANT: must check BEFORE the fallback to avoid starting a redundant
        //    snapshot when the listener-started one already completed (which would
        //    call getEncryptedSnapshot() twice, double-incrementing the enclave epoch).
        EncryptedDataPerturbationSnapshot result = completedSnapshot.getAndSet(null);

        if (result != null) {
            // Background thread finished -- emit real partial
            processEncryptedSnapshot(result);
            localEpoch++;
            coordinator.registerCompletion(localEpoch);
            advancedThisTick = true;
            LOG.info("[DataPerturbation] Task {} emitted real partial for epoch {}", getTaskId(), localEpoch);
        } else if (targetEpoch > localEpoch) {
            // 2. No result ready -- start bg snapshot if not already running (fallback)
            if (snapshotThread == null || !snapshotThread.isAlive()) {
                LOG.info("[DataPerturbation] Task {} starting bg snapshot from tick fallback (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, targetEpoch);
                startBackgroundSnapshot(service);
            }
            // 3. Emit dummy if we've already produced at least one real partial
            if (localEpoch > 0) {
                synchronized (serviceLock) {
                    processEncryptedSnapshot(service.getEncryptedDummyPartial());
                }
                LOG.debug("[DataPerturbation] Task {} emitted dummy (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, targetEpoch);
            }
        }

        // Try to advance the global epoch (every replica will try to trigger this)
        coordinator.tryAdvanceEpoch(targetEpoch);
    }

    private void handlePlaintextTick(DataPerturbationService service, int targetEpoch) throws EnclaveServiceException {
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

    /**
     * Starts the snapshot computation on a background thread.
     * Safe to call from any thread (bolt executor or Curator {@code SharedCountListener} callback).
     */
    private synchronized void startBackgroundSnapshot(DataPerturbationService service) {
        if (snapshotThread != null && snapshotThread.isAlive()) {
            LOG.debug("[DataPerturbation] Previous snapshot still computing -- skipping");
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
