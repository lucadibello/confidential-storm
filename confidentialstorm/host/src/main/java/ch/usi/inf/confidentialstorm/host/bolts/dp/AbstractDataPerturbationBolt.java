package ch.usi.inf.confidentialstorm.host.bolts.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.DataPerturbationService;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationContributionEntryRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.EncryptedDataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import ch.usi.inf.confidentialstorm.host.profiling.ProfilerConfig;
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
 * This guarantees that epochs never complete faster than the configured tick interval.
 * <p>
 * <b>Tick lifecycle</b>:
 * <ol>
 *   <li>Tick arrives -> read targetEpoch (locally cached by Curator's {@code SharedCount})</li>
 *   <li>If bg thread finished: emit real partial, advance localEpoch, register completion</li>
 *   <li>If behind target and no bg thread running: start bg thread</li>
 *   <li>If no result ready: emit dummy (except on the very first tick when localEpoch==0)</li>
 *   <li>Try to advance global epoch via versioned CAS</li>
 *   <li>If epoch just advanced: eagerly start next bg snapshot (result consumed on next tick)</li>
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
     * Holds the result of the background encrypted snapshot computation.
     * Set by the background thread, consumed by the main bolt thread on the next tick.
     * null means the computation is still in progress (or hasn't started).
     */
    private transient AtomicReference<EncryptedDataPerturbationSnapshot> completedSnapshot;

    /**
     * Holds the result of the background plaintext snapshot computation.
     * Used only when {@link #useEncryptedSnapshots()} returns false.
     */
    private transient AtomicReference<DataPerturbationSnapshot> completedPlaintextSnapshot;

    /**
     * The background thread running the snapshot computation.
     */
    private transient Thread snapshotThread;



    /**
     * Set to true when the bolt has reached {@link #getMaxEpochs()} and should stop processing.
     * Once finished, incoming tuples are acked immediately and ticks are ignored.
     */
    private volatile boolean finished = false;

    private transient EpochBarrierCoordinator coordinator;
    private transient long contributionsThisEpoch;

    public AbstractDataPerturbationBolt() {
        super(DataPerturbationService.class);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        super.afterPrepare(topoConf, context);
        this.completedSnapshot = new AtomicReference<>(null);
        this.completedPlaintextSnapshot = new AtomicReference<>(null);
        this.snapshotThread = null;
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
            if (!finished
                    && coordinator.getTargetEpoch() > localEpoch
                    && (snapshotThread == null || !snapshotThread.isAlive())) {
                LOG.info("[DataPerturbation] Task {} starting bg snapshot from watch (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, coordinator.getTargetEpoch());

                // trigger correct snapshot method based on whether we're using encrypted snapshots or not
                if (useEncryptedSnapshots()) {
                    startBackgroundSnapshot(service);
                } else {
                    startBackgroundPlaintextSnapshot(service);
                }
            }
        });

        coordinator.awaitStartup(() -> {
                LOG.info("[DataPerturbation] Task {} ready -- starting epoch processing", getTaskId());
                if (ProfilerConfig.ENABLED) {
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.BARRIER_RELEASED);
                }
        });

        if (ProfilerConfig.ENABLED) {
            getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.TICK_INTERVAL_SECS, getTickIntervalSecs());
            getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.MAX_EPOCHS_CONFIGURED, getMaxEpochs());
        }
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
     * Returns the maximum number of epochs to process before deactivating.
     * After this many epochs, the bolt stops processing tuples and flushes the profiler.
     * Subclasses may override to set a finite limit (e.g., from DP configuration).
     *
     * @return the max epoch count, or {@code -1} (default) to run indefinitely.
     */
    protected int getMaxEpochs() {
        return -1;
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
        // check finished first to avoid unnecessary processing and background snapshot starts after reaching max epochs
        if (finished) {
            if (!isTickTuple(input)) getCollector().ack(input);
            return;
        }

        // Tick tuples trigger the epoch synchronization and snapshot emission logic, regular tuples add contributions.
        if (isTickTuple(input)) {
            handleEpochTick(service);
        } else {
            long t0 = ProfilerConfig.ENABLED && getProfiler().shouldSample() ? System.nanoTime() : 0;
            service.addContribution(new DataPerturbationContributionEntryRequest(
                    getUserIdEntry(input),
                    getWordEntry(input),
                    getClampedCountEntry(input)
            ));
            if (t0 != 0) getProfiler().recordEcall("addContribution", System.nanoTime() - t0);
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementEcallTotal("addContribution");
                contributionsThisEpoch++;
            }
        }
        getCollector().ack(input);
    }

    /**
     * Tick-driven epoch handler. Every tick, each replica either:
     * - Emits a real partial (bg thread finished), OR
     * - Emits a dummy (no result ready, including when caught up waiting for other replicas), OR
     * - Does nothing (only when localEpoch==0, i.e., before the first real partial).
     * <p>
     * Background computation is started reactively by the {@code SharedCountListener}
     * callback when the epoch advances. The tick handler only starts a computation as
     * a fallback if the listener notification was missed.
     */
    private void handleEpochTick(DataPerturbationService service) throws EnclaveServiceException {
        if (!coordinator.isReady()) {
            LOG.debug("[DataPerturbation] Task {} ignoring tick - startup not complete", getTaskId());
            return;
        }

        int targetEpoch = coordinator.getTargetEpoch();

        if (ProfilerConfig.ENABLED) {
            getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.TICK_RECEIVED, targetEpoch);
            getProfiler().recordGauge("epoch_lag", targetEpoch - localEpoch);
        }

        if (useEncryptedSnapshots()) {
            handleEncryptedTick(service, targetEpoch);
        } else {
            handlePlaintextTick(service, targetEpoch);
        }

        if (ProfilerConfig.ENABLED) getProfiler().onTick();
    }

    private void handleEncryptedTick(DataPerturbationService service, int targetEpoch) throws EnclaveServiceException {
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
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementCounter("real_emissions");
                getProfiler().recordGauge("contributions_this_epoch", contributionsThisEpoch);
                getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.EPOCH_ADVANCED, localEpoch);
                contributionsThisEpoch = 0;
            }
            LOG.info("[DataPerturbation] Task {} emitted real partial for epoch {}", getTaskId(), localEpoch);

            // Check if we've reached the max epoch limit after processing this tick
            if (getMaxEpochs() > 0 && localEpoch >= getMaxEpochs()) {
                finished = true;
                LOG.info("[DataPerturbation] Task {} reached max epochs ({}), deactivating",
                        getTaskId(), getMaxEpochs());
                if (ProfilerConfig.ENABLED) {
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.MAX_EPOCH_REACHED, localEpoch);
                    getProfiler().writeReport();
                }
                return;
            }
        } else {
            // 2. No result ready - start bg snapshot if behind and not already running
            if (targetEpoch > localEpoch && (snapshotThread == null || !snapshotThread.isAlive())) {
                LOG.info("[DataPerturbation] Task {} starting bg snapshot (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, targetEpoch);
                startBackgroundSnapshot(service);
            }
            // 3. Emit dummy to maintain constant-rate communication pattern.
            //    This covers the following cases:
            //    (a) behind target with snapshot in progress
            //    (b) caught up (targetEpoch == localEpoch) waiting for other replicas
            if (localEpoch > 0) {
                long t0 = ProfilerConfig.ENABLED ? System.nanoTime() : 0;
                processEncryptedSnapshot(service.getEncryptedDummyPartial());
                if (t0 != 0) getProfiler().recordEcall("getEncryptedDummyPartial", System.nanoTime() - t0);
                if (ProfilerConfig.ENABLED) {
                    getProfiler().incrementEcallTotal("getEncryptedDummyPartial");
                    getProfiler().incrementCounter("dummy_emissions");
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.DUMMY_RELEASED, localEpoch);
                }
                LOG.info("[DataPerturbation] Task {} emitted dummy (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, targetEpoch);
            }
        }

        // Try to advance the global epoch (every replica will try to trigger this)
        coordinator.tryAdvanceEpoch(targetEpoch);
    }

    private void handlePlaintextTick(DataPerturbationService service, int targetEpoch) throws EnclaveServiceException {
        // 1. Check if a previously started bg computation finished.
        DataPerturbationSnapshot result = completedPlaintextSnapshot.getAndSet(null);

        if (result != null) {
            // Background thread finished -- emit real partial
            processSnapshot(result.histogramSnapshot());
            localEpoch++;
            coordinator.registerCompletion(localEpoch);
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementCounter("real_emissions");
                getProfiler().recordGauge("contributions_this_epoch", contributionsThisEpoch);
                getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.EPOCH_ADVANCED, localEpoch);
                contributionsThisEpoch = 0;
            }
            LOG.info("[DataPerturbation] Task {} emitted real partial for epoch {}", getTaskId(), localEpoch);

            // Check if we've reached the max epoch limit
            if (getMaxEpochs() > 0 && localEpoch >= getMaxEpochs()) {
                finished = true;
                LOG.info("[DataPerturbation] Task {} reached max epochs ({}), deactivating",
                        getTaskId(), getMaxEpochs());
                if (ProfilerConfig.ENABLED) {
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.MAX_EPOCH_REACHED, localEpoch);
                    getProfiler().writeReport();
                }
                return;
            }
        } else {
            // 2. No result ready -- start bg snapshot if behind and not already running
            if (targetEpoch > localEpoch && (snapshotThread == null || !snapshotThread.isAlive())) {
                LOG.info("[DataPerturbation] Task {} starting bg snapshot (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, targetEpoch);
                startBackgroundPlaintextSnapshot(service);
            }
            // 3. Emit dummy to maintain constant-rate communication pattern.
            //    This covers both cases: (a) behind target with snapshot in progress,
            //    and (b) caught up (targetEpoch == localEpoch) waiting for other replicas.
            if (localEpoch > 0) {
                long t0 = ProfilerConfig.ENABLED ? System.nanoTime() : 0;
                DataPerturbationSnapshot dummy = service.getDummyPartial();
                if (t0 != 0) getProfiler().recordEcall("getDummyPartial", System.nanoTime() - t0);
                processSnapshot(dummy.histogramSnapshot());
                if (ProfilerConfig.ENABLED) {
                    getProfiler().incrementEcallTotal("getDummyPartial");
                    getProfiler().incrementCounter("dummy_emissions");
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.DUMMY_RELEASED, localEpoch);
                }
                LOG.info("[DataPerturbation] Task {} emitted dummy (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, targetEpoch);
            }
        }

        // Try to advance the global epoch (every replica will try to trigger this)
        coordinator.tryAdvanceEpoch(targetEpoch);
    }

    /**
     * Starts the snapshot computation on a background thread.
     * Called from the bolt executor's tick handler or the SharedCountListener callback.
     */
    private synchronized void startBackgroundSnapshot(DataPerturbationService service) {
        if (snapshotThread != null && snapshotThread.isAlive()) {
            LOG.debug("[DataPerturbation] Previous snapshot still computing -- skipping");
            return;
        }

        snapshotThread = new Thread(() -> {
            try {
                if (ProfilerConfig.ENABLED) {
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_STARTED, localEpoch + 1);
                }
                long ecallStart = ProfilerConfig.ENABLED ? System.nanoTime() : 0;
                EncryptedDataPerturbationSnapshot result = service.getEncryptedSnapshot();
                if (ecallStart != 0) {
                    getProfiler().recordEcall("getEncryptedSnapshot", System.nanoTime() - ecallStart);
                    getProfiler().incrementEcallTotal("getEncryptedSnapshot");
                }
                completedSnapshot.set(result);
                if (ProfilerConfig.ENABLED) {
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_COMPLETED, localEpoch + 1);
                }
                LOG.debug("[DataPerturbation] Background snapshot computation complete");
            } catch (EnclaveServiceException e) {
                LOG.error("[DataPerturbation] Background snapshot computation failed", e);
            }
        }, "dp-snapshot-" + getTaskId());
        snapshotThread.setDaemon(true);
        snapshotThread.start();
    }

    /**
     * Starts the plaintext snapshot computation on a background thread.
     * Same concurrency pattern as {@link #startBackgroundSnapshot}, but stores
     * the result in {@link #completedPlaintextSnapshot}.
     */
    private synchronized void startBackgroundPlaintextSnapshot(DataPerturbationService service) {
        if (snapshotThread != null && snapshotThread.isAlive()) {
            LOG.debug("[DataPerturbation] Previous snapshot still computing -- skipping");
            return;
        }

        snapshotThread = new Thread(() -> {
            try {
                if (ProfilerConfig.ENABLED) {
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_STARTED, localEpoch + 1);
                }
                long snapshotStart = ProfilerConfig.ENABLED ? System.nanoTime() : 0;
                DataPerturbationSnapshot result = service.getSnapshot();
                if (snapshotStart != 0) {
                    getProfiler().recordEcall("getSnapshot", System.nanoTime() - snapshotStart);
                    getProfiler().incrementEcallTotal("getSnapshot");
                }
                completedPlaintextSnapshot.set(result);
                if (ProfilerConfig.ENABLED) {
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_COMPLETED, localEpoch + 1);
                }
                LOG.debug("[DataPerturbation] Background plaintext snapshot computation complete");
            } catch (EnclaveServiceException e) {
                LOG.error("[DataPerturbation] Background plaintext snapshot computation failed", e);
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
