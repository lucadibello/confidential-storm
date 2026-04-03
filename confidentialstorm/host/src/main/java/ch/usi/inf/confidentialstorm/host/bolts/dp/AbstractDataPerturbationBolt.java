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

/**
 * Base implementation for a bolt that performs data perturbation (DP).
 * This bolt delegates the DP mechanism to an enclave-based {@link DataPerturbationService}.
 * <p>
 * <b>Epoch synchronization</b> uses a Curator {@code SharedCount} via {@link EpochBarrierCoordinator}.
 * The global counter ({@code /current-epoch}) only advances when ALL replicas have completed
 * the current epoch, ensuring all replicas call {@code startEncryptedSnapshot()} the same number
 * of times, keeping enclave timeSteps perfectly in sync.
 * <p>
 * <b>Tick-driven emission</b>: Every replica emits exactly once per tick tuple -- either the
 * real partial (if the enclave-internal computation finished) or a dummy (if still computing).
 * This guarantees that epochs never complete faster than the configured tick interval.
 * <p>
 * <b>Async snapshot via start/poll</b>: Instead of blocking a host thread on a long ECALL,
 * the snapshot computation runs inside the enclave on an internal thread. The host issues
 * a short {@code startEncryptedSnapshot()} ECALL to kick it off, then polls with
 * {@code pollEncryptedSnapshot()} on each tick. All ECALLs are sub-millisecond, so
 * {@code addContribution()} is never blocked for meaningful duration.
 * <p>
 * <b>Tick lifecycle</b>:
 * <ol>
 *   <li>Tick arrives -> read targetEpoch (locally cached by Curator's {@code SharedCount})</li>
 *   <li>If snapshot in progress: poll enclave; if ready, emit real partial, advance localEpoch</li>
 *   <li>If behind target and no snapshot in progress: call startSnapshot ECALL</li>
 *   <li>If no result ready: emit dummy (except on the very first tick when localEpoch==0)</li>
 *   <li>Try to advance global epoch via versioned CAS</li>
 * </ol>
 */
public abstract class AbstractDataPerturbationBolt extends ConfidentialBolt<DataPerturbationService> {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractDataPerturbationBolt.class);

    private static final int DEFAULT_TICK_INTERVAL_SECS = 5;
    private static final int DEFAULT_EPOCH_TIMEOUT_SECS = 30;

    /**
     * How many times this replica has completed a snapshot and emitted the result.
     * Must stay in sync with the enclave's internal epoch/timeStep.
     * Volatile because it is read by the Curator event thread (setOnEpochAdvanced callback)
     * and written by the bolt executor thread.
     */
    private volatile int localEpoch = 0;

    /**
     * Whether an async snapshot computation is currently in progress inside the enclave.
     * Accessed only from the bolt executor thread (single-threaded per Storm task)
     * and from the Curator event thread (setOnEpochAdvanced callback), so we use volatile.
     */
    private volatile boolean snapshotInProgress = false;

    /**
     * Lock to serialize access to the enclave service.
     * With the start/poll pattern, every ECALL completes in sub-millisecond time,
     * so this lock is never held for meaningful duration.
     */
    private transient Object serviceLock;


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
        this.serviceLock = new Object();

        int totalReplicas = context.getComponentTasks(context.getThisComponentId()).size();
        int minTaskId = context.getComponentTasks(context.getThisComponentId()).stream()
                .mapToInt(Integer::intValue).min().orElse(getTaskId());
        boolean isLeader = getTaskId() == minTaskId;

        this.coordinator = new EpochBarrierCoordinator(
                topoConf, context.getStormId(), getTaskId(),
                totalReplicas, DEFAULT_EPOCH_TIMEOUT_SECS, isLeader);

        // Register the SharedCountListener callback: when the global epoch advances,
        // start the next enclave-internal snapshot immediately (from the Curator event thread).
        DataPerturbationService service = state.getEnclaveManager().getService();
        coordinator.setOnEpochAdvanced(() -> {
            if (!finished
                    && coordinator.getTargetEpoch() > localEpoch
                    && !snapshotInProgress) {
                LOG.info("[DataPerturbation] Task {} starting snapshot from watch (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, coordinator.getTargetEpoch());
                try {
                    synchronized (serviceLock) {
                        boolean ret = useEncryptedSnapshots() ? service.startEncryptedSnapshot() : service.startSnapshot();
                        if (!ret) {
                            LOG.warn("[DataPerturbation] Task {} failed to start snapshot from watch - start call " +
                                    "returned false, likely because the thread is still running from a previous tick.",
                                    getTaskId());
                            return;
                        }
                        snapshotInProgress = true;
                    }
                    if (ProfilerConfig.ENABLED) {
                        getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_STARTED, localEpoch + 1);
                    }
                } catch (EnclaveServiceException e) {
                    LOG.error("[DataPerturbation] Task {} failed to start snapshot from watch", getTaskId(), e);
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
        // check finished first to avoid unnecessary processing after reaching max epochs
        if (finished) {
            if (!isTickTuple(input)) getCollector().ack(input);
            return;
        }

        // Tick tuples trigger the epoch synchronization and snapshot emission logic, regular tuples add contributions.
        if (isTickTuple(input)) {
            handleEpochTick(service);
        } else {
            synchronized (serviceLock) {
                long t0 = ProfilerConfig.ENABLED && getProfiler().shouldSample() ? System.nanoTime() : 0;
                service.addContribution(new DataPerturbationContributionEntryRequest(
                        getUserIdEntry(input),
                        getWordEntry(input),
                        getClampedCountEntry(input)
                ));
                if (t0 != 0) getProfiler().recordEcall("addContribution", System.nanoTime() - t0);
            }
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementEcallTotal("addContribution");
                contributionsThisEpoch++;
            }
        }
        getCollector().ack(input);
    }

    /**
     * Tick-driven epoch handler. Every tick, each replica either:
     * - Emits a real partial (enclave snapshot ready), OR
     * - Emits a dummy (snapshot still computing or caught up waiting), OR
     * - Does nothing (only when localEpoch==0, i.e., before the first real partial).
     * <p>
     * Snapshot computation is started reactively by the {@code SharedCountListener}
     * callback when the epoch advances. The tick handler starts a computation as
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
        if (snapshotInProgress) {
            // Poll for the result of the enclave-internal async snapshot
            EncryptedDataPerturbationSnapshot result;
            synchronized (serviceLock) {
                long t0 = ProfilerConfig.ENABLED && getProfiler().shouldSample() ? System.nanoTime() : 0;
                LOG.info("[POLLING] Task {} polling for encrypted snapshot result for epoch {}", getTaskId(), localEpoch + 1);
                result = service.pollEncryptedSnapshot();
                LOG.info("[POLLING] Task {} pollEncryptedSnapshot call returned for epoch {}", getTaskId(), localEpoch + 1);
                if (t0 != 0) getProfiler().recordEcall("pollEncryptedSnapshot", System.nanoTime() - t0);
            }
            if (ProfilerConfig.ENABLED) getProfiler().incrementEcallTotal("pollEncryptedSnapshot");

            if (result.ready()) {
                LOG.info("[POLLING] Task {} snapshot ready for epoch {}", getTaskId(), localEpoch + 1);
                // Snapshot finished -- emit real partial
                processEncryptedSnapshot(result);
                snapshotInProgress = false;
                localEpoch++;
                coordinator.registerCompletion(localEpoch);
                if (ProfilerConfig.ENABLED) {
                    getProfiler().incrementCounter("real_emissions");
                    getProfiler().recordGauge("contributions_this_epoch", contributionsThisEpoch);
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.EPOCH_ADVANCED, localEpoch);
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_COMPLETED, localEpoch);
                    contributionsThisEpoch = 0;
                }
                LOG.info("[POLLING] Task {} emitted real partial for epoch {}", getTaskId(), localEpoch);

                // Check if we've reached the max epoch limit
                if (getMaxEpochs() > 0 && localEpoch >= getMaxEpochs()) {
                    finished = true;
                    LOG.info("[POLLING] Task {} reached max epochs ({}), deactivating",
                            getTaskId(), getMaxEpochs());
                    if (ProfilerConfig.ENABLED) {
                        getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.MAX_EPOCH_REACHED, localEpoch);
                        getProfiler().writeReport();
                    }
                    return;
                }
            } else {
                LOG.info("[POLLING] Task {} snapshot still computing for epoch {}", getTaskId(), localEpoch + 1);
                // Snapshot still computing -- emit dummy
                emitEncryptedDummy(service, targetEpoch);
            }
        } else if (targetEpoch > localEpoch) {
            // Behind target and no snapshot in progress -- start one
            LOG.info("[DataPerturbation] Task {} starting snapshot (localEpoch={}, targetEpoch={})",
                    getTaskId(), localEpoch, targetEpoch);
            synchronized (serviceLock) {
                long t0 = ProfilerConfig.ENABLED && getProfiler().shouldSample() ? System.nanoTime() : 0;
                boolean ret = service.startEncryptedSnapshot();
                if (!ret) {
                    LOG.warn("[DataPerturbation] Task {} failed to start snapshot - start call " +
                            "returned false, likely because the thread is still running from a previous tick.",
                            getTaskId());
                    return;
                } else {
                    LOG.info("[POLLING] Task {} startEncryptedSnapshot call succeeded", getTaskId());
                }
                if (t0 != 0) getProfiler().recordEcall("startEncryptedSnapshot", System.nanoTime() - t0);
            }
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementEcallTotal("startEncryptedSnapshot");
                getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_STARTED, localEpoch + 1);
            }
            snapshotInProgress = true;

            // Emit dummy on this tick (result will be polled on next tick)
            emitEncryptedDummy(service, targetEpoch);
        } else {
            // Caught up (targetEpoch == localEpoch), waiting for other replicas
            emitEncryptedDummy(service, targetEpoch);
        }

        // Try to advance the global epoch (every replica will try to trigger this)
        coordinator.tryAdvanceEpoch(targetEpoch);
    }

    private void emitEncryptedDummy(DataPerturbationService service, int targetEpoch) throws EnclaveServiceException {
        if (localEpoch > 0) {
            synchronized (serviceLock) {
                long t0 = ProfilerConfig.ENABLED ? System.nanoTime() : 0;
                processEncryptedSnapshot(service.getEncryptedDummyPartial());
                if (t0 != 0) getProfiler().recordEcall("getEncryptedDummyPartial", System.nanoTime() - t0);
            }
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementEcallTotal("getEncryptedDummyPartial");
                getProfiler().incrementCounter("dummy_emissions");
                getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.DUMMY_RELEASED, localEpoch);
            }
            LOG.info("[DataPerturbation] Task {} emitted dummy (localEpoch={}, targetEpoch={})",
                    getTaskId(), localEpoch, targetEpoch);
        }
    }

    private void handlePlaintextTick(DataPerturbationService service, int targetEpoch) throws EnclaveServiceException {
        if (snapshotInProgress) {
            // Poll for the result of the enclave-internal async snapshot
            DataPerturbationSnapshot result;
            synchronized (serviceLock) {
                long t0 = ProfilerConfig.ENABLED && getProfiler().shouldSample() ? System.nanoTime() : 0;
                result = service.pollSnapshot();
                if (t0 != 0) getProfiler().recordEcall("pollSnapshot", System.nanoTime() - t0);
            }
            if (ProfilerConfig.ENABLED) getProfiler().incrementEcallTotal("pollSnapshot");

            if (result.ready()) {
                // Snapshot finished -- emit real partial
                processSnapshot(result.histogramSnapshot());
                snapshotInProgress = false;
                localEpoch++;
                coordinator.registerCompletion(localEpoch);
                if (ProfilerConfig.ENABLED) {
                    getProfiler().incrementCounter("real_emissions");
                    getProfiler().recordGauge("contributions_this_epoch", contributionsThisEpoch);
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.EPOCH_ADVANCED, localEpoch);
                    getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_COMPLETED, localEpoch);
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
                // Snapshot still computing -- emit dummy
                emitPlaintextDummy(service, targetEpoch);
            }
        } else if (targetEpoch > localEpoch) {
            // Behind target and no snapshot in progress -- start one
            LOG.info("[DataPerturbation] Task {} starting snapshot (localEpoch={}, targetEpoch={})",
                    getTaskId(), localEpoch, targetEpoch);
            synchronized (serviceLock) {
                long t0 = ProfilerConfig.ENABLED && getProfiler().shouldSample() ? System.nanoTime() : 0;
                service.startSnapshot();
                if (t0 != 0) getProfiler().recordEcall("startSnapshot", System.nanoTime() - t0);
            }
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementEcallTotal("startSnapshot");
                getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_STARTED, localEpoch + 1);
            }
            snapshotInProgress = true;

            // Emit dummy on this tick (result will be polled on next tick)
            emitPlaintextDummy(service, targetEpoch);
        } else {
            // Caught up (targetEpoch == localEpoch), waiting for other replicas
            emitPlaintextDummy(service, targetEpoch);
        }

        // Try to advance the global epoch (every replica will try to trigger this)
        coordinator.tryAdvanceEpoch(targetEpoch);
    }

    private void emitPlaintextDummy(DataPerturbationService service, int targetEpoch) throws EnclaveServiceException {
        if (localEpoch > 0) {
            synchronized (serviceLock) {
                long t0 = ProfilerConfig.ENABLED ? System.nanoTime() : 0;
                DataPerturbationSnapshot dummy = service.getDummyPartial();
                if (t0 != 0) getProfiler().recordEcall("getDummyPartial", System.nanoTime() - t0);
                processSnapshot(dummy.histogramSnapshot());
            }
            if (ProfilerConfig.ENABLED) {
                getProfiler().incrementEcallTotal("getDummyPartial");
                getProfiler().incrementCounter("dummy_emissions");
                getProfiler().recordLifecycleEvent(DPBoltLifecycleEvent.DUMMY_RELEASED, localEpoch);
            }
            LOG.info("[DataPerturbation] Task {} emitted dummy (localEpoch={}, targetEpoch={})",
                    getTaskId(), localEpoch, targetEpoch);
        }
    }

    protected abstract EncryptedValue getUserIdEntry(Tuple input);
    protected abstract EncryptedValue getWordEntry(Tuple input);
    protected abstract EncryptedValue getClampedCountEntry(Tuple input);
    protected abstract void processSnapshot(Map<String, Long> histogramSnapshot) throws EnclaveServiceException;
}
