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
 * of times, keeping enclave timeSteps perfectly in sync.
 * <p>
 * <b>Tick-driven emission</b>: Every replica emits exactly once per tick tuple either the
 * real partial (if the background computation finished) or a dummy (if still computing).
 * The first tick after the global epoch advances starts the background computation without
 * emitting anything. This creates a fixed, synchronized transmission pattern across replicas.
 * <p>
 * <b>Tick lifecycle</b>:
 * <ol>
 *   <li>Tick arrives -> read targetEpoch from ZK</li>
 *   <li>If the background thread finished: emit real partial, advance localEpoch, register completion,
 *       and if targetEpoch still ahead, start next bg computation to stay in sync</li>
 *   <li>If the background thread is still running: emit dummy to maintain the same transmission pattern</li>
 *   <li>If no background thread is running and targetEpoch > localEpoch: start new background computation without
 *       emitting (we just advanced the global epoch, so we are now behind and we need to catch up)</li>
 *   <li>Leader: try to advance global epoch</li>
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
     * - Emits a real partial (background thread finished) and starts the next background computation, OR
     * - Emits a dummy (background thread still running), OR
     * - Starts a background computation without emitting (first tick after epoch advance).
     */
    private void handleEpochTick(DataPerturbationService service) throws EnclaveServiceException {
        // Reject ticks before every replica is ready to process them (avoids epoch drift at startup)
        if (!coordinator.isReady()) {
            LOG.debug("[DataPerturbation] Task {} ignoring tick - startup not complete", getTaskId());
            return;
        }

        // Read the current target epoch from ZK. This is the epoch that all replicas should be working towards
        int targetEpoch = coordinator.getTargetEpoch();

        // Handle the tick accordingly based on the encrypted vs plaintext snapshot mode
        if (useEncryptedSnapshots()) {
            handleEncryptedTick(service, targetEpoch);
        } else {
            handlePlaintextTick(service, targetEpoch);
        }
    }

    private void handleEncryptedTick(DataPerturbationService service, int targetEpoch) throws EnclaveServiceException {
        // Get the result of the background computation (if it finished) and reset to null for the next round
        EncryptedDataPerturbationSnapshot result = completedSnapshot.getAndSet(null);

        if (result != null) {
            // Background thread finished - emit real partial
            processEncryptedSnapshot(result);
            localEpoch++;
            coordinator.registerCompletion(localEpoch);
            LOG.info("[DataPerturbation] Task {} emitted real partial for epoch {}", getTaskId(), localEpoch);

            // If there's already a new target, start the next background computation immediately
            if (targetEpoch > localEpoch && (snapshotThread == null || !snapshotThread.isAlive())) {
                LOG.info("[DataPerturbation] Task {} starting bg snapshot (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, targetEpoch);
                startBackgroundSnapshot(service);
            }
        } else if (snapshotThread != null && snapshotThread.isAlive()) {
            // Background thread still running - emit dummy to maintain transmission pattern
            synchronized (serviceLock) {
                processEncryptedSnapshot(service.getEncryptedDummyPartial());
            }
            LOG.info("[DataPerturbation] Task {} emitted dummy (localEpoch={}, targetEpoch={})",
                    getTaskId(), localEpoch, targetEpoch);
        } else if (targetEpoch > localEpoch) {
            // No background thread is running, and we are behind - start new background computation
            LOG.info("[DataPerturbation] Task {} starting bg snapshot (localEpoch={}, targetEpoch={})",
                    getTaskId(), localEpoch, targetEpoch);
            startBackgroundSnapshot(service);

            // Emit dummy on every tick except the very first (localEpoch==0 means no prior epoch to dummy for)
            if (localEpoch > 0) {
                synchronized (serviceLock) {
                    processEncryptedSnapshot(service.getEncryptedDummyPartial());
                }
                LOG.debug("[DataPerturbation] Task {} emitted dummy (localEpoch={}, targetEpoch={})",
                        getTaskId(), localEpoch, targetEpoch);
            }
        }
        // else: targetEpoch == localEpoch and there is no background thread = we're already in sync and just waiting for

        // Leader tries to advance global epoch after processing the tick
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
     */
    private void startBackgroundSnapshot(DataPerturbationService service) {
        if (snapshotThread != null && snapshotThread.isAlive()) {
            LOG.debug("[DataPerturbation] Previous snapshot still computing - skipping");
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
