package ch.usi.inf.examples.synthetic_baseline.bolts;

import ch.usi.inf.examples.synthetic_baseline.config.DPConfig;
import ch.usi.inf.examples.synthetic_baseline.dp.DPUtil;
import ch.usi.inf.examples.synthetic_baseline.dp.EpochBarrierCoordinator;
import ch.usi.inf.examples.synthetic_baseline.dp.StreamingDPMechanism;
import ch.usi.inf.examples.synthetic_baseline.profiling.BaselineBoltLifecycleEvent;
import ch.usi.inf.examples.synthetic_baseline.profiling.BoltProfiler;
import ch.usi.inf.examples.synthetic_baseline.profiling.DPBoltLifecycleEvent;
import ch.usi.inf.examples.synthetic_baseline.profiling.ProfilerConfig;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Data perturbation bolt - runs the DP-SQLP streaming mechanism
 * directly in the JVM with the same epoch orchestration as the enclave version.
 * <p>
 * Uses background snapshot thread for fair benchmark comparison (same concurrency
 * pattern as the enclave version, minus ECALL/encryption overhead).
 */
public class BaselineDataPerturbationBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BaselineDataPerturbationBolt.class);

    private static final int DEFAULT_EPOCH_TIMEOUT_SECS = 30;
    private static final String DUMMY_MARKER_KEY = "__dummy";

    private OutputCollector collector;
    private int taskId;
    private StreamingDPMechanism mechanism;

    private int maxTimeSteps;
    private int tickIntervalSecs;
    private int localEpoch = 0;
    private transient AtomicReference<Map<String, Long>> completedSnapshot;
    private transient Thread snapshotThread;
    private transient Object serviceLock;

    private volatile boolean finished = false;

    private transient EpochBarrierCoordinator coordinator;
    private transient BoltProfiler profiler;
    private transient long contributionsThisEpoch;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.taskId = context.getThisTaskId();
        this.completedSnapshot = new AtomicReference<>(null);
        this.snapshotThread = null;
        this.serviceLock = new Object();

        // Read runtime config from topology conf (set by the submitter via Storm Config),
        this.maxTimeSteps = ((Number) topoConf.getOrDefault("dp.max.time.steps", DPConfig.maxTimeSteps())).intValue();
        this.tickIntervalSecs = ((Number) topoConf.getOrDefault("dp.tick.interval.secs", 5)).intValue();
        long mu = ((Number) topoConf.getOrDefault("dp.mu", DPConfig.mu())).longValue();

        // Budget split (from SyntheticDataPerturbationServiceProvider):
        //   epsilon: 50/50 split
        //   delta: 2/3 for key selection, 1/3 for histogram
        double rhoK = DPUtil.cdpRho(DPConfig.EPSILON / 2.0, (2.0 / 3.0) * DPConfig.DELTA);
        double sigmaKey = DPUtil.calculateSigma(rhoK, maxTimeSteps, 1.0);

        double rhoH = DPUtil.cdpRho(DPConfig.EPSILON / 2.0, (1.0 / 3.0) * DPConfig.DELTA);
        double l1Sens = DPUtil.l1Sensitivity(DPConfig.MAX_CONTRIBUTIONS_PER_USER, DPConfig.PER_RECORD_CLAMP);
        double sigmaHist = DPUtil.calculateSigma(rhoH, maxTimeSteps, l1Sens);

        this.mechanism = new StreamingDPMechanism(
                sigmaKey, sigmaHist, this.maxTimeSteps, mu, DPConfig.MAX_CONTRIBUTIONS_PER_USER);

        LOG.info("[BaselineDataPerturbation {}] DP mechanism initialized: sigmaKey={}, sigmaHist={}, maxTimeSteps={}, mu={}",
                taskId, sigmaKey, sigmaHist, maxTimeSteps, mu);

        // Set up epoch coordination via ZooKeeper
        int totalReplicas = context.getComponentTasks(context.getThisComponentId()).size();
        int minTaskId = context.getComponentTasks(context.getThisComponentId()).stream()
                .mapToInt(Integer::intValue).min().orElse(taskId);
        boolean isLeader = taskId == minTaskId;

        this.coordinator = new EpochBarrierCoordinator(
                topoConf, context.getStormId(), taskId,
                totalReplicas, DEFAULT_EPOCH_TIMEOUT_SECS, isLeader);

        // Register listener: when epoch advances, start bg snapshot
        coordinator.setOnEpochAdvanced(() -> {
            if (!finished
                    && coordinator.getTargetEpoch() > localEpoch
                    && (snapshotThread == null || !snapshotThread.isAlive())) {
                LOG.info("[BaselineDataPerturbation] Task {} starting bg snapshot from watch (localEpoch={}, targetEpoch={})",
                        taskId, localEpoch, coordinator.getTargetEpoch());
                startBackgroundSnapshot();
            }
        });

        coordinator.awaitStartup(() ->
                LOG.info("[BaselineDataPerturbation] Task {} ready -- starting epoch processing", taskId));

        // Initialize profiler
        if (ProfilerConfig.ENABLED) {
            this.profiler = new BoltProfiler(context.getThisComponentId(), taskId);
            profiler.recordLifecycleEvent(BaselineBoltLifecycleEvent.COMPONENT_STARTED);
            profiler.recordLifecycleEvent(DPBoltLifecycleEvent.TICK_INTERVAL_SECS, tickIntervalSecs);
            profiler.recordLifecycleEvent(DPBoltLifecycleEvent.MAX_EPOCHS_CONFIGURED, maxTimeSteps);
        }

        LOG.info("[BaselineDataPerturbation {}] Prepared with {} replicas", taskId, totalReplicas);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Integer.getInteger("dp.tick.interval.secs", 5));
        return config;
    }

    @Override
    public void execute(Tuple input) {
        if (finished) {
            if (!isTickTuple(input)) collector.ack(input);
            return;
        }

        if (isTickTuple(input)) {
            handleEpochTick();
        } else {
            // Add contribution to the mechanism (no ECALL + no encryption)
            String userId = input.getStringByField("userId");
            String word = input.getStringByField("word");
            double clampedCount = input.getDoubleByField("count");

            long t0 = ProfilerConfig.ENABLED && profiler.shouldSample() ? System.nanoTime() : 0;
            synchronized (serviceLock) {
                mechanism.addContribution(userId, word, clampedCount);
            }
            if (t0 != 0) profiler.recordEcall("addContribution", System.nanoTime() - t0);
            if (ProfilerConfig.ENABLED) {
                profiler.incrementEcallTotal("addContribution");
                contributionsThisEpoch++;
            }
        }
        collector.ack(input);
    }

    private static Map<String, Long> createDummyHistogram() {
        return Map.of(DUMMY_MARKER_KEY, 0L);
    }

    private void handleEpochTick() {
        if (!coordinator.isReady()) {
            LOG.debug("[BaselineDataPerturbation] Task {} ignoring tick -- startup not complete", taskId);
            return;
        }

        int targetEpoch = coordinator.getTargetEpoch();
        if (ProfilerConfig.ENABLED) {
            profiler.recordGauge("epoch_lag", targetEpoch - localEpoch);
        }

        // 1. Check if bg snapshot finished
        Map<String, Long> result = completedSnapshot.getAndSet(null);

        if (result != null) {
            // Emit real partial
            emitHistogram(result, false);
            localEpoch++;
            coordinator.registerCompletion(localEpoch);

            if (ProfilerConfig.ENABLED) {
                profiler.incrementCounter("real_emissions");
                profiler.recordGauge("contributions_this_epoch", contributionsThisEpoch);
                profiler.recordLifecycleEvent(DPBoltLifecycleEvent.EPOCH_ADVANCED, localEpoch);
                contributionsThisEpoch = 0;
            }
            LOG.info("[BaselineDataPerturbation] Task {} emitted real partial for epoch {}", taskId, localEpoch);

            if (maxTimeSteps > 0 && localEpoch >= maxTimeSteps) {
                finished = true;
                LOG.info("[BaselineDataPerturbation] Task {} reached max epochs ({}), deactivating",
                        taskId, maxTimeSteps);
                if (ProfilerConfig.ENABLED) {
                    profiler.recordLifecycleEvent(DPBoltLifecycleEvent.MAX_EPOCH_REACHED, localEpoch);
                    profiler.writeReport();
                }
                return;
            }
        } else if (targetEpoch > localEpoch) {
            // 2. No result — start bg snapshot if not already running (fallback)
            if (snapshotThread == null || !snapshotThread.isAlive()) {
                LOG.info("[BaselineDataPerturbation] Task {} starting bg snapshot from tick fallback (localEpoch={}, targetEpoch={})",
                        taskId, localEpoch, targetEpoch);
                startBackgroundSnapshot();
            }
            // 3. Emit dummy if we've already produced at least one real partial
            if (localEpoch > 0) {
                emitHistogram(createDummyHistogram(), true); // No ECALL + no encryption
                if (ProfilerConfig.ENABLED) {
                    profiler.incrementCounter("dummy_emissions");
                }
                LOG.debug("[BaselineDataPerturbation] Task {} emitted dummy (localEpoch={}, targetEpoch={})",
                        taskId, localEpoch, targetEpoch);
            }
        }

        // Try to advance global epoch
        coordinator.tryAdvanceEpoch(targetEpoch);

        if (ProfilerConfig.ENABLED) profiler.onTick();
    }

    private synchronized void startBackgroundSnapshot() {
        if (snapshotThread != null && snapshotThread.isAlive()) {
            LOG.debug("[BaselineDataPerturbation] Previous snapshot still computing -- skipping");
            return;
        }

        snapshotThread = new Thread(() -> {
            if (ProfilerConfig.ENABLED) {
                profiler.recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_STARTED, localEpoch + 1);
            }
            long lockWaitStart = ProfilerConfig.ENABLED ? System.nanoTime() : 0;
            long snapshotStart;
            Map<String, Long> result;
            synchronized (serviceLock) {
                snapshotStart = ProfilerConfig.ENABLED ? System.nanoTime() : 0;
                result = mechanism.snapshot();
                if (snapshotStart != 0) {
                    profiler.recordEcall("snapshot", System.nanoTime() - snapshotStart);
                    profiler.incrementEcallTotal("snapshot");
                }
            }
            if (lockWaitStart != 0) {
                profiler.recordEcall("snapshot_lock_wait", snapshotStart - lockWaitStart);
            }
            completedSnapshot.set(result);
            if (ProfilerConfig.ENABLED) {
                profiler.recordLifecycleEvent(DPBoltLifecycleEvent.SNAPSHOT_COMPLETED, localEpoch + 1);
            }
            LOG.debug("[BaselineDataPerturbation] Background snapshot computation complete");
        }, "dp-snapshot-" + taskId);
        snapshotThread.setDaemon(true);
        snapshotThread.start();
    }

    private void emitHistogram(Map<String, Long> histogram, boolean isDummy) {
        collector.emit(new Values(histogram, isDummy, localEpoch + 1, String.valueOf(taskId)));
    }

    @Override
    public void cleanup() {
        if (ProfilerConfig.ENABLED && profiler != null) {
            profiler.recordLifecycleEvent(BaselineBoltLifecycleEvent.COMPONENT_STOPPING);
            profiler.writeReport();
        }
        if (coordinator != null) {
            try {
                coordinator.close();
            } catch (Exception e) {
                LOG.warn("[BaselineDataPerturbation] Error closing EpochBarrierCoordinator", e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("histogram", "isDummy", "epoch", "producerId"));
    }

    private static boolean isTickTuple(Tuple tuple) {
        return "__system".equals(tuple.getSourceComponent())
                && "__tick".equals(tuple.getSourceStreamId());
    }
}
