package ch.usi.inf.examples.microbatch_baseline.bolts;

import ch.usi.inf.examples.microbatch_baseline.config.DPConfig;
import ch.usi.inf.examples.microbatch_baseline.dp.CompositionMode;
import ch.usi.inf.examples.microbatch_baseline.dp.DPUtil;
import ch.usi.inf.examples.microbatch_baseline.dp.StreamingDPMechanism;
import ch.usi.inf.examples.microbatch_baseline.profiling.BaselineBoltLifecycleEvent;
import ch.usi.inf.examples.microbatch_baseline.profiling.BoltProfiler;
import ch.usi.inf.examples.microbatch_baseline.profiling.ProfilerConfig;
import ch.usi.inf.examples.microbatch_baseline.util.BatchMarker;
import ch.usi.inf.examples.microbatch_baseline.util.ComponentConstants;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Data perturbation bolt for the micro-batch benchmark.
 *
 * <p>Differs from the synthetic-throughput variant: there is no
 * tick-based / ZooKeeper-coordinated epoch advancement. Snapshots are
 * triggered by END control markers instead:
 * <ul>
 *   <li>For each batch {@code b}, the bolt accumulates incoming contributions
 *       as before via {@link StreamingDPMechanism#addContribution}.</li>
 *   <li>It receives one END marker per upstream contribution-bounding replica
 *       (allGrouping on the control stream). After END has arrived from
 *       <em>all</em> upstream contribution-bounding tasks (and a small grace
 *       period elapses to drain late data tuples that crossed streams
 *       out-of-order), the bolt snapshots the mechanism, emits the partial
 *       histogram tagged with the batch id, and forwards END downstream.</li>
 * </ul>
 */
public class BaselineDataPerturbationBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BaselineDataPerturbationBolt.class);

    /** Grace delay after the last END arrives, in millis, to drain in-flight data tuples. */
    private static final long DEFAULT_END_GRACE_MS = 50L;
    /** Tick frequency that drives the grace-window re-check when no more data/control tuples arrive. */
    private static final int TICK_FREQ_SECS = 1;

    private OutputCollector collector;
    private int taskId;
    private StreamingDPMechanism mechanism;
    private int upstreamCbTasks;
    private long endGraceMs;

    private transient BoltProfiler profiler;
    private transient long contributionsThisBatch;

    /** Per-batch state: how many END markers we've seen so far. */
    private final TreeMap<Integer, BatchState> pendingBatches = new TreeMap<>();
    private int lastFlushedBatch = -1;

    private static final class BatchState {
        final Set<Integer> endsFrom = new HashSet<>();
        long firstEndArrivedMs = 0L;
        boolean flushed = false;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.taskId = context.getThisTaskId();
        this.upstreamCbTasks = context.getComponentTasks(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING).size();
        this.endGraceMs = ((Number) topoConf.getOrDefault("microbatch.end.grace.ms", DEFAULT_END_GRACE_MS))
                .longValue();

        long mu = ((Number) topoConf.getOrDefault("dp.mu", DPConfig.mu())).longValue();
        int maxTimeSteps = ((Number) topoConf.getOrDefault("dp.max.time.steps", DPConfig.maxTimeSteps())).intValue();

        final double epsilonK = DPConfig.EPSILON / 2.0;
        final double deltaK = (2.0 / 3.0) * DPConfig.DELTA;
        final double epsilonH = DPConfig.EPSILON / 2.0;
        final double deltaH = (1.0 / 3.0) * DPConfig.DELTA;
        final double alpha = 0.5;
        final CompositionMode composition = CompositionMode.ZCDP_LINEAR;

        this.mechanism = new StreamingDPMechanism(
                composition,
                epsilonK, deltaK,
                epsilonH, deltaH,
                maxTimeSteps,
                mu,
                DPConfig.MAX_CONTRIBUTIONS_PER_USER,
                DPConfig.PER_RECORD_CLAMP,
                alpha);

        DPUtil.DpCalibration cal = DPUtil.calibrate(
                composition, epsilonK, deltaK, epsilonH, deltaH,
                DPConfig.MAX_CONTRIBUTIONS_PER_USER, maxTimeSteps, DPConfig.PER_RECORD_CLAMP, alpha);
        LOG.info("[MicroBatchDataPerturbation {}] DP mechanism initialized: composition={}, sigmaKey={}, "
                        + "sigmaHist={}, thresholdQuantile={}, mu={}, upstreamCbTasks={}",
                taskId, composition, cal.sigmaKey(), cal.sigmaHist(), cal.thresholdQuantile(),
                mu, upstreamCbTasks);

        if (ProfilerConfig.ENABLED) {
            this.profiler = new BoltProfiler(context.getThisComponentId(), taskId);
            profiler.recordLifecycleEvent(BaselineBoltLifecycleEvent.COMPONENT_STARTED);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_FREQ_SECS);
        return conf;
    }

    @Override
    public void execute(Tuple input) {
        if (isTickTuple(input)) {
            // Tick drives the grace-window re-check. Without it, the last END
            // marker arrives while we're still inside the grace window, no
            // more tuples come in, and `tryFlushReadyBatches` is never called
            // again -> the batch deadlocks.
            tryFlushReadyBatches();
            collector.ack(input);
            return;
        }
        if (ComponentConstants.CONTROL_STREAM.equals(input.getSourceStreamId())) {
            handleControl(input);
            collector.ack(input);
            return;
        }

        String userId = input.getStringByField("userId");
        String word = input.getStringByField("word");
        double clampedCount = input.getDoubleByField("count");

        long t0 = ProfilerConfig.ENABLED && profiler.shouldSample() ? System.nanoTime() : 0;
        mechanism.addContribution(userId, word, clampedCount);
        if (t0 != 0) profiler.recordEcall("addContribution", System.nanoTime() - t0);
        if (ProfilerConfig.ENABLED) {
            profiler.incrementEcallTotal("addContribution");
            contributionsThisBatch++;
        }
        collector.ack(input);

        tryFlushReadyBatches();
    }

    private void handleControl(Tuple input) {
        String type = input.getStringByField(BatchMarker.F_TYPE);
        int batchId = input.getIntegerByField(BatchMarker.F_BATCH_ID);
        int upstreamTaskId = input.getSourceTask();

        collector.emit(ComponentConstants.CONTROL_STREAM, input,
                new Values(type,
                        batchId,
                        input.getDoubleByField(BatchMarker.F_SIZE_GB),
                        input.getLongByField(BatchMarker.F_RECORD_COUNT),
                        input.getLongByField(BatchMarker.F_BYTES_PER_TUPLE),
                        input.getLongByField(BatchMarker.F_T_NANOS),
                        input.getLongByField(BatchMarker.F_T_EPOCH_MS)));

        if (!BatchMarker.END.equals(type)) {
            return;
        }

        BatchState st = pendingBatches.computeIfAbsent(batchId, k -> new BatchState());
        if (st.flushed) return;
        st.endsFrom.add(upstreamTaskId);
        if (st.firstEndArrivedMs == 0L) st.firstEndArrivedMs = System.currentTimeMillis();

        LOG.info("[MicroBatchDataPerturbation {}] END for batch {} from upstream task {} ({}/{} ends)",
                taskId, batchId, upstreamTaskId, st.endsFrom.size(), upstreamCbTasks);

        tryFlushReadyBatches();
    }

    private void tryFlushReadyBatches() {
        long now = System.currentTimeMillis();
        for (Map.Entry<Integer, BatchState> e : pendingBatches.entrySet()) {
            int batchId = e.getKey();
            BatchState st = e.getValue();
            if (st.flushed) continue;
            if (st.endsFrom.size() < upstreamCbTasks) continue;
            if (now - st.firstEndArrivedMs < endGraceMs) continue;

            flushBatch(batchId, st);
        }
    }

    private void flushBatch(int batchId, BatchState st) {
        long t0 = System.nanoTime();
        Map<String, Long> snapshot = mechanism.snapshot();
        long elapsedNs = System.nanoTime() - t0;

        st.flushed = true;
        lastFlushedBatch = Math.max(lastFlushedBatch, batchId);
        LOG.info("[MicroBatchDataPerturbation {}] Flushed batch {} -> {} keys (snapshot {} ms, {} contributions)",
                taskId, batchId, snapshot.size(), elapsedNs / 1_000_000L, contributionsThisBatch);
        contributionsThisBatch = 0L;

        if (ProfilerConfig.ENABLED) {
            profiler.recordEcall("snapshot", elapsedNs);
            profiler.incrementEcallTotal("snapshot");
        }

        collector.emit(new Values(snapshot, false, batchId, String.valueOf(taskId)));
    }

    @Override
    public void cleanup() {
        if (ProfilerConfig.ENABLED && profiler != null) {
            profiler.recordLifecycleEvent(BaselineBoltLifecycleEvent.COMPONENT_STOPPING);
            profiler.writeReport();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("histogram", "isDummy", "batchId", "producerId"));
        declarer.declareStream(ComponentConstants.CONTROL_STREAM, BatchMarker.fields());
    }

    private static boolean isTickTuple(Tuple t) {
        return "__system".equals(t.getSourceComponent())
                && "__tick".equals(t.getSourceStreamId());
    }
}
