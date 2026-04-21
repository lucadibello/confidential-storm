package ch.usi.inf.examples.synthetic_baseline.bolts;

import ch.usi.inf.examples.synthetic_baseline.config.DPConfig;
import ch.usi.inf.examples.synthetic_baseline.profiling.BaselineBoltLifecycleEvent;
import ch.usi.inf.examples.synthetic_baseline.profiling.BoltProfiler;
import ch.usi.inf.examples.synthetic_baseline.profiling.DPBoltLifecycleEvent;
import ch.usi.inf.examples.synthetic_baseline.profiling.ProfilerConfig;
import ch.usi.inf.examples.synthetic_baseline.util.ComponentConstants;
import ch.usi.inf.examples.synthetic_baseline.util.GroundTruthCollector;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.*;

/**
 * Baseline histogram aggregation bolt - receives partial histograms,
 * merges them in-JVM, and writes benchmark reports.
 * <p>
 * This bolt matches the aggregation logic from the enclave-side AbstractHistogramAggregationServiceProvider
 * and the tick-based release logic from AbstractHistogramAggregationBolt.
 */
public class BaselineHistogramAggregationBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BaselineHistogramAggregationBolt.class);

    private static final String DEFAULT_OUTPUT_DIR = "/logs/storm/profiler";
    private static final String DUMMY_MARKER_KEY = "__dummy";
    private static final int MAX_PENDING_EPOCHS = 8;

    private OutputCollector collector;
    private int expectedUpstreamTasks = -1;
    private int maxTimeSteps;
    private int tickIntervalSecs;
    private boolean finished = false;

    // Tick-based release state
    private Map<String, Long> lastCompleteHistogram = null;
    private int lastCompletedEpoch = -1;
    private boolean hasNewHistogram = false;
    private int roundCount = 0;

    // Per-epoch aggregation state (inlined from enclave service)
    private final TreeMap<Integer, EpochState> epochStates = new TreeMap<>();

    private String outputFile;

    private transient BoltProfiler profiler;
    private transient int ticksSinceLastCompletion;
    private transient int dummyMergesThisEpoch;
    private transient int realMergesThisEpoch;

    private static final class EpochState {
        final Map<String, Long> mergedHistogram = new HashMap<>();
        final Set<String> contributors = new HashSet<>();
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.expectedUpstreamTasks = context.getComponentTasks(
                ComponentConstants.BOLT_DATA_PERTURBATION).size();
        this.maxTimeSteps = ((Number) topoConf.getOrDefault("dp.max.time.steps", DPConfig.maxTimeSteps())).intValue();
        this.tickIntervalSecs = ((Number) topoConf.getOrDefault("dp.tick.interval.secs", 5)).intValue();
        String outputDir = (String) topoConf.getOrDefault("synthetic.output.dir", DEFAULT_OUTPUT_DIR);
        int runId = ((Number) topoConf.getOrDefault("synthetic.run.id", 1)).intValue();
        this.outputFile = String.format("%s/synthetic-report-run%d.txt", outputDir, runId);

        if (ProfilerConfig.ENABLED) {
            this.profiler = new BoltProfiler(context.getThisComponentId(), context.getThisTaskId());
            profiler.recordLifecycleEvent(BaselineBoltLifecycleEvent.COMPONENT_STARTED);
            profiler.recordLifecycleEvent(DPBoltLifecycleEvent.TICK_INTERVAL_SECS, tickIntervalSecs);
            profiler.recordLifecycleEvent(DPBoltLifecycleEvent.MAX_EPOCHS_CONFIGURED, maxTimeSteps);
        }

        LOG.info("[BaselineHistogramAggregation] Expecting {} upstream producers, output: {}",
                expectedUpstreamTasks, outputFile);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Integer.getInteger("dp.tick.interval.secs", 5));
        return config;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input) {
        if (finished) {
            if (!isTickTuple(input)) collector.ack(input);
            return;
        }

        if (isTickTuple(input)) {
            handleTick();
        } else {
            handlePartialHistogram(input);
        }
    }

    private void handleTick() {
        if (ProfilerConfig.ENABLED) {
            ticksSinceLastCompletion++;
            profiler.onTick();
        }

        if (hasNewHistogram && lastCompleteHistogram != null) {
            LOG.info("[BaselineHistogramAggregation] Releasing histogram for epoch {} ({} keys)",
                    lastCompletedEpoch, lastCompleteHistogram.size());
            processCompleteHistogram(lastCompleteHistogram);
            hasNewHistogram = false;
            if (ProfilerConfig.ENABLED) profiler.incrementCounter("histograms_released");
        } else if (lastCompleteHistogram != null) {
            LOG.info("[BaselineHistogramAggregation] No new histogram at release tick (last was epoch {})",
                    lastCompletedEpoch);
            processStaleHistogram(lastCompleteHistogram);
            if (ProfilerConfig.ENABLED) profiler.incrementCounter("stale_releases");
        } else {
            LOG.info("[BaselineHistogramAggregation] No histogram available yet at release tick");
        }
    }

    @SuppressWarnings("unchecked")
    private void handlePartialHistogram(Tuple input) {
        Map<String, Long> partial = (Map<String, Long>) input.getValueByField("histogram");
        boolean isDummy = input.getBooleanByField("isDummy");
        int epoch = input.getIntegerByField("epoch");
        String producerId = input.getStringByField("producerId");

        // Time the merge operation
        long t0 = ProfilerConfig.ENABLED && profiler.shouldSample() ? System.nanoTime() : 0;

        // Discard dummies
        if (isDummy || partial.containsKey(DUMMY_MARKER_KEY)) {
            LOG.info("[BaselineHistogramAggregation] Dummy partial discarded (epoch {}, producer {})",
                    epoch, producerId);
            if (ProfilerConfig.ENABLED) {
                profiler.incrementCounter("dummies_received");
                dummyMergesThisEpoch++;
            }
            if (t0 != 0) profiler.recordEcall("mergePartial", System.nanoTime() - t0);
            if (ProfilerConfig.ENABLED) profiler.incrementEcallTotal("mergePartial");
            collector.ack(input);
            return;
        }

        // Get or create epoch state
        EpochState state = epochStates.computeIfAbsent(epoch, k -> new EpochState());

        // Evict oldest epochs if too many pending
        while (epochStates.size() > MAX_PENDING_EPOCHS) {
            int oldestEpoch = epochStates.firstKey();
            epochStates.remove(oldestEpoch);
            LOG.warn("[BaselineHistogramAggregation] Evicted stale epoch {} (pending exceeded {})",
                    oldestEpoch, MAX_PENDING_EPOCHS);
        }

        // Reject duplicates from same producer in same epoch
        if (state.contributors.contains(producerId)) {
            LOG.warn("[BaselineHistogramAggregation] Duplicate partial from producer {} in epoch {}, ignoring",
                    producerId, epoch);
            if (t0 != 0) profiler.recordEcall("mergePartial", System.nanoTime() - t0);
            if (ProfilerConfig.ENABLED) profiler.incrementEcallTotal("mergePartial");
            collector.ack(input);
            return;
        }

        // Merge partial into epoch accumulator
        for (Map.Entry<String, Long> entry : partial.entrySet()) {
            state.mergedHistogram.merge(entry.getKey(), entry.getValue(), Long::sum);
        }
        state.contributors.add(producerId);

        if (t0 != 0) profiler.recordEcall("mergePartial", System.nanoTime() - t0);
        if (ProfilerConfig.ENABLED) {
            profiler.incrementEcallTotal("mergePartial");
            profiler.incrementCounter("real_partials_received");
            realMergesThisEpoch++;
        }

        LOG.info("[BaselineHistogramAggregation] Received partial from producer {} ({}/{} in epoch {})",
                producerId, state.contributors.size(), expectedUpstreamTasks, epoch);

        // Check if all replicas have contributed
        if (state.contributors.size() >= expectedUpstreamTasks) {
            Map<String, Long> result = new HashMap<>(state.mergedHistogram);
            epochStates.remove(epoch);

            lastCompleteHistogram = result;
            lastCompletedEpoch = epoch;
            hasNewHistogram = true;
            LOG.info("[BaselineHistogramAggregation] Epoch {} complete ({} keys), buffered for next release tick",
                    epoch, result.size());

            if (ProfilerConfig.ENABLED) {
                profiler.recordGauge("ticks_to_completion", ticksSinceLastCompletion);
                profiler.recordGauge("dummy_merges_this_epoch", dummyMergesThisEpoch);
                profiler.recordGauge("real_merges_this_epoch", realMergesThisEpoch);
                profiler.recordGauge("total_merges_this_epoch", dummyMergesThisEpoch + realMergesThisEpoch);
                profiler.recordLifecycleEvent(DPBoltLifecycleEvent.EPOCH_ADVANCED, lastCompletedEpoch);

                ticksSinceLastCompletion = 0;
                realMergesThisEpoch = 0;
                dummyMergesThisEpoch = 0;
            }

            // Check max epoch limit
            if (maxTimeSteps > 0 && lastCompletedEpoch >= maxTimeSteps) {
                finished = true;
                LOG.info("[BaselineHistogramAggregation] Reached max epochs ({}), deactivating",
                        maxTimeSteps);
                if (ProfilerConfig.ENABLED) {
                    profiler.recordLifecycleEvent(DPBoltLifecycleEvent.MAX_EPOCH_REACHED, lastCompletedEpoch);
                    profiler.writeReport();
                }
            }
        }

        collector.ack(input);
    }

    private void processCompleteHistogram(Map<String, Long> mergedHistogram) {
        roundCount++;
        LOG.info("[DP-GLOBAL-COMPLETE] Received complete histogram for round #{} with {} keys",
                roundCount, mergedHistogram.size());

        Map<String, Long> groundTruth = GroundTruthCollector.snapshot();
        writeReport(mergedHistogram, groundTruth, false);
    }

    private void processStaleHistogram(Map<String, Long> staleHistogram) {
        LOG.info("[DP-GLOBAL-STALE] Received stale histogram for round #{} with {} keys",
                roundCount, staleHistogram.size());

        Map<String, Long> groundTruth = GroundTruthCollector.snapshot();
        writeReport(staleHistogram, groundTruth, true);
    }

    private void writeReport(Map<String, Long> dp, Map<String, Long> gt, boolean stale) {
        long l0 = dp.values().stream().filter(v -> v > 0).count();

        var union = new HashSet<String>();
        union.addAll(dp.keySet());
        union.addAll(gt.keySet());

        long lInf = union.stream()
                .mapToLong(k -> Math.abs(dp.getOrDefault(k, 0L) - gt.getOrDefault(k, 0L)))
                .max().orElse(0L);

        long l1 = union.stream()
                .mapToLong(k -> Math.abs(dp.getOrDefault(k, 0L) - gt.getOrDefault(k, 0L)))
                .sum();

        double l2 = Math.sqrt(union.stream()
                .mapToLong(k -> {
                    long diff = dp.getOrDefault(k, 0L) - gt.getOrDefault(k, 0L);
                    return diff * diff;
                }).sum());

        File file = new File(outputFile);
        file.getParentFile().mkdirs();

        boolean isFirstWrite = roundCount == 1 && !stale;
        try (PrintWriter out = new PrintWriter(new FileWriter(file, !isFirstWrite))) {
            if (isFirstWrite) {
                out.println("# Synthetic DP Histogram Baseline Benchmark - Run " + RUN_ID);
                out.println("# Timestamp: " + Instant.now());
                out.println("# Format: tick, timestamp, stale, keys_retained(l0), l_inf, l_1, l_2, dp_keys, gt_keys");
                out.println("#");
            }

            out.printf("tick_%04d,%s,%s,%d,%d,%d,%.2f,%d,%d%n",
                    roundCount, Instant.now(), stale ? "stale" : "fresh", l0, lInf, l1, l2, dp.size(), gt.size());
            out.flush();

            LOG.info("Round {} metrics: l0={}, l_inf={}, l_1={}, l_2={:.2f}, stale={}",
                    roundCount, l0, lInf, l1, l2, stale);
        } catch (IOException e) {
            LOG.error("Error writing report to {}", outputFile, e);
        }
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
        // Terminal sink -- no output streams
    }

    private static boolean isTickTuple(Tuple tuple) {
        return "__system".equals(tuple.getSourceComponent())
                && "__tick".equals(tuple.getSourceStreamId());
    }
}
