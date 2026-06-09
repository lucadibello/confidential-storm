package ch.usi.inf.examples.microbatch_baseline.bolts;

import ch.usi.inf.examples.microbatch_baseline.profiling.BaselineBoltLifecycleEvent;
import ch.usi.inf.examples.microbatch_baseline.profiling.BoltProfiler;
import ch.usi.inf.examples.microbatch_baseline.profiling.ProfilerConfig;
import ch.usi.inf.examples.microbatch_baseline.util.BatchCompletionCoordinator;
import ch.usi.inf.examples.microbatch_baseline.util.BatchMarker;
import ch.usi.inf.examples.microbatch_baseline.util.ComponentConstants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Aggregation bolt for the micro-batch benchmark.
 *
 * <p>Drives the timing measurement at the topology sink:
 * <ul>
 *   <li>On BEGIN(batchId, t0): records the spout-side start timestamp.</li>
 *   <li>On partial-histogram tuples (one per DataPerturbation replica per
 *       batch): merges into the per-batch accumulator.</li>
 *   <li>On END(batchId) received from <em>all</em> upstream DataPerturbation
 *       replicas <em>and</em> all expected partials accounted for, records
 *       the wall-clock end timestamp, computes the elapsed duration, appends
 *       a CSV row, and publishes batch completion to
 *       {@link BatchCompletionCoordinator} so the spout proceeds to the next
 *       batch.</li>
 * </ul>
 */
public class BaselineHistogramAggregationBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BaselineHistogramAggregationBolt.class);

    private static final String DEFAULT_OUTPUT_DIR = "/logs/storm/profiler";
    private static final String CSV_HEADER = "run_id,parallelism,batch_id,size_gb,n_records,"
            + "bytes_per_tuple,duration_ms,t_begin_epoch_ms,t_end_epoch_ms,dp_keys,producers";

    private OutputCollector collector;
    private int expectedDpTasks = -1;
    private int parallelism = -1;
    private int runId;
    private Path csvPath;

    private transient BatchCompletionCoordinator completion;
    private transient BoltProfiler profiler;

    private final TreeMap<Integer, BatchTiming> batches = new TreeMap<>();

    private static final class BatchTiming {
        double sizeGb;
        long recordCount;
        long bytesPerTuple;
        long beginEpochMs = -1L;
        long beginNanos = -1L;
        boolean beginRecorded = false;

        final Set<Integer> endsFrom = new HashSet<>();

        final Map<String, Long> merged = new HashMap<>();
        final Set<String> producers = new HashSet<>();

        boolean completed = false;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.expectedDpTasks = context.getComponentTasks(
                ComponentConstants.BOLT_DATA_PERTURBATION).size();
        this.parallelism = expectedDpTasks;

        String outputDir = (String) topoConf.getOrDefault("synthetic.output.dir", DEFAULT_OUTPUT_DIR);
        this.runId = ((Number) topoConf.getOrDefault("synthetic.run.id", 1)).intValue();
        this.csvPath = Paths.get(outputDir, String.format("microbatch-baseline-run%d.csv", runId));

        try {
            Files.createDirectories(csvPath.getParent());
            if (!Files.exists(csvPath)) {
                try (BufferedWriter w = Files.newBufferedWriter(csvPath, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE_NEW)) {
                    w.write(CSV_HEADER);
                    w.newLine();
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to initialise CSV at {}", csvPath, e);
        }

        this.completion = new BatchCompletionCoordinator(topoConf, context.getStormId());

        if (ProfilerConfig.ENABLED) {
            this.profiler = new BoltProfiler(context.getThisComponentId(), context.getThisTaskId());
            profiler.recordLifecycleEvent(BaselineBoltLifecycleEvent.COMPONENT_STARTED);
        }

        LOG.info("[MicroBatchAggregator] expecting {} DP tasks, CSV -> {}", expectedDpTasks, csvPath);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input) {
        if (ComponentConstants.CONTROL_STREAM.equals(input.getSourceStreamId())) {
            handleControl(input);
            collector.ack(input);
            return;
        }

        Map<String, Long> partial = (Map<String, Long>) input.getValueByField("histogram");
        int batchId = input.getIntegerByField("batchId");
        String producerId = input.getStringByField("producerId");

        BatchTiming b = batches.computeIfAbsent(batchId, k -> new BatchTiming());
        if (!b.producers.add(producerId)) {
            LOG.warn("[MicroBatchAggregator] duplicate partial from {} for batch {}, ignoring",
                    producerId, batchId);
            collector.ack(input);
            return;
        }
        for (Map.Entry<String, Long> e : partial.entrySet()) {
            b.merged.merge(e.getKey(), e.getValue(), Long::sum);
        }
        LOG.info("[MicroBatchAggregator] partial from {} for batch {} ({}/{} producers, {} keys merged)",
                producerId, batchId, b.producers.size(), expectedDpTasks, b.merged.size());

        tryComplete(batchId, b);
        collector.ack(input);
    }

    private void handleControl(Tuple input) {
        String type = input.getStringByField(BatchMarker.F_TYPE);
        int batchId = input.getIntegerByField(BatchMarker.F_BATCH_ID);

        BatchTiming b = batches.computeIfAbsent(batchId, k -> new BatchTiming());
        if (BatchMarker.BEGIN.equals(type)) {
            if (!b.beginRecorded) {
                b.sizeGb = input.getDoubleByField(BatchMarker.F_SIZE_GB);
                b.recordCount = input.getLongByField(BatchMarker.F_RECORD_COUNT);
                b.bytesPerTuple = input.getLongByField(BatchMarker.F_BYTES_PER_TUPLE);
                b.beginNanos = input.getLongByField(BatchMarker.F_T_NANOS);
                b.beginEpochMs = input.getLongByField(BatchMarker.F_T_EPOCH_MS);
                b.beginRecorded = true;
                LOG.info("[MicroBatchAggregator] BEGIN batch {} (size={} GB, target {} records)",
                        batchId, b.sizeGb, b.recordCount);
            }
        } else if (BatchMarker.END.equals(type)) {
            b.endsFrom.add(input.getSourceTask());
            LOG.info("[MicroBatchAggregator] END for batch {} from task {} ({}/{} ends)",
                    batchId, input.getSourceTask(), b.endsFrom.size(), expectedDpTasks);
            tryComplete(batchId, b);
        }
    }

    private void tryComplete(int batchId, BatchTiming b) {
        if (b.completed) return;
        if (!b.beginRecorded) return;
        if (b.producers.size() < expectedDpTasks) return;
        if (b.endsFrom.size() < expectedDpTasks) return;

        long endEpochMs = System.currentTimeMillis();
        long durationMs = endEpochMs - b.beginEpochMs;
        b.completed = true;

        LOG.info("[MicroBatchAggregator] *** batch {} COMPLETE: size={} GB, records={}, duration={} ms, keys={}",
                batchId, b.sizeGb, b.recordCount, durationMs, b.merged.size());

        appendCsv(batchId, b, endEpochMs, durationMs);
        completion.publishCompletion(batchId);

        if (ProfilerConfig.ENABLED) {
            profiler.incrementCounter("batches_completed");
        }
    }

    private void appendCsv(int batchId, BatchTiming b, long endEpochMs, long durationMs) {
        try (BufferedWriter w = Files.newBufferedWriter(csvPath, StandardCharsets.UTF_8,
                StandardOpenOption.APPEND)) {
            w.write(String.format("%d,%d,%d,%.4f,%d,%d,%d,%d,%d,%d,%d",
                    runId, parallelism, batchId, b.sizeGb, b.recordCount,
                    b.bytesPerTuple, durationMs, b.beginEpochMs, endEpochMs,
                    b.merged.size(), b.producers.size()));
            w.newLine();
        } catch (IOException e) {
            LOG.error("Failed to append CSV row for batch {}", batchId, e);
        }
    }

    @Override
    public void cleanup() {
        if (ProfilerConfig.ENABLED && profiler != null) {
            profiler.recordLifecycleEvent(BaselineBoltLifecycleEvent.COMPONENT_STOPPING);
            profiler.writeReport();
        }
        if (completion != null) completion.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Terminal sink.
    }
}
