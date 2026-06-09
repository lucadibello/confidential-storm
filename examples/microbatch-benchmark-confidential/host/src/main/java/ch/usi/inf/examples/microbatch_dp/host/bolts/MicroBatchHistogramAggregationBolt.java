package ch.usi.inf.examples.microbatch_dp.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.HistogramAggregationService;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.dp.AbstractHistogramAggregationBolt;
import ch.usi.inf.examples.microbatch_dp.common.config.DPConfig;
import ch.usi.inf.examples.microbatch_dp.host.topology.BatchCompletionCoordinator;
import ch.usi.inf.examples.microbatch_dp.host.topology.BatchMarker;
import ch.usi.inf.examples.microbatch_dp.common.topology.ComponentConstants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Confidential micro-batch aggregator.
 *
 * <p>Reuses the framework's tick-driven {@link AbstractHistogramAggregationBolt}
 * for the (currently dormant) DP histogram merging path. Adds:
 * <ul>
 *   <li>BEGIN/END control marker handling: BEGIN records {@code t0}, END from
 *       all upstream DP replicas finalises {@code t1}.</li>
 *   <li>Per-batch CSV row appending (one row per completed batch).</li>
 *   <li>ZooKeeper-published completion notification so the spout can advance
 *       to the next batch sequentially.</li>
 * </ul>
 *
 * <p>Note: in micro-batch mode the upstream DP bolt disables ticks (so no
 * encrypted partials flow through), and timing is anchored on the BEGIN/END
 * markers only. The existing aggregator-side DP merge machinery stays wired
 * up but is effectively dormant.
 */
public class MicroBatchHistogramAggregationBolt extends AbstractHistogramAggregationBolt {
    private static final String DEFAULT_OUTPUT_DIR = "/logs/storm/profiler";
    private static final String CSV_HEADER = "run_id,parallelism,batch_id,size_gb,n_records,"
            + "bytes_per_tuple,duration_ms,t_begin_epoch_ms,t_end_epoch_ms,producers";
    private static final Logger LOG = LoggerFactory.getLogger(MicroBatchHistogramAggregationBolt.class);

    private static final int TICK_DISABLED_SECS = Integer.MAX_VALUE / 4;

    private int runId;
    private int maxEpochs;
    private int parallelism = -1;
    private int expectedDpTasks = -1;
    private Path csvPath;
    private transient BatchCompletionCoordinator completion;

    private final TreeMap<Integer, BatchTiming> batches = new TreeMap<>();

    private static final class BatchTiming {
        double sizeGb;
        long recordCount;
        long bytesPerTuple;
        long beginEpochMs = -1L;
        long beginNanos = -1L;
        boolean beginRecorded = false;
        final Set<Integer> endsFrom = new HashSet<>();
        boolean completed = false;
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        this.maxEpochs = ((Number) topoConf.getOrDefault("dp.max.time.steps", DPConfig.maxTimeSteps())).intValue();
        String outputDir = (String) topoConf.getOrDefault("synthetic.output.dir", DEFAULT_OUTPUT_DIR);
        this.runId = ((Number) topoConf.getOrDefault("synthetic.run.id", 1)).intValue();
        this.expectedDpTasks = context.getComponentTasks(
                ComponentConstants.BOLT_DATA_PERTURBATION.toString()).size();
        this.parallelism = expectedDpTasks;
        this.csvPath = Paths.get(outputDir, String.format("microbatch-confidential-run%d.csv", runId));

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

        super.afterPrepare(topoConf, context);
        LOG.info("[MicroBatchAggregator] expecting {} DP tasks, CSV -> {}", expectedDpTasks, csvPath);
    }

    @Override
    protected int getMaxEpochs() {
        return maxEpochs;
    }

    @Override
    protected int getTickIntervalSecs() {
        return TICK_DISABLED_SECS;
    }

    @Override
    protected int getExpectedUpstreamTaskCount(TopologyContext context) {
        return context.getComponentTasks(
                ComponentConstants.BOLT_DATA_PERTURBATION.toString()).size();
    }

    @Override
    protected void configureService(HistogramAggregationService service, TopologyContext context) {
        int upstreamTasks = getExpectedUpstreamTaskCount(context);
        service.setExpectedReplicaCount(upstreamTasks);
        LOG.info("[MicroBatchAggregator] configured with upstream parallelism={}", upstreamTasks);
    }

    @Override
    protected EncryptedValue getEncryptedPartialHistogram(Tuple input) {
        return (EncryptedValue) input.getValueByField("encryptedHistogram");
    }

    @Override
    protected void processTuple(Tuple input, HistogramAggregationService service) throws EnclaveServiceException {
        if (ComponentConstants.CONTROL_STREAM.equals(input.getSourceStreamId())) {
            handleControl(input);
            getCollector().ack(input);
            return;
        }
        if (ComponentConstants.GROUND_TRUTH_STREAM.equals(input.getSourceStreamId())) {
            getCollector().ack(input);
            return;
        }
        super.processTuple(input, service);
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
        if (b.endsFrom.size() < expectedDpTasks) return;

        long endEpochMs = System.currentTimeMillis();
        long durationMs = endEpochMs - b.beginEpochMs;
        b.completed = true;

        LOG.info("[MicroBatchAggregator] *** batch {} COMPLETE: size={} GB, records={}, duration={} ms",
                batchId, b.sizeGb, b.recordCount, durationMs);

        appendCsv(batchId, b, endEpochMs, durationMs);
        completion.publishCompletion(batchId);
    }

    private void appendCsv(int batchId, BatchTiming b, long endEpochMs, long durationMs) {
        try (BufferedWriter w = Files.newBufferedWriter(csvPath, StandardCharsets.UTF_8,
                StandardOpenOption.APPEND)) {
            w.write(String.format("%d,%d,%d,%.4f,%d,%d,%d,%d,%d,%d",
                    runId, parallelism, batchId, b.sizeGb, b.recordCount,
                    b.bytesPerTuple, durationMs, b.beginEpochMs, endEpochMs,
                    b.endsFrom.size()));
            w.newLine();
        } catch (IOException e) {
            LOG.error("Failed to append CSV row for batch {}", batchId, e);
        }
    }

    @Override
    protected void processCompleteHistogram(Map<String, Long> mergedHistogram) {
        // Dormant in micro-batch mode -- DP partials are not emitted.
    }

    @Override
    protected void processStaleHistogram(Map<String, Long> staleHistogram) {
        // Dormant in micro-batch mode.
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Terminal sink.
    }

    @Override
    protected void beforeCleanup() {
        if (completion != null) completion.close();
        super.beforeCleanup();
    }
}
