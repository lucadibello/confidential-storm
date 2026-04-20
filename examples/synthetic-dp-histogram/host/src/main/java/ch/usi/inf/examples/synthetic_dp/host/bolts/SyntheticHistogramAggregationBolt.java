package ch.usi.inf.examples.synthetic_dp.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.HistogramAggregationService;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.dp.AbstractHistogramAggregationBolt;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class SyntheticHistogramAggregationBolt extends AbstractHistogramAggregationBolt {
    private static final String DEFAULT_OUTPUT_DIR = "/logs/storm/profiler";
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticHistogramAggregationBolt.class);

    private String outputFile;
    private int runId;
    private int roundCount = 0;
    private int maxEpochs;
    private int tickIntervalSecs;

    /**
     * Worker-local ground-truth histogram populated from the spout's ground-truth stream.
     * Stays empty when {@code synthetic.ground-truth.enabled=false}: the bolt always
     * subscribes to the stream, but the spout skips emission on it, so no tuples arrive.
     * <p>
     * The aggregation bolt has parallelism=1, so a plain {@link HashMap} is safe: all
     * ground-truth tuples and all complete-histogram callbacks run on the bolt's single
     * executor thread.
     */
    private final Map<String, Long> groundTruth = new HashMap<>();

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        this.maxEpochs = ((Number) topoConf.getOrDefault("dp.max.time.steps", DPConfig.maxTimeSteps())).intValue();
        this.tickIntervalSecs = ((Number) topoConf.getOrDefault("dp.tick.interval.secs", 5)).intValue();
        String outputDir = (String) topoConf.getOrDefault("synthetic.output.dir", DEFAULT_OUTPUT_DIR);
        this.runId = ((Number) topoConf.getOrDefault("synthetic.run.id", 1)).intValue();
        this.outputFile = String.format("%s/synthetic-report-run%d.txt", outputDir, this.runId);
        super.afterPrepare(topoConf, context);
    }

    @Override
    protected int getMaxEpochs() {
        return maxEpochs;
    }

    @Override
    protected int getTickIntervalSecs() {
        return tickIntervalSecs > 0 ? tickIntervalSecs : Integer.getInteger("dp.tick.interval.secs", super.getTickIntervalSecs());
    }

    @Override
    protected int getExpectedUpstreamTaskCount(TopologyContext context) {
        return context.getComponentTasks(
                ComponentConstants.BOLT_DATA_PERTURBATION.toString()
        ).size();
    }

    @Override
    protected void configureService(HistogramAggregationService service, TopologyContext context) {
        int upstreamTasks = getExpectedUpstreamTaskCount(context);

        LOG.info("SyntheticHistogramAggregationBolt: configuring with upstream parallelism={}, output file: {}",
                upstreamTasks, outputFile);

        // Pass runtime replica count to the enclave service via ECALL
        service.setExpectedReplicaCount(upstreamTasks);
    }

    @Override
    protected EncryptedValue getEncryptedPartialHistogram(Tuple input) {
        return (EncryptedValue) input.getValueByField("encryptedHistogram");
    }

    @Override
    protected void processTuple(Tuple input, HistogramAggregationService service) throws EnclaveServiceException {
        // ground-truth tuples are plaintext and never go through the enclave -- intercept before super()
        if (ComponentConstants.GROUND_TRUTH_STREAM.equals(input.getSourceStreamId())) {
            String key = input.getStringByField("key");
            groundTruth.merge(key, 1L, Long::sum);
            getCollector().ack(input);
            return;
        }
        super.processTuple(input, service);
    }

    @Override
    protected void processCompleteHistogram(Map<String, Long> mergedHistogram) {
        roundCount++;
        LOG.info("[DP-GLOBAL-COMPLETE] Received complete histogram for round #{} with {} keys", roundCount, mergedHistogram.size());
        writeReport(mergedHistogram, Collections.unmodifiableMap(groundTruth), false);
    }

    @Override
    protected void processStaleHistogram(Map<String, Long> staleHistogram) {
        LOG.info("[DP-GLOBAL-STALE] Received stale histogram for round #{} with {} keys", roundCount, staleHistogram.size());
        writeReport(staleHistogram, Collections.unmodifiableMap(groundTruth), true);
    }

    private void writeReport(Map<String, Long> dp, Map<String, Long> gt, boolean stale) {
        boolean isFinal = roundCount == maxEpochs;

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
                out.println("# Synthetic DP Histogram Benchmark - Run " + runId);
                out.println("# Timestamp: " + Instant.now());
                out.println("# Format: tick, timestamp, stale, final, keys_retained(l0), l_inf, l_1, l_2, dp_keys, gt_keys");
                out.println("#");
            }

            out.printf("tick_%04d,%s,%s,%s,%d,%d,%d,%.2f,%d,%d%n",
                    roundCount, Instant.now(),
                    stale ? "stale" : "fresh",
                    isFinal ? "final" : "-",
                    l0, lInf, l1, l2, dp.size(), gt.size());
            out.flush();

            if (isFinal) {
                LOG.info("[FINAL] Run {} epoch {}/{}: l0={} l_inf={} l_1={} l_2={:.2f} dp_keys={} gt_keys={}",
                        runId, roundCount, maxEpochs, l0, lInf, l1, l2, dp.size(), gt.size());
            } else {
                LOG.info("Round {}/{}: l0={} l_inf={} l_1={} l_2={:.2f} dp_keys={} gt_keys={} stale={}",
                        roundCount, maxEpochs, l0, lInf, l1, l2, dp.size(), gt.size(), stale);
            }
        } catch (IOException e) {
            LOG.error("Error writing report to {}", outputFile, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Terminal sink -- no output streams
    }
}
