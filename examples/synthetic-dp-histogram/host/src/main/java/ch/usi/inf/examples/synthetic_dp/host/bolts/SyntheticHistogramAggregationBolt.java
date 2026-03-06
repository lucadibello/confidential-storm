package ch.usi.inf.examples.synthetic_dp.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.HistogramAggregationService;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.dp.AbstractHistogramAggregationBolt;
import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import ch.usi.inf.examples.synthetic_dp.host.GroundTruthCollector;
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
import java.util.HashSet;
import java.util.Map;

public class SyntheticHistogramAggregationBolt extends AbstractHistogramAggregationBolt {
    private static final String OUTPUT_DIR = System.getProperty("synthetic.output.dir", "data");
    private static final int RUN_ID = Integer.getInteger("synthetic.run.id", 1);
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticHistogramAggregationBolt.class);

    private final String outputFile;
    private int roundCount = 0;

    public SyntheticHistogramAggregationBolt() {
        this.outputFile = String.format("%s/synthetic-report-run%d.txt", OUTPUT_DIR, RUN_ID);
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
    protected void processCompleteHistogram(Map<String, Long> mergedHistogram) {
        roundCount++;
        LOG.info("[DP-GLOBAL-COMPLETE] Received complete histogram for round #{} with {} keys", roundCount, mergedHistogram.size());

        Map<String, Long> groundTruth = GroundTruthCollector.snapshot();
        writeReport(mergedHistogram, groundTruth, false);
    }

    @Override
    protected void processStaleHistogram(Map<String, Long> staleHistogram) {
        LOG.info("[DP-GLOBAL-STALE] Received stale histogram for round #{} with {} keys", roundCount, staleHistogram.size());

        Map<String, Long> groundTruth = GroundTruthCollector.snapshot();
        writeReport(staleHistogram, groundTruth, true);
    }

    private void writeReport(Map<String, Long> dp, Map<String, Long> gt, boolean stale) {
        // L0: Number of retained keys (keys with count > 0)
        long l0 = dp.values().stream().filter(v -> v > 0).count();

        // Union of all keys from both histograms
        var union = new HashSet<String>();
        union.addAll(dp.keySet());
        union.addAll(gt.keySet());

        // L_inf: Maximum absolute error
        long lInf = union.stream()
                .mapToLong(k -> Math.abs(dp.getOrDefault(k, 0L) - gt.getOrDefault(k, 0L)))
                .max().orElse(0L);

        // L1: Sum of absolute errors
        long l1 = union.stream()
                .mapToLong(k -> Math.abs(dp.getOrDefault(k, 0L) - gt.getOrDefault(k, 0L)))
                .sum();

        // L2: Euclidean distance (RMS error)
        double l2 = Math.sqrt(union.stream()
                .mapToLong(k -> {
                    long diff = dp.getOrDefault(k, 0L) - gt.getOrDefault(k, 0L);
                    return diff * diff;
                }).sum());

        // Write to file in append mode
        File file = new File(outputFile);
        file.getParentFile().mkdirs();

        boolean isFirstWrite = roundCount == 1 && !stale;
        try (PrintWriter out = new PrintWriter(new FileWriter(file, !isFirstWrite))) {
            if (isFirstWrite) {
                out.println("# Synthetic DP Histogram Benchmark - Run " + RUN_ID);
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
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Terminal sink — no output streams
    }
}
