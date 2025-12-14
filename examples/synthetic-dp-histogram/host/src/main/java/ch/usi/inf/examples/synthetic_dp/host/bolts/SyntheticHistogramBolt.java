package ch.usi.inf.examples.synthetic_dp.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticHistogramService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticSnapshotResponse;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticUpdateRequest;
import ch.usi.inf.examples.synthetic_dp.host.GroundTruthCollector;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyntheticHistogramBolt extends ConfidentialBolt<SyntheticHistogramService> {
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticHistogramBolt.class);
    private static final String OUTPUT_DIR = System.getProperty("synthetic.output.dir", "data");
    private static final int RUN_ID = Integer.getInteger("synthetic.run.id", 1);

    private final int tickSeconds;
    private final String outputFile;
    private int tickCount = 0;
    private long totalTuples = 0;

    // Async snapshot handling to avoid blocking the bolt on ticks (initialized in afterPrepare)
    private transient ExecutorService snapshotExecutor;
    private final AtomicBoolean snapshotRunning = new AtomicBoolean(false);
    private final AtomicBoolean pendingTick = new AtomicBoolean(false);

    public SyntheticHistogramBolt(int tickSeconds) {
        super(SyntheticHistogramService.class);
        this.tickSeconds = tickSeconds;
        this.outputFile = String.format("%s/synthetic-report-run%d.txt", OUTPUT_DIR, RUN_ID);
    }

    public Map<String, Object> getComponentConfiguration() {
        return Map.of(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickSeconds);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, org.apache.storm.task.TopologyContext context) {
        this.snapshotExecutor = Executors.newSingleThreadExecutor();

        int maxTimeSteps = ((Number) topoConf.getOrDefault("dp.max.time.steps", 100)).intValue();
        long mu = ((Number) topoConf.getOrDefault("dp.mu", 50L)).longValue();
        
        LOG.info("Configuring enclave service with maxTimeSteps={}, mu={}", maxTimeSteps, mu);
        try {
            state.getEnclaveManager().getService().configure(maxTimeSteps, mu);
        } catch (EnclaveServiceException e) {
            LOG.error("Failed to configure enclave service", e);
            throw new RuntimeException("Failed to configure enclave service", e);
        }
    }

    @Override
    protected void processTuple(Tuple input, SyntheticHistogramService service) throws EnclaveServiceException {
        // if tick tuple (processing-time) produce snapshot and write report
        if (isTick(input)) {
            // If a snapshot is already running, coalesce this tick and ack immediately.
            if (!snapshotRunning.compareAndSet(false, true)) {
                pendingTick.set(true);
                getCollector().ack(input);
                return;
            }

            // Run snapshot asynchronously to avoid blocking the bolt's execute thread
            snapshotExecutor.submit(() -> {
                try {
                    tickCount++;
                    LOG.info("Processing tick #{} - producing DP snapshot", tickCount);

                    SyntheticSnapshotResponse resp = service.snapshot();
                    LOG.info("Received DP snapshot with {} keys", resp.counts().size());

                    Map<String, Long> groundTruth = GroundTruthCollector.snapshot();
                    writeReport(resp.counts(), groundTruth);

                    LOG.info("Tick #{}: Processed {} total tuples, DP keys={}, GT keys={}",
                            tickCount, totalTuples, resp.counts().size(), groundTruth.size());
                } catch (Exception e) {
                    LOG.error("Error during asynchronous snapshot", e);
                } finally {
                    // Ack the original tick tuple
                    getCollector().ack(input);
                    snapshotRunning.set(false);

                    // If ticks arrived while running, trigger one more snapshot run
                    if (pendingTick.compareAndSet(true, false)) {
                        try {
                            // Submit a follow-up snapshot task (no tuple to ack here)
                            snapshotExecutor.submit(() -> {
                                try {
                                    tickCount++;
                                    LOG.info("Processing coalesced tick #{} - producing DP snapshot", tickCount);
                                    SyntheticSnapshotResponse resp = service.snapshot();
                                    LOG.info("Received DP snapshot with {} keys", resp.counts().size());
                                    Map<String, Long> groundTruth = GroundTruthCollector.snapshot();
                                    writeReport(resp.counts(), groundTruth);
                                    LOG.info("Tick #{}: Processed {} total tuples, DP keys={}, GT keys={}",
                                            tickCount, totalTuples, resp.counts().size(), groundTruth.size());
                                } catch (Exception ex) {
                                    LOG.error("Error during coalesced snapshot", ex);
                                }
                            });
                        } catch (Exception ex) {
                            LOG.error("Failed to submit coalesced snapshot task", ex);
                        }
                    }
                }
            });
            return;
        }

        // if data tuple (event-time) process update
        totalTuples++;
        LOG.trace("Processing data tuple #{} - updating histogram", totalTuples);

        EncryptedValue key = (EncryptedValue) input.getValueByField("key");
        EncryptedValue count = (EncryptedValue) input.getValueByField("count");
        EncryptedValue user = (EncryptedValue) input.getValueByField("user");

        // pass encrypted values to service for update
        service.update(new SyntheticUpdateRequest(key, count, user));

        // acknowledge tuple
        getCollector().ack(input);
    }

    private boolean isTick(Tuple t) {
        return t.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && t.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void writeReport(Map<String, Long> dp, Map<String, Long> gt) {
        LOG.debug("Computing metrics comparing DP histogram with ground truth");

        // L0: Number of retained keys (keys with count > 0)
        long l0 = dp.values().stream().filter(v -> v > 0).count();

        // Union of all keys from both histograms
        var union = new java.util.HashSet<String>();
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

        // Write header on first tick
        boolean isFirstTick = tickCount == 1;
        try (PrintWriter out = new PrintWriter(new FileWriter(file, !isFirstTick))) {
            if (isFirstTick) {
                out.println("# Synthetic DP Histogram Benchmark - Run " + RUN_ID);
                out.println("# Timestamp: " + Instant.now());
                out.println("# Format: tick, timestamp, keys_retained(l0), l_inf, l_1, l_2, dp_keys, gt_keys");
                out.println("#");
            }

            out.printf("tick_%04d,%s,%d,%d,%d,%.2f,%d,%d%n",
                    tickCount, Instant.now(), l0, lInf, l1, l2, dp.size(), gt.size());
            out.flush();

            LOG.info("Tick {} metrics: l0={}, l_inf={}, l_1={}, l_2={}",
                    tickCount, l0, lInf, l1, String.format("%.2f", l2));
        } catch (IOException e) {
            LOG.error("Error writing report to {}", outputFile, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // sink
    }
}
