package ch.usi.inf.examples.microbatch_baseline.spouts;

import ch.usi.inf.examples.microbatch_baseline.config.DPConfig;
import ch.usi.inf.examples.microbatch_baseline.config.MicroBatchConfig;
import ch.usi.inf.examples.microbatch_baseline.config.MicroBatchConfig.BatchPlan;
import ch.usi.inf.examples.microbatch_baseline.util.BatchCompletionCoordinator;
import ch.usi.inf.examples.microbatch_baseline.util.BatchMarker;
import ch.usi.inf.examples.microbatch_baseline.util.ComponentConstants;
import ch.usi.inf.examples.microbatch_baseline.util.ZipfMandelbrotDistribution;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Micro-batch baseline spout: emits a sequence of fixed-byte-budget batches
 * separated by BEGIN/END control markers, waiting for each batch to drain
 * end-to-end (signalled via {@link BatchCompletionCoordinator}) before starting
 * the next one.
 *
 * <p>For batch {@code b} of target size {@code S_b} GB:
 * <ol>
 *   <li>Emit {@code BEGIN(b, S_b, N_b, bytesPerTuple, t0)} on the control
 *       stream (allGrouping to every downstream replica).</li>
 *   <li>Emit {@code N_b} data tuples on the default stream (fields-grouped by
 *       {@code routingKey}). Tuples are sampled from the same Zipf-Mandelbrot
 *       user/key distributions as the existing throughput benchmark, so the
 *       DP workload is realistic.</li>
 *   <li>Emit {@code END(b, N_b)} on the control stream.</li>
 *   <li>Block until the aggregator publishes completion for batch {@code b}
 *       via {@link BatchCompletionCoordinator}, then advance.</li>
 * </ol>
 */
public class MicroBatchBaselineSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(MicroBatchBaselineSpout.class);

    private SpoutOutputCollector collector;
    private int numUsers;
    private int numKeys;

    private ZipfMandelbrotDistribution keyDistribution;
    private ZipfMandelbrotDistribution userContributionDistribution;
    private long[] userRemainingContributions;
    private Random rng;

    private transient BatchCompletionCoordinator completion;
    private long completionTimeoutMs;

    private List<BatchPlan> plan;
    private int currentBatchIdx = 0;
    private long emittedInCurrentBatch = 0L;
    private boolean currentBatchBeginSent = false;
    private boolean currentBatchEndSent = false;
    private boolean exhausted = false;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.numUsers = ((Number) conf.getOrDefault("synthetic.num.users", 10_000_000)).intValue();
        this.numKeys = ((Number) conf.getOrDefault("synthetic.num.keys", 1_000_000)).intValue();
        long randomSeed = ((Number) conf.getOrDefault("synthetic.seed", 42L)).longValue();

        this.rng = new Random(randomSeed);
        this.userContributionDistribution = new ZipfMandelbrotDistribution(100_000, 26, 6.738, rng);
        this.keyDistribution = new ZipfMandelbrotDistribution(numKeys, 1000, 1.4, rng);

        this.userRemainingContributions = new long[numUsers];
        for (int i = 0; i < numUsers; i++) {
            long target = userContributionDistribution.sample();
            userRemainingContributions[i] = Math.max(1L, Math.min(target, DPConfig.MAX_CONTRIBUTIONS_PER_USER));
        }

        double[] sizesGb = MicroBatchConfig.sizesGb(conf);
        int runsPerSize = MicroBatchConfig.runsPerSize(conf);
        long bytesPerTuple = MicroBatchConfig.bytesPerTuple(conf);
        this.completionTimeoutMs = MicroBatchConfig.completionTimeoutMs(conf);
        this.plan = MicroBatchConfig.buildPlan(sizesGb, runsPerSize, bytesPerTuple);

        LOG.info("[MicroBatchSpout] plan: {} batches across sizes={}, runsPerSize={}, bytesPerTuple={}",
                plan.size(), java.util.Arrays.toString(sizesGb), runsPerSize, bytesPerTuple);
        for (BatchPlan b : plan) {
            LOG.info("[MicroBatchSpout]   batch {} -> size={} GB, records={}, run={}",
                    b.batchId(), b.sizeGb(), b.recordCount(), b.runIndex());
        }

        this.completion = new BatchCompletionCoordinator(conf, context.getStormId());
    }

    @Override
    public void close() {
        if (completion != null) completion.close();
    }

    @Override
    public void nextTuple() {
        if (exhausted) return;
        if (currentBatchIdx >= plan.size()) {
            LOG.info("[MicroBatchSpout] All {} batches emitted -- spout becoming idle.", plan.size());
            exhausted = true;
            return;
        }

        BatchPlan batch = plan.get(currentBatchIdx);

        if (!currentBatchBeginSent) {
            long tNanos = System.nanoTime();
            long tEpochMs = System.currentTimeMillis();
            collector.emit(ComponentConstants.CONTROL_STREAM,
                    new Values(BatchMarker.BEGIN, batch.batchId(), batch.sizeGb(),
                            batch.recordCount(), recoverBytesPerTuple(batch),
                            tNanos, tEpochMs));
            currentBatchBeginSent = true;
            emittedInCurrentBatch = 0L;
            LOG.info("[MicroBatchSpout] BEGIN batch {} (size={} GB, target={} records)",
                    batch.batchId(), batch.sizeGb(), batch.recordCount());
            return;
        }

        if (emittedInCurrentBatch < batch.recordCount()) {
            emitOneDataTuple();
            emittedInCurrentBatch++;
            if (emittedInCurrentBatch % 1_000_000 == 0) {
                LOG.info("[MicroBatchSpout] batch {} progress: {}/{}",
                        batch.batchId(), emittedInCurrentBatch, batch.recordCount());
            }
            return;
        }

        if (!currentBatchEndSent) {
            long tNanos = System.nanoTime();
            long tEpochMs = System.currentTimeMillis();
            collector.emit(ComponentConstants.CONTROL_STREAM,
                    new Values(BatchMarker.END, batch.batchId(), batch.sizeGb(),
                            batch.recordCount(), recoverBytesPerTuple(batch),
                            tNanos, tEpochMs));
            currentBatchEndSent = true;
            LOG.info("[MicroBatchSpout] END batch {} ({} records emitted) -- awaiting aggregator completion",
                    batch.batchId(), emittedInCurrentBatch);
            return;
        }

        try {
            completion.awaitCompletion(batch.batchId(), completionTimeoutMs);
            LOG.info("[MicroBatchSpout] aggregator confirmed batch {} complete", batch.batchId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("[MicroBatchSpout] interrupted waiting for batch {} completion", batch.batchId(), e);
            exhausted = true;
            return;
        }

        currentBatchIdx++;
        currentBatchBeginSent = false;
        currentBatchEndSent = false;
        emittedInCurrentBatch = 0L;
    }

    private long recoverBytesPerTuple(BatchPlan b) {
        double totalBytes = b.sizeGb() * 1024.0 * 1024.0 * 1024.0;
        return Math.max(1L, (long) Math.round(totalBytes / (double) b.recordCount()));
    }

    private void emitOneDataTuple() {
        int userId = pickUserWithRemainingBudget();
        final String key = Integer.toString(keyDistribution.sample());
        final String userIdStr = Integer.toString(userId);
        collector.emit(new Values(key, 1.0, userIdStr, userIdStr));
        userRemainingContributions[userId]--;
    }

    private int pickUserWithRemainingBudget() {
        for (int attempt = 0; attempt < 64; attempt++) {
            int u = rng.nextInt(numUsers);
            if (userRemainingContributions[u] > 0) return u;
        }
        for (int i = 0; i < numUsers; i++) {
            userRemainingContributions[i] = Math.max(1L,
                    Math.min(userContributionDistribution.sample(), DPConfig.MAX_CONTRIBUTIONS_PER_USER));
        }
        return rng.nextInt(numUsers);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count", "userId", "routingKey"));
        declarer.declareStream(ComponentConstants.CONTROL_STREAM, BatchMarker.fields());
    }
}
