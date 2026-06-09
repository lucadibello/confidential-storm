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
    private boolean currentBatchPregenerated = false;
    private boolean currentBatchBeginSent = false;
    private boolean currentBatchEndSent = false;
    private boolean exhausted = false;

    // Pre-generated workload for the active batch. Populated before BEGIN is
    // emitted so the BEGIN->END window measures only the topology pipeline
    // cost, not the per-tuple RNG / Zipf sampling cost.
    private int[] preKeyIds;
    private int[] preUserIds;

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

        if (!currentBatchPregenerated) {
            pregenerateBatch(batch);
            currentBatchPregenerated = true;
            return;
        }

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
            emitPregeneratedTuple((int) emittedInCurrentBatch);
            emittedInCurrentBatch++;
            if (emittedInCurrentBatch % 1_000_000 == 0) {
                LOG.info("[MicroBatchSpout] batch {} progress: {}/{}",
                        batch.batchId(), emittedInCurrentBatch, batch.recordCount());
            }
            return;
        }

        if (!currentBatchEndSent) {
            // Pregen buffers have been fully drained into Storm's outbound queue
            // (the Values objects we emitted reference fresh Strings, not the
            // int arrays). Drop the references now so GC can reclaim them
            // BEFORE we go into the await-completion wait, which can be long.
            // This guarantees only ONE batch's pregen footprint is resident at
            // any moment: prior batch is gone, next batch hasn't started.
            preKeyIds = null;
            preUserIds = null;

            long tNanos = System.nanoTime();
            long tEpochMs = System.currentTimeMillis();
            collector.emit(ComponentConstants.CONTROL_STREAM,
                    new Values(BatchMarker.END, batch.batchId(), batch.sizeGb(),
                            batch.recordCount(), recoverBytesPerTuple(batch),
                            tNanos, tEpochMs));
            currentBatchEndSent = true;
            LOG.info("[MicroBatchSpout] END batch {} ({} records emitted, pregen buffers released) -- awaiting aggregator completion",
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
        currentBatchPregenerated = false;
        currentBatchBeginSent = false;
        currentBatchEndSent = false;
        emittedInCurrentBatch = 0L;
    }

    private void pregenerateBatch(BatchPlan batch) {
        long recordCount = batch.recordCount();
        if (recordCount > Integer.MAX_VALUE) {
            throw new IllegalStateException("Batch " + batch.batchId() + " has "
                    + recordCount + " records; pre-generation buffer caps at "
                    + Integer.MAX_VALUE + ". Split the batch or use long-indexed arrays.");
        }
        int n = (int) recordCount;
        // Encourage the JVM to reclaim the previous batch's int[] arrays before
        // we allocate the (often larger) next pair. Without this, the next
        // allocation may race ahead of background GC and trigger an OOM even
        // though only the new arrays should be resident.
        if (currentBatchIdx > 0) {
            System.gc();
        }
        long t0 = System.nanoTime();
        preKeyIds = new int[n];
        preUserIds = new int[n];
        for (int i = 0; i < n; i++) {
            preKeyIds[i] = keyDistribution.sample();
            preUserIds[i] = pickUserWithRemainingBudget();
            userRemainingContributions[preUserIds[i]]--;
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        LOG.info("[MicroBatchSpout] Pre-generated batch {} ({} records, size={} GB) in {} ms -- outside BEGIN/END window",
                batch.batchId(), n, batch.sizeGb(), elapsedMs);
    }

    private void emitPregeneratedTuple(int idx) {
        final String key = Integer.toString(preKeyIds[idx]);
        final String userIdStr = Integer.toString(preUserIds[idx]);
        collector.emit(new Values(key, 1.0, userIdStr, userIdStr));
    }

    private long recoverBytesPerTuple(BatchPlan b) {
        double totalBytes = b.sizeGb() * 1024.0 * 1024.0 * 1024.0;
        return Math.max(1L, (long) Math.round(totalBytes / (double) b.recordCount()));
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
