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
 * <p><strong>Parallel-spout protocol.</strong> When the topology runs with
 * {@code numSpouts > 1}, each spout instance pre-generates and emits only its
 * share ({@code recordCount / numSpouts}, plus the remainder spread across the
 * first instances) of every batch. To keep the BEGIN→END measurement window
 * meaningful across instances, control markers are emitted by the
 * <em>leader</em> (task index 0) only:
 * <ol>
 *   <li>Leader emits {@code BEGIN(b, ...)} on the control stream
 *       (allGrouping to every downstream replica).</li>
 *   <li>Every instance emits its share of data tuples on the default stream
 *       (fields-grouped by {@code routingKey}). User IDs are partitioned
 *       stride-wise by {@code userId % numSpouts == taskIndex} so each user's
 *       contribution budget is owned by exactly one spout instance.</li>
 *   <li>After draining its share, every instance signals
 *       {@link BatchCompletionCoordinator#signalEmitDone}.</li>
 *   <li>The leader waits for all instances to have signalled, then emits
 *       {@code END(b, ...)}.</li>
 *   <li>Every instance waits for the aggregator's completion publication
 *       before starting the next batch.</li>
 * </ol>
 * The grace window in the DP bolt absorbs any remaining cross-stream skew
 * between the leader's END marker and late data tuples still in flight from
 * non-leader instances.
 */
public class MicroBatchBaselineSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(MicroBatchBaselineSpout.class);

    private SpoutOutputCollector collector;
    private int numUsers;
    private int numKeys;

    // Spout parallelism / partition.
    private int taskIndex;
    private int numSpouts;
    private boolean isLeader;

    // Number of users owned by this instance (stride-partitioned by taskIndex).
    // Local index i in userRemainingContributions corresponds to global user ID
    // i * numSpouts + taskIndex.
    private int myUserCount;

    private ZipfMandelbrotDistribution keyDistribution;
    private ZipfMandelbrotDistribution userContributionDistribution;
    private long[] userRemainingContributions;
    private Random rng;

    private transient BatchCompletionCoordinator completion;
    private long completionTimeoutMs;

    private List<BatchPlan> plan;
    private int currentBatchIdx = 0;
    /** Number of records this instance must emit for the active batch. */
    private long myRecordsThisBatch = 0L;
    private long emittedInCurrentBatch = 0L;
    private boolean currentBatchPregenerated = false;
    private boolean currentBatchBeginSent = false;
    private boolean currentBatchEmitSignalled = false;
    private boolean currentBatchEndSent = false;
    private boolean exhausted = false;

    // Pre-generated workload for the active batch (this instance's share).
    // Populated before BEGIN is emitted so the BEGIN->END window measures only
    // the topology pipeline cost, not the per-tuple RNG / Zipf sampling cost.
    private int[] preKeyIds;
    private int[] preUserIds;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.numUsers = ((Number) conf.getOrDefault("synthetic.num.users", 10_000_000)).intValue();
        this.numKeys = ((Number) conf.getOrDefault("synthetic.num.keys", 1_000_000)).intValue();
        long randomSeed = ((Number) conf.getOrDefault("synthetic.seed", 42L)).longValue();

        this.taskIndex = context.getThisTaskIndex();
        this.numSpouts = context.getComponentTasks(context.getThisComponentId()).size();
        this.isLeader = (taskIndex == 0);

        // Seed each instance distinctly so we don't generate identical sequences.
        this.rng = new Random(randomSeed + taskIndex);
        this.userContributionDistribution = new ZipfMandelbrotDistribution(100_000, 26, 6.738, rng);
        this.keyDistribution = new ZipfMandelbrotDistribution(numKeys, 1000, 1.4, rng);

        this.myUserCount = sharePerInstance(numUsers, taskIndex, numSpouts);
        this.userRemainingContributions = new long[myUserCount];
        for (int i = 0; i < myUserCount; i++) {
            long target = userContributionDistribution.sample();
            userRemainingContributions[i] = Math.max(1L, Math.min(target, DPConfig.MAX_CONTRIBUTIONS_PER_USER));
        }

        double[] sizesGb = MicroBatchConfig.sizesGb(conf);
        int runsPerSize = MicroBatchConfig.runsPerSize(conf);
        long bytesPerTuple = MicroBatchConfig.bytesPerTuple(conf);
        this.completionTimeoutMs = MicroBatchConfig.completionTimeoutMs(conf);
        this.plan = MicroBatchConfig.buildPlan(sizesGb, runsPerSize, bytesPerTuple);

        LOG.info("[MicroBatchSpout {}/{}] leader={}, plan: {} batches across sizes={}, runsPerSize={}, "
                        + "bytesPerTuple={}, myUserCount={}/{}",
                taskIndex, numSpouts, isLeader, plan.size(), java.util.Arrays.toString(sizesGb),
                runsPerSize, bytesPerTuple, myUserCount, numUsers);
        if (isLeader) {
            for (BatchPlan b : plan) {
                LOG.info("[MicroBatchSpout leader] batch {} -> size={} GB, totalRecords={}, run={}",
                        b.batchId(), b.sizeGb(), b.recordCount(), b.runIndex());
            }
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
            LOG.info("[MicroBatchSpout {}] All {} batches emitted -- spout becoming idle.",
                    taskIndex, plan.size());
            exhausted = true;
            return;
        }

        BatchPlan batch = plan.get(currentBatchIdx);

        if (!currentBatchPregenerated) {
            pregenerateBatch(batch);
            currentBatchPregenerated = true;
            return;
        }

        if (isLeader && !currentBatchBeginSent) {
            long tNanos = System.nanoTime();
            long tEpochMs = System.currentTimeMillis();
            collector.emit(ComponentConstants.CONTROL_STREAM,
                    new Values(BatchMarker.BEGIN, batch.batchId(), batch.sizeGb(),
                            batch.recordCount(), recoverBytesPerTuple(batch),
                            tNanos, tEpochMs));
            currentBatchBeginSent = true;
            emittedInCurrentBatch = 0L;
            LOG.info("[MicroBatchSpout leader] BEGIN batch {} (size={} GB, target={} records total)",
                    batch.batchId(), batch.sizeGb(), batch.recordCount());
            return;
        }

        if (emittedInCurrentBatch < myRecordsThisBatch) {
            emitPregeneratedTuple((int) emittedInCurrentBatch);
            emittedInCurrentBatch++;
            if (emittedInCurrentBatch % 1_000_000 == 0) {
                LOG.info("[MicroBatchSpout {}] batch {} progress: {}/{} (my share)",
                        taskIndex, batch.batchId(), emittedInCurrentBatch, myRecordsThisBatch);
            }
            return;
        }

        if (!currentBatchEmitSignalled) {
            // Drop pregen buffers now: their contents have been emitted (Values
            // hold their own Strings, not int[] refs) so they're dead weight
            // during the upcoming barrier + completion wait.
            preKeyIds = null;
            preUserIds = null;
            completion.signalEmitDone(batch.batchId(), numSpouts);
            currentBatchEmitSignalled = true;
            LOG.info("[MicroBatchSpout {}] signalled emit-done for batch {} ({} records emitted)",
                    taskIndex, batch.batchId(), emittedInCurrentBatch);
            return;
        }

        if (isLeader && !currentBatchEndSent) {
            try {
                completion.awaitAllEmitDone(batch.batchId(), numSpouts, completionTimeoutMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("[MicroBatchSpout leader] interrupted waiting for emit barrier on batch {}",
                        batch.batchId(), e);
                exhausted = true;
                return;
            }
            long tNanos = System.nanoTime();
            long tEpochMs = System.currentTimeMillis();
            collector.emit(ComponentConstants.CONTROL_STREAM,
                    new Values(BatchMarker.END, batch.batchId(), batch.sizeGb(),
                            batch.recordCount(), recoverBytesPerTuple(batch),
                            tNanos, tEpochMs));
            currentBatchEndSent = true;
            LOG.info("[MicroBatchSpout leader] END batch {} (all {} spouts done) -- awaiting aggregator completion",
                    batch.batchId(), numSpouts);
            return;
        }

        try {
            completion.awaitCompletion(batch.batchId(), completionTimeoutMs);
            LOG.info("[MicroBatchSpout {}] aggregator confirmed batch {} complete",
                    taskIndex, batch.batchId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("[MicroBatchSpout {}] interrupted waiting for batch {} completion",
                    taskIndex, batch.batchId(), e);
            exhausted = true;
            return;
        }

        currentBatchIdx++;
        currentBatchPregenerated = false;
        currentBatchBeginSent = false;
        currentBatchEmitSignalled = false;
        currentBatchEndSent = false;
        emittedInCurrentBatch = 0L;
        myRecordsThisBatch = 0L;
    }

    private void pregenerateBatch(BatchPlan batch) {
        long totalRecords = batch.recordCount();
        long myShare = shareForLong(totalRecords, taskIndex, numSpouts);
        if (myShare > Integer.MAX_VALUE) {
            throw new IllegalStateException("Spout " + taskIndex + " share for batch "
                    + batch.batchId() + " is " + myShare
                    + " records; pre-generation buffer caps at " + Integer.MAX_VALUE
                    + ". Increase spout parallelism or split the batch.");
        }
        int n = (int) myShare;
        this.myRecordsThisBatch = myShare;
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
            int localUserIdx = pickUserWithRemainingBudget();
            preUserIds[i] = localToGlobalUser(localUserIdx);
            userRemainingContributions[localUserIdx]--;
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        LOG.info("[MicroBatchSpout {}] Pre-generated batch {} share ({} of {} records, size={} GB) in {} ms",
                taskIndex, batch.batchId(), n, totalRecords, batch.sizeGb(), elapsedMs);
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
            int u = rng.nextInt(myUserCount);
            if (userRemainingContributions[u] > 0) return u;
        }
        for (int i = 0; i < myUserCount; i++) {
            userRemainingContributions[i] = Math.max(1L,
                    Math.min(userContributionDistribution.sample(), DPConfig.MAX_CONTRIBUTIONS_PER_USER));
        }
        return rng.nextInt(myUserCount);
    }

    private int localToGlobalUser(int localIdx) {
        return localIdx * numSpouts + taskIndex;
    }

    private static int sharePerInstance(int total, int index, int parties) {
        int base = total / parties;
        return base + (index < (total % parties) ? 1 : 0);
    }

    private static long shareForLong(long total, int index, int parties) {
        long base = total / parties;
        return base + ((long) index < (total % parties) ? 1L : 0L);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count", "userId", "routingKey"));
        declarer.declareStream(ComponentConstants.CONTROL_STREAM, BatchMarker.fields());
    }
}
