package ch.usi.inf.examples.microbatch_dp.host.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.spouts.ConfidentialSpout;
import ch.usi.inf.examples.microbatch_dp.common.api.MicroBatchDataService;
import ch.usi.inf.examples.microbatch_dp.common.api.model.MicroBatchEncryptedRecord;
import ch.usi.inf.examples.microbatch_dp.common.config.DPConfig;
import ch.usi.inf.examples.microbatch_dp.host.config.MicroBatchConfig;
import ch.usi.inf.examples.microbatch_dp.host.config.MicroBatchConfig.BatchPlan;
import ch.usi.inf.examples.microbatch_dp.host.topology.BatchCompletionCoordinator;
import ch.usi.inf.examples.microbatch_dp.host.topology.BatchMarker;
import ch.usi.inf.examples.microbatch_dp.common.topology.ComponentConstants;
import ch.usi.inf.examples.microbatch_dp.host.util.ZipfMandelbrotDistribution;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Micro-batch confidential spout: same BEGIN/END control protocol as the
 * baseline variant, but each data tuple goes through the SGX-resident
 * encryption service before being emitted, so the recorded duration includes
 * the per-tuple ECALL + encryption cost.
 *
 * <p>When the topology runs with {@code numSpouts > 1}, each instance emits
 * only its share of every batch. The leader (task index 0) emits BEGIN/END
 * control markers; every instance signals
 * {@link BatchCompletionCoordinator#signalEmitDone} once it has drained its
 * share, and the leader waits on
 * {@link BatchCompletionCoordinator#awaitAllEmitDone} before emitting END.
 * See {@code MicroBatchBaselineSpout} for the full protocol notes.
 */
public class MicroBatchSpout extends ConfidentialSpout<MicroBatchDataService> {

    private static final Logger LOG = LoggerFactory.getLogger(MicroBatchSpout.class);

    private int numUsers;
    private int numKeys;

    private int taskIndex;
    private int numSpouts;
    private boolean isLeader;
    /** Number of users owned by this instance (stride-partitioned by taskIndex). */
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

    // Pre-generated workload for this instance's share of the active batch.
    // RNG / Zipf sampling runs before BEGIN so the BEGIN->END window measures
    // pure pipeline cost (per-tuple encryption + emit + downstream).
    private int[] preKeyIds;
    private int[] preUserIds;

    // Records encrypted per ECALL. The enclave-transition cost is fixed and
    // dominates the per-record cost when called one-at-a-time. Batching ~1024
    // records per call amortises the transition across the whole chunk.
    private static final int ENCRYPT_CHUNK_SIZE = 1024;

    // Buffer of already-encrypted records, refilled by one ECALL at a time
    // and drained one tuple per nextTuple() call.
    private MicroBatchEncryptedRecord[] encryptedChunk;
    private int chunkOffset;
    private int chunkValid;

    public MicroBatchSpout() {
        super(MicroBatchDataService.class);
    }

    @Override
    protected void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.numUsers = ((Number) conf.getOrDefault("synthetic.num.users", 10_000_000)).intValue();
        this.numKeys = ((Number) conf.getOrDefault("synthetic.num.keys", 1_000_000)).intValue();
        long randomSeed = ((Number) conf.getOrDefault("synthetic.seed", 42L)).longValue();

        this.taskIndex = context.getThisTaskIndex();
        this.numSpouts = context.getComponentTasks(context.getThisComponentId()).size();
        this.isLeader = (taskIndex == 0);

        // Unique seed per instance to avoid identical sequences.
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

        this.completion = new BatchCompletionCoordinator(conf, context.getStormId());
    }

    @Override
    public void close() {
        if (completion != null) completion.close();
        super.close();
    }

    @Override
    protected void executeNextTuple() throws EnclaveServiceException {
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
            getCollector().emit(ComponentConstants.CONTROL_STREAM,
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
            if (encryptedChunk == null || chunkOffset >= chunkValid) {
                refillEncryptedChunk();
            }
            MicroBatchEncryptedRecord rec = encryptedChunk[chunkOffset++];
            getCollector().emit(new Values(rec.key(), rec.count(), rec.userId(), rec.routingKey()));
            emittedInCurrentBatch++;
            if (emittedInCurrentBatch % 100_000 == 0) {
                LOG.info("[MicroBatchSpout {}] batch {} progress: {}/{} (my share)",
                        taskIndex, batch.batchId(), emittedInCurrentBatch, myRecordsThisBatch);
            }
            return;
        }

        if (!currentBatchEmitSignalled) {
            // Free pregeneration and chunk buffers to reclaim memory during barrier and completion wait.
            preKeyIds = null;
            preUserIds = null;
            encryptedChunk = null;
            chunkOffset = 0;
            chunkValid = 0;
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
            getCollector().emit(ComponentConstants.CONTROL_STREAM,
                    new Values(BatchMarker.END, batch.batchId(), batch.sizeGb(),
                            batch.recordCount(), recoverBytesPerTuple(batch),
                            tNanos, tEpochMs));
            currentBatchEndSent = true;
            LOG.info("[MicroBatchSpout leader] END batch {} (all {} spouts done)",
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
        // Suggest GC to reclaim previous batch arrays before allocating the next pair to prevent OOM.
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

    private long recoverBytesPerTuple(BatchPlan b) {
        double totalBytes = b.sizeGb() * 1024.0 * 1024.0 * 1024.0;
        return Math.max(1L, (long) Math.round(totalBytes / (double) b.recordCount()));
    }

    private void refillEncryptedChunk() throws EnclaveServiceException {
        long remaining = myRecordsThisBatch - emittedInCurrentBatch;
        int n = (int) Math.min((long) ENCRYPT_CHUNK_SIZE, remaining);
        int base = (int) emittedInCurrentBatch;
        String[] ks = new String[n];
        String[] cs = new String[n];
        String[] us = new String[n];
        for (int i = 0; i < n; i++) {
            ks[i] = Integer.toString(preKeyIds[base + i]);
            cs[i] = "1";
            us[i] = Integer.toString(preUserIds[base + i]);
        }
        MicroBatchEncryptedRecord[] result = getService().encryptRecords(ks, cs, us);
        if (result == null) {
            throw new EnclaveServiceException(
                    "Batched encryption returned null for " + n + " records (offset " + base + ")");
        }
        encryptedChunk = result;
        chunkOffset = 0;
        chunkValid = n;
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
        declarer.declareStream(ComponentConstants.GROUND_TRUTH_STREAM, new Fields("key"));
        declarer.declareStream(ComponentConstants.CONTROL_STREAM, BatchMarker.fields());
    }
}
