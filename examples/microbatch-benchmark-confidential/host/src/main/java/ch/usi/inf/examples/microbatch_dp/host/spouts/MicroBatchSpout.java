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
 */
public class MicroBatchSpout extends ConfidentialSpout<MicroBatchDataService> {

    private static final Logger LOG = LoggerFactory.getLogger(MicroBatchSpout.class);

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

    // Pre-generated workload for the active batch. RNG / Zipf sampling runs
    // before BEGIN so the BEGIN->END window measures pure pipeline cost
    // (per-tuple encryption + emit + downstream), not dataset generation.
    private int[] preKeyIds;
    private int[] preUserIds;

    public MicroBatchSpout() {
        super(MicroBatchDataService.class);
    }

    @Override
    protected void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
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
            getCollector().emit(ComponentConstants.CONTROL_STREAM,
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
            emitPregeneratedEncryptedTuple((int) emittedInCurrentBatch);
            emittedInCurrentBatch++;
            if (emittedInCurrentBatch % 100_000 == 0) {
                LOG.info("[MicroBatchSpout] batch {} progress: {}/{}",
                        batch.batchId(), emittedInCurrentBatch, batch.recordCount());
            }
            return;
        }

        if (!currentBatchEndSent) {
            // Drop pregen buffers BEFORE the await-completion wait: by now the
            // int arrays have been fully drained into encrypted Values, so they
            // hold no live references. Releasing them here means the long
            // await-completion period carries zero pregen footprint, and only
            // ONE batch's pregen ever sits in memory at a time.
            preKeyIds = null;
            preUserIds = null;

            long tNanos = System.nanoTime();
            long tEpochMs = System.currentTimeMillis();
            getCollector().emit(ComponentConstants.CONTROL_STREAM,
                    new Values(BatchMarker.END, batch.batchId(), batch.sizeGb(),
                            batch.recordCount(), recoverBytesPerTuple(batch),
                            tNanos, tEpochMs));
            currentBatchEndSent = true;
            LOG.info("[MicroBatchSpout] END batch {} ({} records emitted, pregen buffers released)",
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
        // we allocate the (often larger) next pair. Without this hint, the new
        // allocation can race ahead of background GC and trigger an OOM even
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

    private long recoverBytesPerTuple(BatchPlan b) {
        double totalBytes = b.sizeGb() * 1024.0 * 1024.0 * 1024.0;
        return Math.max(1L, (long) Math.round(totalBytes / (double) b.recordCount()));
    }

    private void emitPregeneratedEncryptedTuple(int idx) throws EnclaveServiceException {
        final int userId = preUserIds[idx];
        final String key = Integer.toString(preKeyIds[idx]);
        final String userIdStr = Integer.toString(userId);
        MicroBatchEncryptedRecord rec = getService().encryptRecord(key, "1", userIdStr);
        if (rec == null) {
            throw new EnclaveServiceException("Failed to encrypt record for user " + userId);
        }
        getCollector().emit(new Values(rec.key(), rec.count(), rec.userId(), rec.routingKey()));
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
        declarer.declareStream(ComponentConstants.GROUND_TRUTH_STREAM, new Fields("key"));
        declarer.declareStream(ComponentConstants.CONTROL_STREAM, BatchMarker.fields());
    }
}
