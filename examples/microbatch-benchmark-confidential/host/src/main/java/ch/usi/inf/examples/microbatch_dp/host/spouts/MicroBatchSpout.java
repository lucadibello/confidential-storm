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
    private boolean currentBatchBeginSent = false;
    private boolean currentBatchEndSent = false;
    private boolean exhausted = false;

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
            emitOneEncryptedTuple();
            emittedInCurrentBatch++;
            if (emittedInCurrentBatch % 100_000 == 0) {
                LOG.info("[MicroBatchSpout] batch {} progress: {}/{}",
                        batch.batchId(), emittedInCurrentBatch, batch.recordCount());
            }
            return;
        }

        if (!currentBatchEndSent) {
            long tNanos = System.nanoTime();
            long tEpochMs = System.currentTimeMillis();
            getCollector().emit(ComponentConstants.CONTROL_STREAM,
                    new Values(BatchMarker.END, batch.batchId(), batch.sizeGb(),
                            batch.recordCount(), recoverBytesPerTuple(batch),
                            tNanos, tEpochMs));
            currentBatchEndSent = true;
            LOG.info("[MicroBatchSpout] END batch {} ({} records emitted)", batch.batchId(), emittedInCurrentBatch);
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

    private void emitOneEncryptedTuple() throws EnclaveServiceException {
        int userId = pickUserWithRemainingBudget();
        final String key = Integer.toString(keyDistribution.sample());
        MicroBatchEncryptedRecord rec = getService().encryptRecord(key, "1", Integer.toString(userId));
        if (rec == null) {
            throw new EnclaveServiceException("Failed to encrypt record for user " + userId);
        }
        getCollector().emit(new Values(rec.key(), rec.count(), rec.userId(), rec.routingKey()));
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
        declarer.declareStream(ComponentConstants.GROUND_TRUTH_STREAM, new Fields("key"));
        declarer.declareStream(ComponentConstants.CONTROL_STREAM, BatchMarker.fields());
    }
}
