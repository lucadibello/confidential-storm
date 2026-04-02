package ch.usi.inf.examples.synthetic_dp.host.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticDataService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticEncryptedRecord;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.examples.synthetic_dp.host.GroundTruthCollector;
import ch.usi.inf.examples.synthetic_dp.host.util.ZipfMandelbrotDistribution;
import ch.usi.inf.confidentialstorm.host.spouts.ConfidentialSpout;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.spout.SpoutOutputCollector;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Synthetic data spout matching Section 5.1 specifications:
 * - 10 million unique users
 * - 1 million distinct keys, where each key is sampled from Zipf-Mandelbrot(N=1_000_000,q=1000,s=1.4) distribution
 * <p>
 * When ground truth collection is enabled ({@code synthetic.ground-truth.enabled=true}),
 * the spout mirrors the enclave-side per-user contribution limit (C={@link DPConfig#MAX_CONTRIBUTIONS_PER_USER})
 * to track only tuples that would survive bounding. This is needed for utility measurement
 * (comparing noisy DP output against the true histogram) but adds overhead from per-user bookkeeping.
 */
public class SyntheticSpout extends ConfidentialSpout<SyntheticDataService> {
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticSpout.class);

    private int numUsers;
    private int numKeys;
    private long randomSeed;
    private boolean groundTruthEnabled;

    /**
     * The Zipf-Mandelbrot distribution for key selection.
     */
    private ZipfMandelbrotDistribution keyDistribution;

    /**
     * Random number generator for sampling.
     */
    private Random rng;

    private final AtomicLong totalRecordsEmitted = new AtomicLong(0);

    /**
     * Per-user contribution counts, only allocated when ground truth is enabled.
     * Mirrors the enclave-side {@code UserContributionLimiter} to predict which
     * tuples will survive the bounding bolt.
     */
    private Map<Long, Long> userContributionCounts;

    public SyntheticSpout() {
        super(SyntheticDataService.class);
    }

    @Override
    protected void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.numUsers = ((Number) conf.getOrDefault("synthetic.num.users", 10_000_000)).intValue();
        this.numKeys = ((Number) conf.getOrDefault("synthetic.num.keys", 1_000_000)).intValue();
        this.randomSeed = ((Number) conf.getOrDefault("synthetic.seed", 42L)).longValue();
        this.groundTruthEnabled = Boolean.parseBoolean(
                String.valueOf(conf.getOrDefault("synthetic.ground-truth.enabled", "false")));

        LOG.info("SyntheticSpout initialized with: numUsers={}, numKeys={}, seed={}, groundTruth={}",
                numUsers, numKeys, randomSeed, groundTruthEnabled);

        this.rng = new Random(randomSeed);

        // Key distribution: zipf-mandelbrot with N=10^6, q=1000, s=1.4 (Section 5.1)
        this.keyDistribution = new ZipfMandelbrotDistribution(numKeys, 1000, 1.4, rng);

        if (groundTruthEnabled) {
            this.userContributionCounts = new ConcurrentHashMap<>();
        }
    }

    @Override
    protected void executeNextTuple() throws EnclaveServiceException {
        long userId = rng.nextInt(numUsers);
        int keyId = keyDistribution.sample();
        String key = "k" + keyId;

        SyntheticEncryptedRecord rec = getService().encryptRecord(key, "1", Long.toString(userId));
        if (rec == null) {
            return;
        }

        // Track ground truth only for tuples that would survive contribution bounding
        if (groundTruthEnabled) {
            long count = userContributionCounts.merge(userId, 1L, Long::sum);
            if (count <= DPConfig.MAX_CONTRIBUTIONS_PER_USER) {
                GroundTruthCollector.record(key, 1L);
            }
        }

        totalRecordsEmitted.incrementAndGet();
        getCollector().emit(new Values(rec.key(), rec.count(), rec.userId(), rec.routingKey()));

        if (totalRecordsEmitted.get() % 100_000 == 0) {
            LOG.info("SyntheticSpout: {} records emitted", totalRecordsEmitted.get());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count", "userId", "routingKey"));
    }
}
