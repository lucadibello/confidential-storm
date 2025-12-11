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
 * - Contribution per user sampled Zipf-Mandelbrot(N=100_000,q=26,s=6.738) distribution
 * - 1 million distinct keys, where each key is sampled from Zipf-Mandelbrot(N=1_000_000,q=1000,s=1.4) distribution
 */
public class SyntheticSpout extends ConfidentialSpout<SyntheticDataService> {
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticSpout.class);

    // Paper specifications (Section 5.1)
    private int numUsers;
    private int numKeys;
    private int batchSize;
    private long sleepMs;
    private long randomSeed;

    /**
     * The Zipf-Mandelbrot distribution for key selection and user record contribution counts.
     */
    private ZipfMandelbrotDistribution keyDistribution;

    /**
     * The Zipf-Mandelbrot distribution for user record contribution counts.
     */
    private ZipfMandelbrotDistribution userRecordDistribution;

    /**
     * Random number generator for sampling.
     */
    private Random rng;

    /**
     * Tracking user contribution counts to enforce contribution bounding.
     */
    private final Map<Long, Integer> userCounts = new ConcurrentHashMap<>();

    /**
     * Tracking target record counts per user based on the natural distribution.
     */
    private final Map<Long, Integer> userTargetRecords = new ConcurrentHashMap<>();

    // Statistics for validation
    private final AtomicLong totalRecordsEmitted = new AtomicLong(0);
    private final AtomicLong totalRecordsDropped = new AtomicLong(0);
    private final Map<String, Long> keyFrequency = new ConcurrentHashMap<>();

    public SyntheticSpout() {
        super(SyntheticDataService.class);
    }

    @Override
    protected void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        // Initialize configuration from topology config (safely handling types)
        this.numUsers = ((Number) conf.getOrDefault("synthetic.num.users", 10_000_000)).intValue();
        this.numKeys = ((Number) conf.getOrDefault("synthetic.num.keys", 1_000_000)).intValue();
        this.batchSize = ((Number) conf.getOrDefault("synthetic.batch.size", 20_000)).intValue();
        this.sleepMs = ((Number) conf.getOrDefault("synthetic.sleep.ms", 100L)).longValue();
        this.randomSeed = ((Number) conf.getOrDefault("synthetic.seed", 42L)).longValue();

        LOG.info("SyntheticSpout initialized with: numUsers={}, numKeys={}, batchSize={}, sleepMs={}, seed={}",
                numUsers, numKeys, batchSize, sleepMs, randomSeed);

        this.rng = new Random(randomSeed);

        // create distributions based on paper specifications (Section 5.1)

        // Key distribution: zipf-mandelbrot with N=10^6, q=1000, s=1.4
        this.keyDistribution = new ZipfMandelbrotDistribution(numKeys, 1000, 1.4, rng);
        // User record contribution count distribution: zipf-mandelbrot with N=10^5, q=26, s=6.738
        this.userRecordDistribution = new ZipfMandelbrotDistribution(100_000, 26, 6.738, rng);
    }

    @Override
    protected void executeNextTuple() throws EnclaveServiceException {
        LOG.debug("Emitting batch of {} records", batchSize);

        for (int i = 0; i < batchSize; i++) {
            // Select random user
            long userId = rng.nextInt(numUsers);

            // Determine target record count for this user (if not already set)
            int targetRecords = userTargetRecords.computeIfAbsent(userId,
                    uid -> userRecordDistribution.sample());

            // Check if user has reached their target (natural distribution limit)
            int currentCount = userCounts.getOrDefault(userId, 0);
            if (currentCount >= targetRecords) {
                // User has emitted all their records according to distribution
                totalRecordsDropped.incrementAndGet();
                continue;
            }

            // Check contribution bounding (C = 32 per paper)
            if (currentCount >= DPConfig.MAX_CONTRIBUTIONS_PER_USER) {
                totalRecordsDropped.incrementAndGet();
                continue;
            }

            // Sample key from Zipf distribution
            int keyId = keyDistribution.sample();
            String key = "k" + keyId;

            // Track key frequency for validation
            keyFrequency.merge(key, 1L, Long::sum);

            // Increment user contribution count
            userCounts.merge(userId, 1, Integer::sum);

            // Record ground truth for utility evaluation
            GroundTruthCollector.record(key, 1L);

            // Encrypt record using enclave service
            LOG.trace("Encrypting record for user {}: key={}", userId, key);
            SyntheticEncryptedRecord rec = getService().encryptRecord(key, "1", Long.toString(userId));
            if (rec == null) {
                totalRecordsDropped.incrementAndGet();
                continue;
            }

            // Emit encrypted record to histogram bolt
            totalRecordsEmitted.incrementAndGet();
            getCollector().emit(new Values(rec.key(), rec.count(), rec.userId()));
        }

        // Log statistics periodically (every 100 batches)
        if (totalRecordsEmitted.get() % (batchSize * 100L) == 0) {
            logStatistics();
        }

        // Small delay between batches to avoid overwhelming the system
        if (sleepMs > 0) {
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Logs validation statistics to verify distribution properties.
     */
    private void logStatistics() {
        long emitted = totalRecordsEmitted.get();
        long dropped = totalRecordsDropped.get();
        int uniqueUsers = userCounts.size();

        LOG.info("=== Spout Statistics ===");
        LOG.info("Records emitted: {}", emitted);
        LOG.info("Records dropped: {}", dropped);
        LOG.info("Unique users: {}", uniqueUsers);
        LOG.info("Unique keys: {}", keyFrequency.size());
        LOG.info("========================");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count", "user"));
    }
}
