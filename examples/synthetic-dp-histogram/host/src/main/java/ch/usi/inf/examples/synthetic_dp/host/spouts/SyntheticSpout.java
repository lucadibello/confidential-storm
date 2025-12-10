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

    /**
     * Number of unique users. Default: 10 million (paper specification).
     */
    private static final int NUM_USERS = Integer.getInteger("synthetic.num.users", 10_000_000);

    /**
     * Number of distinct keys. Default: 1 million (paper specification).
     */
    private static final int NUM_KEYS = Integer.getInteger("synthetic.num.keys", 1_000_000);

    /**
     * The number of records to emit in each batch. Default: 20000.
     */
    private static final int BATCH_SIZE = Integer.getInteger("synthetic.batch.size", 20_000);

    /**
     * Sleep time (in milliseconds) between batches to avoid overwhelming the system. Default: 100 ms.
     */
    private static final long SLEEP_MS = Long.getLong("synthetic.sleep.ms", 100L);

    /**
     * Random seed for reproducibility. Default: 42.
     */
    private static final long RANDOM_SEED = Long.getLong("synthetic.seed", 42L);

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
        this.rng = new Random(RANDOM_SEED);

        // create distributions based on paper specifications (Section 5.1)

        // Key distribution: zipf-mandelbrot with N=10^6, q=1000, s=1.4
        this.keyDistribution = new ZipfMandelbrotDistribution(NUM_KEYS, 1000, 1.4, rng);
        // User record contribution count distribution: zipf-mandelbrot with N=10^5, q=26, s=6.738
        this.userRecordDistribution = new ZipfMandelbrotDistribution(100_000, 26, 6.738, rng);
    }

    @Override
    protected void executeNextTuple() throws EnclaveServiceException {
        LOG.debug("Emitting batch of {} records", BATCH_SIZE);

        for (int i = 0; i < BATCH_SIZE; i++) {
            // Select random user
            long userId = rng.nextInt(NUM_USERS);

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
        if (totalRecordsEmitted.get() % (BATCH_SIZE * 100L) == 0) {
            logStatistics();
        }

        // Small delay between batches to avoid overwhelming the system
        if (SLEEP_MS > 0) {
            try {
                Thread.sleep(SLEEP_MS);
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
