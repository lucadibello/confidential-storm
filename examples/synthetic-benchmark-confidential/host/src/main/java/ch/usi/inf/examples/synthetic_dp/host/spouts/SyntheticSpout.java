package ch.usi.inf.examples.synthetic_dp.host.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.spouts.ConfidentialSpout;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticDataService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticEncryptedRecord;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import ch.usi.inf.examples.synthetic_dp.host.util.ZipfMandelbrotDistribution;
import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synthetic data spout matching Section 5.1 specifications:
 * - 10 million unique users
 * - 1 million distinct keys, where each key is sampled from Zipf-Mandelbrot(N=1_000_000,q=1000,s=1.4) distribution
 * <p>
 * When ground truth collection is enabled ({@code synthetic.ground-truth.enabled=true}),
 * the spout emits a plaintext {@code (key)} tuple on {@link ComponentConstants#GROUND_TRUTH_STREAM}
 * for every record, so the aggregation bolt can build a worker-local ground-truth histogram.
 * When disabled, nothing is emitted on that stream (no allocation, no extra Storm traffic).
 */
public class SyntheticSpout extends ConfidentialSpout<SyntheticDataService> {

    private static final Logger LOG = LoggerFactory.getLogger(
        SyntheticSpout.class
    );

    private int numUsers;
    private int numKeys;
    private long randomSeed;
    private boolean groundTruthEnabled;

    /** Zipf-Mandelbrot distribution for key selection. */
    private ZipfMandelbrotDistribution keyDistribution;

    /** Zipf-Mandelbrot distribution of per-user record contributions. */
    private ZipfMandelbrotDistribution userContributionDistribution;

    /** Remaining contribution budget for each user. */
    private long[] userRemainingContributions;

    /** RNG for sampling. */
    private Random rng;


    public SyntheticSpout() {
        super(SyntheticDataService.class);
    }

    @Override
    protected void afterOpen(
        Map<String, Object> conf,
        TopologyContext context,
        SpoutOutputCollector collector
    ) {
        this.numUsers = (
            (Number) conf.getOrDefault("synthetic.num.users", 10_000_000)
        ).intValue();
        this.numKeys = (
            (Number) conf.getOrDefault("synthetic.num.keys", 1_000_000)
        ).intValue();
        this.randomSeed = (
            (Number) conf.getOrDefault("synthetic.seed", 42L)
        ).longValue();
        this.groundTruthEnabled = Boolean.parseBoolean(
            String.valueOf(
                conf.getOrDefault("synthetic.ground-truth.enabled", "false")
            )
        );

        LOG.info(
            "SyntheticSpout initialized with: numUsers={}, numKeys={}, seed={}, groundTruth={}",
            numUsers,
            numKeys,
            randomSeed,
            groundTruthEnabled
        );

        this.rng = new Random(randomSeed);

        // Zipf-Mandelbrot distribution for user contributions (N=10^5, q=26, s=6.738)
        this.userContributionDistribution = new ZipfMandelbrotDistribution(
            100_000,
            26,
            6.738,
            rng
        );

        // Zipf-Mandelbrot distribution for keys (N=10^6, q=1000, s=1.4)
        this.keyDistribution = new ZipfMandelbrotDistribution(
            numKeys,
            1000,
            1.4,
            rng
        );

        // Initialize user contribution budgets
        this.userRemainingContributions = new long[numUsers];
        for (int i = 0; i < numUsers; i++) {
            // Sample target contribution count for the user
            long target = userContributionDistribution.sample();

            // Validate constraints
            assert target >
            0 : "User contribution distribution should only produce positive values";
            assert target <=
            DPConfig.MAX_CONTRIBUTIONS_PER_USER : "User contribution distribution should be calibrated to produce values within the max contributions per user";

            userRemainingContributions[i] = target;
        }

    }

    @Override
    protected void executeNextTuple() throws EnclaveServiceException {
        // Select next user at random
        final int userId = rng.nextInt(numUsers);

        // Skip if user contribution budget is exhausted
        if (userRemainingContributions[userId] <= 0) return;

        // Generate a new key for this user
        final String key = Integer.toString(keyDistribution.sample());

        // Encrypt record via enclave service
        // TODO: Change 'count' parameter in service API to long
        SyntheticEncryptedRecord rec = getService().encryptRecord(
            key,
            "1",
            Integer.toString(userId)
        );
        // Terminate topology on encryption failure
        if (rec == null) {
            throw new EnclaveServiceException(
                "Failed to encrypt record for user " + userId
            );
        }

        // Emit encrypted record to contribution-bounding bolt
        getCollector().emit(
            new Values(rec.key(), rec.count(), rec.userId(), rec.routingKey())
        );

        // Emit plaintext key for ground-truth tracking
        if (groundTruthEnabled) {
            getCollector().emit(
                ComponentConstants.GROUND_TRUTH_STREAM,
                new Values(key)
            );
        }

        // Decrement remaining user contribution budget
        userRemainingContributions[userId]--;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count", "userId", "routingKey"));

        // Plaintext key stream for ground-truth evaluation
        declarer.declareStream(
            ComponentConstants.GROUND_TRUTH_STREAM,
            new Fields("key")
        );
    }
}
