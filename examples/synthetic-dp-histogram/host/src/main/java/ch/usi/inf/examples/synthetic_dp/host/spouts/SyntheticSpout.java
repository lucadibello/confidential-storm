package ch.usi.inf.examples.synthetic_dp.host.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.spouts.ConfidentialSpout;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticDataService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticEncryptedRecord;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.examples.synthetic_dp.host.GroundTruthCollector;
import ch.usi.inf.examples.synthetic_dp.host.util.ZipfMandelbrotDistribution;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
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
 * the spout mirrors the enclave-side per-user contribution limit (C={@link DPConfig#MAX_CONTRIBUTIONS_PER_USER})
 * to track only tuples that would survive bounding. This is needed for utility measurement
 * (comparing noisy DP output against the true histogram) but adds overhead from per-user bookkeeping.
 */
public class SyntheticSpout extends ConfidentialSpout<SyntheticDataService> {

    private static final Logger LOG = LoggerFactory.getLogger(
        SyntheticSpout.class
    );

    private int numUsers;
    private int numKeys;
    private long randomSeed;
    private boolean groundTruthEnabled;

    /**
     * The Zipf-Mandelbrot distribution for key selection.
     */
    private ZipfMandelbrotDistribution keyDistribution;

    /**
     * The Zipf-Mandelbrot distribution for user contribution counts, used to decide how many records each user contributes.
     */
    private ZipfMandelbrotDistribution userContributionDistribution;

    /**
     * Stores the remaining contributions for each user, initialized from the user contribution distribution.
     */
    private long[] userRemainingContributions;

    /**
     * Random number generator for sampling.
     */
    private Random rng;

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

        // User contribution distribution: zipf-mandelbrot with N=10^5, q=26, s=6.738 (Section 5.1),
        // NOTE: with this settings, ~15% of users contribute more than 10 records.
        this.userContributionDistribution = new ZipfMandelbrotDistribution(
            100_000,
            26,
            6.738,
            rng
        );

        // Key distribution: zipf-mandelbrot with N=10^6, q=1000, s=1.4 (Section 5.1)
        // NOTE: with these settings, roughly 1/3 of records have the first 10^3 keys
        this.keyDistribution = new ZipfMandelbrotDistribution(
            numKeys,
            1000,
            1.4,
            rng
        );

        // Initialize target contributions for each user based on the user contribution distribution
        this.userRemainingContributions = new long[numUsers];
        for (int i = 0; i < numUsers; i++) {
            // Sample the target contribution count for this user, capped at MAX_CONTRIBUTIONS_PER
            long target = userContributionDistribution.sample();

            // NOTE: these assertions are just to ensure that the user contribution distribution is calibrated correctly to produce values in the expected range
            assert target >
            0 : "User contribution distribution should only produce positive values";
            assert target <=
            DPConfig.MAX_CONTRIBUTIONS_PER_USER : "User contribution distribution should be calibrated to produce values within the max contributions per user";

            // Initialize the remaining contributions for this user
            userRemainingContributions[i] = target;
        }

        // enable ground truth if requested to keep track of accurate counts of distributions
        if (groundTruthEnabled) {
            this.userContributionCounts = new ConcurrentHashMap<>();
        }
    }

    @Override
    protected void executeNextTuple() throws EnclaveServiceException {
        // fetch the next user to emit for
        final int userId = rng.nextInt(numUsers);

        // check if user has already emitted their target contribution count, if so skip
        if (userRemainingContributions[userId] <= 0) return;

        // fetch a new record for this user
        final int keyId = keyDistribution.sample();
        final String key = "k" + keyId;

        // encrypt the record using the custom enclave service
        // FIXME: we should convert the "count" parameter from String to long in the service API (the count will always be a number)
        SyntheticEncryptedRecord rec = getService().encryptRecord(
            key,
            "1",
            Integer.toString(userId)
        );
        // if encryption fails, means that the enclave has crashed or is unresponsive - in either case, we should trigger a fatal error to signal that the topology cannot continue processing
        if (rec == null) {
            throw new EnclaveServiceException(
                "Failed to encrypt record for user " + userId
            );
        }

        // Track ground truth only for tuples that would survive contribution bounding
        if (groundTruthEnabled) {
            // FIXME: we need to check if this makes sense!
            long count = userContributionCounts.merge(
                Long.valueOf(userId),
                1L,
                Long::sum
            );
            if (count <= DPConfig.MAX_CONTRIBUTIONS_PER_USER) {
                GroundTruthCollector.record(key, 1L);
            }
        }

        // emit tuple
        getCollector().emit(
            new Values(rec.key(), rec.count(), rec.userId(), rec.routingKey())
        );

        // record that userId has emitted one more record
        userRemainingContributions[userId]--;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count", "userId", "routingKey"));
    }
}
