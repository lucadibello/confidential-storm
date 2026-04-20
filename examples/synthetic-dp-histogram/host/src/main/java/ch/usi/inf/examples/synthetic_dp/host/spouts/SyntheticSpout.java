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

    }

    @Override
    protected void executeNextTuple() throws EnclaveServiceException {
        // fetch the next user to emit for
        final int userId = rng.nextInt(numUsers);

        // check if user has already emitted their target contribution count, if so skip
        if (userRemainingContributions[userId] <= 0) return;

        // fetch a new record for this user
        final String key = Integer.toString(keyDistribution.sample());

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

        // emit tuple on the default stream to the contribution-bounding bolt
        getCollector().emit(
            new Values(rec.key(), rec.count(), rec.userId(), rec.routingKey())
        );

        // emit the plaintext key on the ground-truth stream to the aggregation bolt (only if enabled)
        if (groundTruthEnabled) {
            getCollector().emit(
                ComponentConstants.GROUND_TRUTH_STREAM,
                new Values(key)
            );
        }

        // record that userId has emitted one more record
        userRemainingContributions[userId]--;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count", "userId", "routingKey"));

        // Ground truth stream for emitting plaintext keys for building the ground-truth histogram in the aggregation bolt
        // NOTE: when ground truth collection is disabled, no tuples will be emitted on this stream, so there is no overhead in declaring it
        declarer.declareStream(
            ComponentConstants.GROUND_TRUTH_STREAM,
            new Fields("key")
        );
    }
}
