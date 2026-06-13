package ch.usi.inf.examples.synthetic_baseline.spouts;

import ch.usi.inf.examples.synthetic_baseline.config.DPConfig;
import ch.usi.inf.examples.synthetic_baseline.profiling.BaselineBoltLifecycleEvent;
import ch.usi.inf.examples.synthetic_baseline.profiling.BoltProfiler;
import ch.usi.inf.examples.synthetic_baseline.profiling.ProfilerConfig;
import ch.usi.inf.examples.synthetic_baseline.util.GroundTruthCollector;
import ch.usi.inf.examples.synthetic_baseline.util.ZipfMandelbrotDistribution;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Baseline spout that generates synthetic data and emits plaintext tuples.
 * <p>
 * Matches Section 5.1 specifications:
 * - 10 million unique users
 * - User contribution distribution: Zipf-Mandelbrot(N=10^5, q=26, s=6.738)
 * - 1 million distinct keys, Zipf-Mandelbrot(N=10^6, q=1000, s=1.4)
 */
public class BaselineSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(
        BaselineSpout.class
    );

    private SpoutOutputCollector collector;
    private int numUsers;
    private int numKeys;
    private boolean groundTruthEnabled;

    private transient BoltProfiler profiler;

    private ZipfMandelbrotDistribution keyDistribution;
    private ZipfMandelbrotDistribution userContributionDistribution;
    private long[] userRemainingContributions;
    private Random rng;
    private final AtomicLong totalRecordsEmitted = new AtomicLong(0);

    @Override
    public void open(
        Map<String, Object> conf,
        TopologyContext context,
        SpoutOutputCollector collector
    ) {
        this.collector = collector;
        this.numUsers = (
            (Number) conf.getOrDefault("synthetic.num.users", 10_000_000)
        ).intValue();
        this.numKeys = (
            (Number) conf.getOrDefault("synthetic.num.keys", 1_000_000)
        ).intValue();
        long randomSeed = (
            (Number) conf.getOrDefault("synthetic.seed", 42L)
        ).longValue();
        this.groundTruthEnabled = Boolean.parseBoolean(
            String.valueOf(
                conf.getOrDefault("synthetic.ground-truth.enabled", "false")
            )
        );

        LOG.info(
            "BaselineSpout initialized with: numUsers={}, numKeys={}, seed={}, groundTruth={}",
            numUsers,
            numKeys,
            randomSeed,
            groundTruthEnabled
        );

        this.rng = new Random(randomSeed);

        // User contribution distribution: Zipf-Mandelbrot with N=10^5, q=26, s=6.738
        this.userContributionDistribution = new ZipfMandelbrotDistribution(
            100_000,
            26,
            6.738,
            rng
        );

        // Key distribution: Zipf-Mandelbrot with N=10^6, q=1000, s=1.4
        this.keyDistribution = new ZipfMandelbrotDistribution(
            numKeys,
            1000,
            1.4,
            rng
        );

        // Initialize target contributions for each user
        this.userRemainingContributions = new long[numUsers];
        for (int i = 0; i < numUsers; i++) {
            long target = userContributionDistribution.sample();

            // Verify contribution is within limits
            assert target > 0
                : "User contribution distribution should only produce positive values";
            assert target <= DPConfig.MAX_CONTRIBUTIONS_PER_USER
                : "User contribution distribution should be calibrated to produce values within the max contributions per user";

            userRemainingContributions[i] = target;
        }

        if (ProfilerConfig.ENABLED) {
            this.profiler = new BoltProfiler(
                context.getThisComponentId(),
                context.getThisTaskId()
            );
            profiler.recordLifecycleEvent(
                BaselineBoltLifecycleEvent.COMPONENT_STARTED
            );
        }
    }

    @Override
    public void close() {
        if (ProfilerConfig.ENABLED && profiler != null) {
            profiler.recordLifecycleEvent(
                BaselineBoltLifecycleEvent.COMPONENT_STOPPING
            );
            profiler.writeReport();
        }
    }

    @Override
    public void nextTuple() {
        // Fetch the next user to emit for
        final int userId = rng.nextInt(numUsers);

        // Skip if user has reached their target contribution count
        if (userRemainingContributions[userId] <= 0) return;

        // Sample key for the record
        final String key = Integer.toString(keyDistribution.sample());

        // Emit plaintext: (key, count, userId, routingKey)
        // routingKey is set to userId string for consistent fields-grouping.
        final String userIdStr = Integer.toString(userId);
        collector.emit(new Values(key, 1.0, userIdStr, userIdStr));

        // Record ground truth for utility measurement
        if (groundTruthEnabled) {
            GroundTruthCollector.record(key, 1L);
        }

        // Decrement remaining contributions for the user
        userRemainingContributions[userId]--;

        long emitted = totalRecordsEmitted.incrementAndGet();
        if (emitted % 100_000 == 0) {
            LOG.info("BaselineSpout: {} records emitted", emitted);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count", "userId", "routingKey"));
    }
}
