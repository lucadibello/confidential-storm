package ch.usi.inf.examples.synthetic_baseline.spouts;

import ch.usi.inf.examples.synthetic_baseline.config.DPConfig;
import ch.usi.inf.examples.synthetic_baseline.profiling.BaselineBoltLifecycleEvent;
import ch.usi.inf.examples.synthetic_baseline.profiling.BoltProfiler;
import ch.usi.inf.examples.synthetic_baseline.profiling.ProfilerConfig;
import ch.usi.inf.examples.synthetic_baseline.util.GroundTruthCollector;
import ch.usi.inf.examples.synthetic_baseline.util.ZipfMandelbrotDistribution;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
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
 * Baseline spout that generates the same synthetic data as the enclave version
 * but emits plaintext tuples (no encryption, no ECALL).
 * <p>
 * Matches Section 5.1 specifications:
 * - 10 million unique users
 * - User contribution distribution: Zipf-Mandelbrot(N=10^5, q=26, s=6.738), ~15% users contribute >10 records
 * - 1 million distinct keys, Zipf-Mandelbrot(N=10^6, q=1000, s=1.4), ~1/3 records in first 10^3 keys
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

    /**
     * Per-user contribution counts for ground truth tracking.
     */
    private Map<Long, Long> userContributionCounts;

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
        this.keyDistribution = new ZipfMandelbrotDistribution(
            numKeys,
            1000,
            1.4,
            rng
        );

        if (groundTruthEnabled) {
            this.userContributionCounts = new ConcurrentHashMap<>();
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
        long userId = rng.nextInt(numUsers);
        int keyId = keyDistribution.sample();
        String key = "k" + keyId;

        // Track ground truth only for tuples that would survive contribution bounding
        if (groundTruthEnabled) {
            long count = userContributionCounts.merge(userId, 1L, Long::sum);
            if (count <= DPConfig.MAX_CONTRIBUTIONS_PER_USER) {
                GroundTruthCollector.record(key, 1L);
            }
        }

        // Emit plaintext: (key, count, userId, routingKey)
        // routingKey = userId string for consistent fields-grouping
        String userIdStr = Long.toString(userId);
        collector.emit(new Values(key, 1.0, userIdStr, userIdStr));

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
