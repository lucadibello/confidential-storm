package ch.usi.inf.examples.synthetic_baseline.spouts;

import ch.usi.inf.examples.synthetic_baseline.config.DPConfig;
import ch.usi.inf.examples.synthetic_baseline.util.GroundTruthCollector;
import ch.usi.inf.examples.synthetic_baseline.util.ZipfMandelbrotDistribution;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Baseline spout that generates the same synthetic data as the enclave version
 * but emits plaintext tuples (no encryption, no ECALL).
 */
public class BaselineSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(BaselineSpout.class);

    private SpoutOutputCollector collector;
    private int numUsers;
    private int numKeys;
    private boolean groundTruthEnabled;

    private ZipfMandelbrotDistribution keyDistribution;
    private Random rng;
    private final AtomicLong totalRecordsEmitted = new AtomicLong(0);

    /**
     * Per-user contribution counts for ground truth tracking.
     */
    private Map<Long, Long> userContributionCounts;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.numUsers = ((Number) conf.getOrDefault("synthetic.num.users", 10_000_000)).intValue();
        this.numKeys = ((Number) conf.getOrDefault("synthetic.num.keys", 1_000_000)).intValue();
        long randomSeed = ((Number) conf.getOrDefault("synthetic.seed", 42L)).longValue();
        this.groundTruthEnabled = Boolean.parseBoolean(
                String.valueOf(conf.getOrDefault("synthetic.ground-truth.enabled", "false")));

        LOG.info("BaselineSpout initialized with: numUsers={}, numKeys={}, seed={}, groundTruth={}",
                numUsers, numKeys, randomSeed, groundTruthEnabled);

        this.rng = new Random(randomSeed);
        this.keyDistribution = new ZipfMandelbrotDistribution(numKeys, 1000, 1.4, rng);

        if (groundTruthEnabled) {
            this.userContributionCounts = new ConcurrentHashMap<>();
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
