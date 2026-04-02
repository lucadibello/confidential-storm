package ch.usi.inf.examples.synthetic_baseline.bolts;

import ch.usi.inf.examples.synthetic_baseline.config.DPConfig;
import ch.usi.inf.examples.synthetic_baseline.dp.UserContributionLimiter;
import ch.usi.inf.examples.synthetic_baseline.profiling.BaselineBoltLifecycleEvent;
import ch.usi.inf.examples.synthetic_baseline.profiling.BoltProfiler;
import ch.usi.inf.examples.synthetic_baseline.profiling.ProfilerConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Contribution bounding bolt - performs user contribution limiting
 * and per-record clamping directly in the JVM (no ECALL).
 */
public class BaselineContributionBoundingBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BaselineContributionBoundingBolt.class);

    private OutputCollector collector;
    private UserContributionLimiter limiter;
    private int boltId;
    private transient BoltProfiler profiler;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.limiter = new UserContributionLimiter();
        this.boltId = context.getThisTaskId();

        if (ProfilerConfig.ENABLED) {
            this.profiler = new BoltProfiler(context.getThisComponentId(), boltId);
            profiler.recordLifecycleEvent(BaselineBoltLifecycleEvent.COMPONENT_STARTED);
        }

        LOG.info("[BaselineContributionBounding {}] Prepared", boltId);
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("key");
        double count = input.getDoubleByField("count");
        String userId = input.getStringByField("userId");

        // Check contribution limit (C = MAX_CONTRIBUTIONS_PER_USER)
        long t0 = ProfilerConfig.ENABLED && profiler.shouldSample() ? System.nanoTime() : 0;
        boolean allowed = limiter.allow(userId, DPConfig.MAX_CONTRIBUTIONS_PER_USER);
        double clampedCount = allowed ? Math.max(0.0, Math.min(count, DPConfig.PER_RECORD_CLAMP)) : 0;
        if (t0 != 0) profiler.recordEcall("checkAndClamp", System.nanoTime() - t0);

        if (ProfilerConfig.ENABLED) {
            profiler.incrementEcallTotal("checkAndClamp");
            profiler.incrementCounter(allowed ? "forwarded" : "dropped");
            profiler.onTupleProcessed();
        }

        if (allowed) {
            // dpRoutingKey = word for consistent partitioning to DataPerturbation replicas
            collector.emit(input, new Values(word, clampedCount, userId, word));
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {
        if (ProfilerConfig.ENABLED && profiler != null) {
            profiler.recordLifecycleEvent(BaselineBoltLifecycleEvent.COMPONENT_STOPPING);
            profiler.writeReport();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count", "userId", "dpRoutingKey"));
    }
}
