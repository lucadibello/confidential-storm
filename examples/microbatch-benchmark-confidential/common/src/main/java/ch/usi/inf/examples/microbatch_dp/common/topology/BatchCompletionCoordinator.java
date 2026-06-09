package ch.usi.inf.examples.microbatch_dp.common.topology;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ZooKeeper-backed handshake between the spout and the aggregator for
 * sequential micro-batch execution. See the baseline-variant copy for the
 * full design notes.
 */
public final class BatchCompletionCoordinator implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(BatchCompletionCoordinator.class);

    private static final String COMPLETED_BATCH_PATH = "/microbatch/completed";
    private static final int INITIAL_VALUE = -1;

    private final CuratorFramework client;
    private final SharedCount completedBatch;

    @SuppressWarnings("unchecked")
    public BatchCompletionCoordinator(Map<String, Object> topoConf, String topologyId) {
        List<String> zkHosts = (List<String>) topoConf.get(Config.STORM_ZOOKEEPER_SERVERS);
        int zkPort = ((Number) topoConf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        String connectString = zkHosts.stream()
                .map(h -> h + ":" + zkPort)
                .collect(Collectors.joining(","));

        this.client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(30_000)
                .connectionTimeoutMs(15_000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace("microbatch-storm/" + topologyId)
                .build();
        this.client.start();
        this.completedBatch = new SharedCount(client, COMPLETED_BATCH_PATH, INITIAL_VALUE);
        try {
            this.completedBatch.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start BatchCompletionCoordinator SharedCount", e);
        }
        LOG.info("[BatchCompletion] Connected to ZK at {} (ns=microbatch-storm/{})", connectString, topologyId);
    }

    public void awaitCompletion(int batchId, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            int current = completedBatch.getCount();
            if (current >= batchId) return;
            if (System.currentTimeMillis() >= deadline) {
                throw new InterruptedException("Timed out waiting for batch " + batchId
                        + " (last completed=" + current + ")");
            }
            Thread.sleep(50);
        }
    }

    public void publishCompletion(int batchId) {
        try {
            int prev;
            do {
                prev = completedBatch.getCount();
                if (prev >= batchId) return;
            } while (!completedBatch.trySetCount(completedBatch.getVersionedValue(), batchId));
            LOG.info("[BatchCompletion] Published completion for batch {} (was {})", batchId, prev);
        } catch (Exception e) {
            LOG.error("[BatchCompletion] Failed to publish completion for batch {}", batchId, e);
        }
    }

    @Override
    public void close() {
        try { completedBatch.close(); } catch (Exception e) { LOG.warn("close failure", e); }
        client.close();
    }
}
