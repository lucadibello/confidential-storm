package ch.usi.inf.examples.synthetic_baseline.dp;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.storm.Config;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Coordinates epoch synchronization across DataPerturbationBolt replicas using ZooKeeper.
 * (copied from confidentialstorm/host)
 */
public class EpochBarrierCoordinator implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(EpochBarrierCoordinator.class);

    private static final String STARTUP_BARRIER_PATH = "/startup-barrier";
    private static final String STARTUP_READY_PATH = "/startup-ready";
    private static final String CURRENT_EPOCH_PATH = "/current-epoch";
    private static final String COMPLETED_PATH = "/completed";

    private final CuratorFramework client;
    private final SharedCount sharedEpoch;
    private final int taskId;
    private final int totalReplicas;
    private final boolean isLeader;

    private volatile boolean ready = false;
    private volatile boolean closed = false;

    private volatile Runnable onEpochAdvanced;

    @SuppressWarnings("unchecked")
    public EpochBarrierCoordinator(Map<String, Object> topoConf, String topologyId,
                                   int taskId, int totalReplicas, int epochTimeoutSecs, boolean isLeader) {
        this.taskId = taskId;
        this.totalReplicas = totalReplicas;
        this.isLeader = isLeader;

        List<String> zkHosts = (List<String>) topoConf.get(Config.STORM_ZOOKEEPER_SERVERS);
        int zkPort = ((Number) topoConf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        String connectString = zkHosts.stream().map(h -> h + ":" + zkPort).collect(Collectors.joining(","));

        this.client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(30_000)
                .connectionTimeoutMs(15_000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace("confidential-storm/" + topologyId + "/dp")
                .build();
        client.start();

        this.sharedEpoch = new SharedCount(client, CURRENT_EPOCH_PATH, 0);

        sharedEpoch.addListener(new SharedCountListener() {
            @Override
            public void countHasChanged(SharedCountReader reader, int newCount) {
                LOG.debug("[EpochBarrier] Task {} received epoch change notification: {}",
                        taskId, newCount);
                Runnable callback = onEpochAdvanced;
                if (callback != null) {
                    callback.run();
                }
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                LOG.info("[EpochBarrier] Task {} ZK connection state: {}", taskId, newState);
            }
        });

        LOG.info("[EpochBarrier] Task {} initialized (leader={}, replicas={})",
                taskId, isLeader, totalReplicas);
    }

    public void setOnEpochAdvanced(Runnable callback) {
        this.onEpochAdvanced = callback;
    }

    public void awaitStartup(Runnable onReady) {
        Thread startupThread = new Thread(() -> {
            try {
                sharedEpoch.start();

                DistributedBarrier barrier = new DistributedBarrier(client, STARTUP_BARRIER_PATH);

                if (isLeader) {
                    barrier.setBarrier();
                }

                String myPath = STARTUP_READY_PATH + "/" + taskId;
                client.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(myPath);

                if (isLeader) {
                    LOG.info("[EpochBarrier] Leader task {} waiting for {}/{} replicas",
                            taskId, 0, totalReplicas);
                    while (true) {
                        List<String> children = client.getChildren().forPath(STARTUP_READY_PATH);
                        if (children.size() >= totalReplicas) {
                            LOG.info("[EpochBarrier] All {} replicas registered", totalReplicas);
                            sharedEpoch.setCount(1);
                            LOG.info("[EpochBarrier] Initialized epoch counter to 1");
                            barrier.removeBarrier();
                            break;
                        }
                        Thread.sleep(500);
                    }
                } else {
                    LOG.info("[EpochBarrier] Task {} waiting on startup barrier", taskId);
                    boolean success = barrier.waitOnBarrier(60, TimeUnit.SECONDS);
                    if (!success) {
                        LOG.warn("[EpochBarrier] Task {} startup barrier timed out after 60s", taskId);
                    }
                }

                ready = true;
                onReady.run();
                LOG.info("[EpochBarrier] Task {} startup complete", taskId);

            } catch (Exception e) {
                LOG.error("[EpochBarrier] Task {} startup failed", taskId, e);
            }
        }, "epoch-startup-" + taskId);
        startupThread.setDaemon(true);
        startupThread.start();
    }

    public boolean isReady() {
        return ready;
    }

    public int getTargetEpoch() {
        return sharedEpoch.getCount();
    }

    public void registerCompletion(int epoch) {
        String path = COMPLETED_PATH + "/" + epoch + "/" + taskId;
        try {
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path);
            LOG.debug("[EpochBarrier] Task {} registered completion for epoch {}", taskId, epoch);
        } catch (Exception e) {
            LOG.debug("[EpochBarrier] Task {} completion registration for epoch {} failed (may already exist)", taskId, epoch, e);
        }
    }

    public boolean tryAdvanceEpoch(int currentTargetEpoch) {
        String completedPath = COMPLETED_PATH + "/" + currentTargetEpoch;
        try {
            if (client.checkExists().forPath(completedPath) == null) {
                return false;
            }

            List<String> children = client.getChildren().forPath(completedPath);

            if (children.size() >= totalReplicas) {
                int nextEpoch = currentTargetEpoch + 1;
                VersionedValue<Integer> previous = sharedEpoch.getVersionedValue();
                boolean success = previous.getValue() == currentTargetEpoch
                        && sharedEpoch.trySetCount(previous, nextEpoch);
                if (success) {
                    LOG.info("[EpochBarrier] Task {} advanced to epoch {}", taskId, nextEpoch);
                    if (isLeader) {
                        pruneCompletedEpoch(currentTargetEpoch - 1);
                    }
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.debug("[EpochBarrier] Task {} failed to check/advance epoch {} (benign)", taskId, currentTargetEpoch, e);
        }
        return false;
    }

    private void pruneCompletedEpoch(int oldEpoch) {
        if (oldEpoch < 0) return;
        try {
            String path = COMPLETED_PATH + "/" + oldEpoch;
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
            }
        } catch (Exception e) {
            LOG.debug("[EpochBarrier] Failed to prune completed epoch {} (benign)", oldEpoch, e);
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            sharedEpoch.close();
        } catch (Exception e) {
            LOG.debug("[EpochBarrier] Task {} error closing SharedCount", taskId, e);
        }
        try {
            client.close();
        } catch (Exception e) {
            LOG.warn("[EpochBarrier] Task {} error closing ZK client", taskId, e);
        }
    }
}
