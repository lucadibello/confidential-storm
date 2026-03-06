package ch.usi.inf.confidentialstorm.host.bolts.dp;

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
 * <p>
 * Uses a Curator {@link SharedCount} on {@code /current-epoch} as the global epoch counter.
 * The {@code SharedCountListener} notifies all replicas immediately when the epoch advances,
 * so they can start their next background computation without waiting for the next tick.
 * <p>
 * Replicas register completion under {@code /completed/{epoch}/{taskId}}. Any replica may
 * advance the counter when all completions are present.
 * <p>
 * Coordination phases:
 * <ol>
 *   <li><b>Startup barrier:</b> All replicas register and wait until all are ready.</li>
 *   <li><b>Epoch gating:</b> The shared counter gates when replicas may start the next snapshot.
 *       The {@code SharedCountListener} pushes epoch changes to all replicas reactively.</li>
 * </ol>
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

    /** Callback invoked when the epoch advances (from SharedCount listener). May be null. */
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

        // SharedCount with seed value 0 -- leader will set it to 1 after startup
        this.sharedEpoch = new SharedCount(client, CURRENT_EPOCH_PATH, 0);

        // Register listener for epoch changes
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

    /**
     * Sets the callback to invoke when the global epoch advances.
     * The callback runs on a Curator {@link SharedCountListener} event thread -- it must be non-blocking.
     */
    public void setOnEpochAdvanced(Runnable callback) {
        this.onEpochAdvanced = callback;
    }

    /**
     * Waits for all replicas to be ready, then invokes onReady on a background thread.
     * The leader also sets the epoch counter to 1 (the first target epoch).
     */
    public void awaitStartup(Runnable onReady) {
        Thread startupThread = new Thread(() -> {
            try {
                // Start the SharedCount (must happen before any reads/writes)
                sharedEpoch.start();

                DistributedBarrier barrier = new DistributedBarrier(client, STARTUP_BARRIER_PATH);

                if (isLeader) {
                    barrier.setBarrier();
                }

                // Register this replica
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

                            // Set the epoch counter to 1 (first target)
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

    /**
     * Returns whether the startup barrier has been passed.
     */
    public boolean isReady() {
        return ready;
    }

    /**
     * Returns the current target epoch from the SharedCount (locally cached by Curator).
     */
    public int getTargetEpoch() {
        return sharedEpoch.getCount();
    }

    /**
     * Register this replica as having completed the given epoch.
     * Creates an ephemeral node at {@code /completed/{epoch}/{taskId}}.
     */
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

    /**
     * Checks if all replicas have completed {@code currentTargetEpoch},
     * and if so, advances {@code /current-epoch} to {@code currentTargetEpoch + 1}
     * using {@link SharedCount#trySetCount(VersionedValue, int)} for compare-and-swap safety.
     * <p>
     * Any replica may call this. The versioned CAS ensures that only one replica
     * actually performs the write if multiple race -- the first one to write wins,
     * and subsequent attempts fail harmlessly because the version has changed.
     * The {@link SharedCountListener} then notifies all replicas of the new value.
     * Only the leader prunes old completion nodes to avoid concurrent delete races.
     *
     * @return true if this replica successfully advanced the epoch
     */
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
                // Another replica already advanced -- that's fine, the listener will notify us
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
