package ch.usi.inf.confidentialstorm.host.bolts.dp;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.storm.Config;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Coordinates epoch synchronization across DataPerturbationBolt replicas using ZooKeeper.
 * <p>
 * Uses a shared ZK epoch counter ({@code /current-epoch}) instead of blocking barriers.
 * Replicas only advance their local epoch when the global counter indicates all replicas
 * have completed the previous epoch. This prevents epoch drift without blocking the bolt thread.
 * <p>
 * Coordination phases:
 * <ol>
 *   <li><b>Startup barrier:</b> All replicas register and wait until all are ready.</li>
 *   <li><b>Epoch gating:</b> A shared counter gates when replicas may start the next snapshot.
 *       Replicas register completion under {@code /completed/{epoch}/{taskId}}.
 *       The leader advances the counter when all replicas have completed.</li>
 * </ol>
 */
public class EpochBarrierCoordinator implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(EpochBarrierCoordinator.class);

    private static final String STARTUP_BARRIER_PATH = "/startup-barrier";
    private static final String STARTUP_READY_PATH = "/startup-ready";
    private static final String CURRENT_EPOCH_PATH = "/current-epoch";
    private static final String COMPLETED_PATH = "/completed";

    private final CuratorFramework client;
    private final int taskId;
    private final int totalReplicas;
    private final boolean isLeader;

    private volatile boolean ready = false;

    /** Cached target epoch to avoid ZK reads on every tick when nothing changed. */
    private volatile int cachedTargetEpoch = 0;

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

        LOG.info("[EpochBarrier] Task {} initialized (leader={}, replicas={})",
                taskId, isLeader, totalReplicas);
    }

    /**
     * Waits for all replicas to be ready, then invokes onReady on a background thread.
     * The leader also initializes the epoch counter to 1 (the first target epoch).
     */
    public void awaitStartup(Runnable onReady) {
        Thread startupThread = new Thread(() -> {
            try {
                DistributedBarrier barrier = new DistributedBarrier(client, STARTUP_BARRIER_PATH);

                // only the leader initializes the barrier for everyone to wait on
                if (isLeader) {
                    barrier.setBarrier();
                }

                // Register this replica
                String myPath = STARTUP_READY_PATH + "/" + taskId;
                client.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(myPath);

                // if leader: wait for all replicas to register before removing barrier and proceeding
                if (isLeader) {
                    LOG.info("[EpochBarrier] Leader task {} waiting for {}/{} replicas",
                            taskId, 0, totalReplicas);
                    while (true) {
                        List<String> children = client.getChildren().forPath(STARTUP_READY_PATH);
                        if (children.size() >= totalReplicas) {
                            LOG.info("[EpochBarrier] All {} replicas registered", totalReplicas);

                            // Initialize the epoch counter to 1 (first target)
                            initializeEpochCounter(1);

                            barrier.removeBarrier();
                            break;
                        }
                        Thread.sleep(500);
                    }
                }
                // if slave: wait for leader to remove barrier before proceeding
                else {
                    LOG.info("[EpochBarrier] Task {} waiting on startup barrier", taskId);
                    boolean success = barrier.waitOnBarrier(60, TimeUnit.SECONDS);
                    if (!success) {
                        LOG.warn("[EpochBarrier] Task {} startup barrier timed out after 60s", taskId);
                    }
                }

                // signal ready and execute callback
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
     * Creates the {@code /current-epoch} ZK node with the given initial value.
     * Called by the leader after all replicas have registered.
     */
    private void initializeEpochCounter(int initialValue) {
        try {
            byte[] data = intToBytes(initialValue);
            if (client.checkExists().forPath(CURRENT_EPOCH_PATH) != null) {
                client.setData().forPath(CURRENT_EPOCH_PATH, data);
            } else {
                client.create().creatingParentsIfNeeded().forPath(CURRENT_EPOCH_PATH, data);
            }
            cachedTargetEpoch = initialValue;
            LOG.info("[EpochBarrier] Initialized /current-epoch = {}", initialValue);
        } catch (Exception e) {
            LOG.error("[EpochBarrier] Failed to initialize epoch counter", e);
        }
    }

    /**
     * Non-blocking read of {@code /current-epoch} from ZK.
     * Returns the cached value on ZK error.
     */
    public int getTargetEpoch() {
        try {
            byte[] data = client.getData().forPath(CURRENT_EPOCH_PATH);
            cachedTargetEpoch = bytesToInt(data);
        } catch (Exception e) {
            LOG.debug("[EpochBarrier] Task {} failed to read /current-epoch, using cached={}", taskId, cachedTargetEpoch);
        }
        return cachedTargetEpoch;
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
            // Node may already exist if we re-register (e.g. after a retry)
            LOG.debug("[EpochBarrier] Task {} completion registration for epoch {} failed (may already exist)", taskId, epoch, e);
        }
    }

    /**
     * Leader only: checks if all replicas have completed {@code currentTargetEpoch},
     * and if so, advances {@code /current-epoch} to {@code currentTargetEpoch + 1}.
     *
     * @return true if the epoch was advanced
     */
    public boolean tryAdvanceEpoch(int currentTargetEpoch) {
        if (!isLeader) return false;

        String completedPath = COMPLETED_PATH + "/" + currentTargetEpoch;
        try {
            // if no replicas have registered completion for the current epoch, the path won't exist
            if (client.checkExists().forPath(completedPath) == null) {
                return false;
            }

            // count how many replicas have registered completion for the current epoch
            List<String> children = client.getChildren().forPath(completedPath);

            // check if all replicas have registered completion for the current epoch
            if (children.size() >= totalReplicas) {

                // prepare for the next epoch by advancing the global epoch counter
                int nextEpoch = currentTargetEpoch + 1;
                client.setData().forPath(CURRENT_EPOCH_PATH, intToBytes(nextEpoch));
                cachedTargetEpoch = nextEpoch;

                LOG.info("[EpochBarrier] Advanced to epoch {}", nextEpoch);

                // remove completion nodes for the previous epoch to prevent ZK bloat
                pruneCompletedEpoch(currentTargetEpoch - 1);
                return true;
            }
        } catch (Exception e) {
            LOG.warn("[EpochBarrier] Leader failed to check/advance epoch {}", currentTargetEpoch, e);
        }
        return false;
    }

    private void pruneCompletedEpoch(int oldEpoch) {
        if (oldEpoch < 0) return;

        try {
            String path = COMPLETED_PATH + "/" + oldEpoch;

            if (client.checkExists().forPath(path) != null) { // if the path doesn't exist, nothing to prune
                client.delete().deletingChildrenIfNeeded().forPath(path);
            }
        } catch (Exception e) {
            LOG.debug("[EpochBarrier] Failed to prune completed epoch {} (benign)", oldEpoch, e);
        }
    }

    private static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static int bytesToInt(byte[] data) {
        return ByteBuffer.wrap(data).getInt();
    }

    @Override
    public void close() throws IOException {
        try {
            client.close();
        } catch (Exception e) {
            LOG.warn("[EpochBarrier] Task {} error closing ZK client", taskId, e);
        }
    }
}
