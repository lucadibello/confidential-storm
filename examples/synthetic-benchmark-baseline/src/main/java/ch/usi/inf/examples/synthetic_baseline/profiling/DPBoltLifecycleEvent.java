package ch.usi.inf.examples.synthetic_baseline.profiling;

/**
 * Lifecycle events specific to DP bolts (data perturbation and histogram aggregation).
 */
public enum DPBoltLifecycleEvent implements LifecycleEvent {
    EPOCH_ADVANCED,
    MAX_EPOCH_REACHED,
    SNAPSHOT_STARTED,
    SNAPSHOT_COMPLETED,
    TICK_INTERVAL_SECS,
    MAX_EPOCHS_CONFIGURED
}
