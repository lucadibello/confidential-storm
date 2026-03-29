package ch.usi.inf.confidentialstorm.host.bolts.dp;

import ch.usi.inf.confidentialstorm.host.bolts.LifecycleEvent;

public enum DPBoltLifecycleEvent implements LifecycleEvent {
    BARRIER_RELEASED,
    EPOCH_ADVANCED,
    MAX_EPOCH_REACHED,
    SNAPSHOT_STARTED,
    SNAPSHOT_COMPLETED,
    DUMMY_RELEASED,
    TICK_RECEIVED,
    TICK_INTERVAL_SECS,
    MAX_EPOCHS_CONFIGURED
}
