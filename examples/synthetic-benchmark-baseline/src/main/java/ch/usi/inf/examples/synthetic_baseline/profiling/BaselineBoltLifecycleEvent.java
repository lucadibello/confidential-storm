package ch.usi.inf.examples.synthetic_baseline.profiling;

/**
 * Lifecycle events for baseline bolts. Mirrors {@code ConfidentialBoltLifecycleEvent}
 * from the enclave version but without enclave-specific events.
 */
public enum BaselineBoltLifecycleEvent implements LifecycleEvent {
    COMPONENT_STARTED,
    COMPONENT_STOPPING
}
