package ch.usi.inf.confidentialstorm.host.bolts;

public enum ConfidentialBoltLifecycleEvent implements LifecycleEvent {
    COMPONENT_INITIALIZING_ENCLAVE,
    COMPONENT_ENCLAVE_INITIALIZED,
    COMPONENT_STARTED,
    COMPONENT_STOPPING
}
