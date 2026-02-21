package ch.usi.inf.confidentialstorm.common.tunnel;

/**
 * Configuration key constants and defaults for cloaked tunnel bolts.
 */
public final class TunnelConstants {

    /** Storm config key for the epoch interval in seconds. */
    public static final String EPOCH_INTERVAL_SECS = "confidentialstorm.tunnel.epoch.interval.secs";

    /** Storm config key for the number of slots per batch. */
    public static final String BATCH_SIZE = "confidentialstorm.tunnel.batch.size";

    /** Storm config key for the byte size of each slot. */
    public static final String SLOT_SIZE_BYTES = "confidentialstorm.tunnel.slot.size.bytes";

    /** Default epoch interval: 1 second. */
    public static final int DEFAULT_EPOCH_INTERVAL_SECS = 1;

    /** Default number of slots per batch. */
    public static final int DEFAULT_BATCH_SIZE = 32;

    /** Default slot size in bytes (1500 bytes, roughly one MTU). */
    public static final int DEFAULT_SLOT_SIZE_BYTES = 1500;

    /** Marker byte indicating a real tuple slot. */
    public static final byte MARKER_REAL = 0x01;

    /** Marker byte indicating a dummy/padding slot. */
    public static final byte MARKER_DUMMY = 0x00;

    private TunnelConstants() {
    }
}
