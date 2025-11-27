package ch.usi.inf.confidentialstorm.enclave;

/**
 * Configuration parameters for the secure enclave.
 */
public final class EnclaveConfig {
    private EnclaveConfig() {
        // Prevent instantiation
    }

    /**
     * Hard-coded stream key in hexadecimal format to decrypt data within the enclave.
     *
     * NOTE: this is just for demonstration purposes. For production we may need to setup
     * a secure key provisioning mechanism (i.e., using Intel SGX Remote Attestation).
     */
    public static final String STREAM_KEY_HEX = "a46bf317953bf1a8f71439f74f30cd889ec0aa318f8b6431789fb10d1053d932";

    /**
     * Minimum log level for enclave logging (fixed value).
     */
    public static final LogLevel LOG_LEVEL = LogLevel.INFO;


    /**
     * Whether we should segregate exceptions within the enclave or not.
     *
     * If true, exceptions will be isolated to prevent information leakage.
     * If false, exceptions may propagate normally to the untrusted application.
     */
    public static final boolean ENABLE_EXCEPTION_SEGREGATION = true;

    // Various feature toggles - can be enabled/disabled as needed

    /**
     * Whether route validation is enabled. If true, the enclave will validate routing information
     * before processing data. Routing information is stored in the AAD payload of each encrypted tuple.
     */
    public static final boolean ENABLE_ROUTE_VALIDATION = true;

    /**
     * Whether replay protection is enabled. If true, the enclave will include additional metadata
     * to prevent replay attacks on the encrypted data.
     */
    public static final boolean ENABLE_REPLAY_PROTECTION = true;

    public enum LogLevel {
        DEBUG,
        INFO,
        WARN,
        ERROR
    }
}
