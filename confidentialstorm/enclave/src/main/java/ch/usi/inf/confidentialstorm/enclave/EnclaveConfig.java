package ch.usi.inf.confidentialstorm.enclave;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.enclave.util.logger.LogLevel;

import java.util.ServiceLoader;

/**
 * Configuration parameters for the secure enclave.
 */
public final class EnclaveConfig {
    /**
     * The EnclaveConfiguration provider loaded via ServiceLoader.
     */
    private static final EnclaveConfiguration provider;

    static {
        ServiceLoader<EnclaveConfiguration> loader = ServiceLoader.load(EnclaveConfiguration.class);
        EnclaveConfiguration found = null;
        for (EnclaveConfiguration config : loader) {
            found = config;
            break; // Use the first one found
        }

        if (found == null) {
            EnclaveServiceException exception = new EnclaveServiceException(
                    "EnclaveConfiguration",
                    "No EnclaveConfiguration provider found. Ensure a configuration service is registered via ServiceLoader."
            );
            throw new RuntimeException(exception);
        }

        provider = found;
    }

    // Various feature toggles - can be enabled/disabled as needed
    /**
     * Hard-coded stream key in hexadecimal format to decrypt data within the enclave.
     * <p>
     * NOTE: this is just for demonstration purposes. For production we may need to setup
     * a secure key provisioning mechanism (i.e., using Intel SGX Remote Attestation).
     */
    public static final String STREAM_KEY_HEX = provider.getStreamKeyHex();
    /**
     * Minimum log level for enclave logging (fixed value).
     */
    public static final LogLevel LOG_LEVEL = provider.getLogLevel();
    /**
     * Whether we should segregate exceptions within the enclave or not.
     * <p>
     * If true, exceptions will be isolated to prevent information leakage.
     * If false, exceptions may propagate normally to the untrusted application.
     */
    public static final boolean ENABLE_EXCEPTION_ISOLATION = provider.isExceptionIsolationEnabled();
    /**
     * Whether route validation is enabled. If true, the enclave will validate routing information
     * before processing data. Routing information is stored in the AAD payload of each encrypted tuple.
     */
    public static final boolean ENABLE_ROUTE_VALIDATION = provider.isRouteValidationEnabled();
    /**
     * Whether replay protection is enabled. If true, the enclave will include additional metadata
     * to prevent replay attacks on the encrypted data.
     */
    public static final boolean ENABLE_REPLAY_PROTECTION = provider.isReplayProtectionEnabled();

    private EnclaveConfig() {
        // Prevent instantiation
    }
}