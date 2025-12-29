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
        EnclaveConfiguration found = null;
        try {
            System.err.println("EnclaveConfig: Initializing ServiceLoader...");
            ServiceLoader<EnclaveConfiguration> loader = ServiceLoader.load(EnclaveConfiguration.class);
            for (EnclaveConfiguration config : loader) {
                System.err.println("EnclaveConfig: Found provider: " + config.getClass().getName());
                found = config;
                break; // Use the first one found
            }
        } catch (Throwable t) {
            System.err.println("EnclaveConfig: Error loading provider: " + t.getMessage());
            t.printStackTrace();
        }

        if (found == null) {
            System.err.println("EnclaveConfig: No EnclaveConfiguration provider found! Using hardcoded fallback.");
            found = new EnclaveConfiguration() {
                @Override
                public String getStreamKeyHex() {
                    return "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
                }

                @Override
                public LogLevel getLogLevel() {
                    return LogLevel.INFO;
                }

                @Override
                public boolean isExceptionIsolationEnabled() {
                    return false;
                }

                @Override
                public boolean isRouteValidationEnabled() {
                    return true;
                }

                @Override
                public boolean isReplayProtectionEnabled() {
                    return true;
                }
            };
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