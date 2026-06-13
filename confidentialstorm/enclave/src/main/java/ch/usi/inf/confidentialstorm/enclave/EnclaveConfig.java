package ch.usi.inf.confidentialstorm.enclave;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.enclave.crypto.EncryptionScheme;
import ch.usi.inf.confidentialstorm.enclave.util.logger.LogLevel;

import java.util.ServiceLoader;

/**
 * Configuration parameters for the secure enclave.
 * These parameters are loaded via a {@link EnclaveConfiguration} provider.
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

    // Configuration options
    /**
     * Hexadecimal stream key to decrypt data within the enclave.
     * For production, implement a secure key provisioning mechanism (e.g., using Intel SGX Remote Attestation).
     */
    public static final String STREAM_KEY_HEX = provider.getStreamKeyHex();

    /**
     * Minimum log level for enclave logging (fixed value).
     */
    public static final LogLevel LOG_LEVEL = provider.getLogLevel();

    /**
     * Whether exception isolation is enabled within the enclave.
     * If true, exceptions are isolated to prevent information leakage.
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

    /**
     * The encryption scheme used for sealing payloads.
     * Immutable at runtime; becomes a build-time constant in GraalVM native-image.
     */
    public static final EncryptionScheme ENCRYPTION_SCHEME = provider.getEncryptionScheme();

    private EnclaveConfig() {
        // Prevent instantiation
    }
}
