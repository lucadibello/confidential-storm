package ch.usi.inf.confidentialstorm.enclave;

import ch.usi.inf.confidentialstorm.enclave.crypto.EncryptionScheme;
import ch.usi.inf.confidentialstorm.enclave.util.logger.LogLevel;

/**
 * Interface for providing enclave configuration parameters.
 * Implementations should be registered via ServiceLoader.
 */
public interface EnclaveConfiguration {
    /**
     * Gets the stream key in hexadecimal format.
     *
     * @return the stream key
     */
    String getStreamKeyHex();

    /**
     * Gets the log level for the enclave.
     *
     * @return the log level
     */
    LogLevel getLogLevel();

    /**
     * Checks if exception isolation is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean isExceptionIsolationEnabled();

    /**
     * Checks if route validation is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean isRouteValidationEnabled();

    /**
     * Checks if replay protection is enabled.
     *
     * @return true if enabled, false otherwise
     */
    boolean isReplayProtectionEnabled();

    /**
     * Gets the encryption scheme used by the enclave for sealing payloads.
     * Override this method to switch between AEAD ciphers or disable encryption for benchmarking.
     *
     * @return the encryption scheme (default: {@link EncryptionScheme#CHACHA20_POLY1305})
     */
    default EncryptionScheme getEncryptionScheme() {
        return EncryptionScheme.CHACHA20_POLY1305;
    }
}
