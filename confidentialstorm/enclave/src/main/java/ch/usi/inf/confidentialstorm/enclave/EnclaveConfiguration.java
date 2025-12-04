package ch.usi.inf.confidentialstorm.enclave;

import ch.usi.inf.confidentialstorm.enclave.util.logger.LogLevel;

public interface EnclaveConfiguration {
    String getStreamKeyHex();

    LogLevel getLogLevel();

    boolean isExceptionIsolationEnabled();

    boolean isRouteValidationEnabled();

    boolean isReplayProtectionEnabled();
}
