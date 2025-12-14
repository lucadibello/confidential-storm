package ch.usi.inf.examples.synthetic_dp.enclave;

import ch.usi.inf.confidentialstorm.enclave.EnclaveConfiguration;
import ch.usi.inf.confidentialstorm.enclave.util.logger.LogLevel;
import com.google.auto.service.AutoService;

/**
 * Example enclave configuration: disables route validation/replay for simplicity and uses a fixed stream key.
 */
@AutoService(EnclaveConfiguration.class)
public final class SyntheticEnclaveConfigProvider implements EnclaveConfiguration {
    private static final String DEFAULT_STREAM_KEY =
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    @Override
    public String getStreamKeyHex() {
        return DEFAULT_STREAM_KEY;
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
        return false;
    }

    @Override
    public boolean isReplayProtectionEnabled() {
        return false;
    }
}
