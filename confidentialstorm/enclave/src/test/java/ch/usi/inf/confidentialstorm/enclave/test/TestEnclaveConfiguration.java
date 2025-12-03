package ch.usi.inf.confidentialstorm.enclave.test;

import ch.usi.inf.confidentialstorm.enclave.EnclaveConfiguration;
import ch.usi.inf.confidentialstorm.enclave.util.logger.LogLevel;

public class TestEnclaveConfiguration implements EnclaveConfiguration {

    @Override
    public String getStreamKeyHex() {
        // Use the same key as the test dataset
        return "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    }

    @Override
    public LogLevel getLogLevel() {
        return LogLevel.DEBUG;
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
}
