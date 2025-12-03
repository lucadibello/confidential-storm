package ch.usi.inf.examples.confidential_word_count.enclave;

import ch.usi.inf.confidentialstorm.enclave.EnclaveConfiguration;
import ch.usi.inf.confidentialstorm.enclave.util.logger.LogLevel;
import com.google.auto.service.AutoService;

@AutoService(EnclaveConfiguration.class)
public class WordCountEnclaveConfig implements EnclaveConfiguration {
    @Override
    public String getStreamKeyHex() {
        // FIXME: this is a dummy key, replace with a secure key for production use
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
