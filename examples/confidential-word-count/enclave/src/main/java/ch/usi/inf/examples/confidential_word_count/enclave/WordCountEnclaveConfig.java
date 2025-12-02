package ch.usi.inf.examples.confidential_word_count.enclave;

import ch.usi.inf.confidentialstorm.enclave.EnclaveConfiguration;
import ch.usi.inf.confidentialstorm.enclave.util.logger.LogLevel;
import com.google.auto.service.AutoService;

@AutoService(EnclaveConfiguration.class)
public class WordCountEnclaveConfig implements EnclaveConfiguration {
    @Override
    public String getStreamKeyHex() {
        // FIXME: this is a dummy key, replace with a secure key for production use
        return "5c03a616612ac47a25a16013bc3edaeda0ef306f7576e8a0ef6aba0ccf213cb3";
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
