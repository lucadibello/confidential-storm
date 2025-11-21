package ch.usi.inf.confidentialstorm.host.util;

import org.apache.teaclave.javasdk.host.Enclave;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.apache.teaclave.javasdk.host.exception.ServicesLoadingException;
import org.apache.teaclave.javasdk.host.exception.EnclaveCreatingException;
import org.apache.teaclave.javasdk.host.EnclaveFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class Enclaves {
    private static final String ENCLAVE_TYPE_CONF_KEY = "confidentialstorm.enclave.type";

    // create logger using slf4j
    private static final Logger LOG = LoggerFactory.getLogger(Enclaves.class);

    public static Enclave createEnclave(EnclaveType enclaveType) {
        LOG.info("Creating enclave of type {}", enclaveType);
        try {
            Enclave enclave = EnclaveFactory.create(enclaveType);
            LOG.info("Successfully created enclave of type {}", enclaveType);
            return enclave;
        } catch (Throwable e) {
            LOG.error("Unable to create enclave of type {}", enclaveType, e);
            throw new IllegalStateException("Unable to create enclave of type " + enclaveType, e);
        }
    }


    public static <S> S loadService(Enclave enclave, Class<S> serviceClass) {
        LOG.info("Loading service {} from enclave", serviceClass.getName());
        try {
            Iterator<S> services = enclave.load(serviceClass);
            if (!services.hasNext()) {
                throw new IllegalStateException("No enclave service registered for " + serviceClass.getName());
            }
            S service = services.next();
            LOG.info("Successfully loaded service {} from enclave", serviceClass.getName());
            return service;
        } catch (Throwable e) {
            LOG.error("Unable to load enclave service for {}", serviceClass.getName(), e);
            throw new IllegalStateException("Unable to load enclave service for " + serviceClass.getName(), e);
        }
    }


    public static EnclaveType resolveEnclaveType(Map<String, Object> topoConf, EnclaveType defaultEnclaveType) {
        String override = extractOverrideFromTopologyConf(topoConf);
        if (override == null) {
            return defaultEnclaveType;
        }
        try {
            return EnclaveType.valueOf(override.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            LOG.warn("Unknown enclave type '{}', falling back to {}", override, defaultEnclaveType);
            return defaultEnclaveType;
        }
    }


    private static String extractOverrideFromTopologyConf(Map<String, Object> topoConf) {
        Object confValue = topoConf != null ? topoConf.get(ENCLAVE_TYPE_CONF_KEY) : null;
        if (confValue instanceof EnclaveType type) {
            return type.name();
        }
        if (confValue != null) {
            return Objects.toString(confValue, null);
        }
        String sys = System.getProperty(ENCLAVE_TYPE_CONF_KEY);
        if (sys != null && !sys.isBlank()) {
            return sys;
        }
        String envKey = ENCLAVE_TYPE_CONF_KEY.replace('.', '_').toUpperCase(Locale.ROOT);
        String env = System.getenv(envKey);
        if (env != null && !env.isBlank()) {
            return env;
        }
        return null;
    }
}
