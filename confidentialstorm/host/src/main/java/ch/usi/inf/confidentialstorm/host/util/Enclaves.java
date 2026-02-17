package ch.usi.inf.confidentialstorm.host.util;

import org.apache.teaclave.javasdk.host.Enclave;
import org.apache.teaclave.javasdk.host.EnclaveFactory;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Utility class for enclave creation and service loading.
 */
public class Enclaves {
    private static final String ENCLAVE_TYPE_CONF_KEY = "confidentialstorm.enclave.type";

    // create logger using slf4j
    private static final Logger LOG = LoggerFactory.getLogger(Enclaves.class);

    /**
     * Creates an enclave of the specified type.
     *
     * @param enclaveType the enclave type
     * @return the created enclave
     * @throws IllegalStateException if creation fails
     */
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

    /**
     * Loads a service from the specified enclave.
     *
     * @param enclave      the enclave to load from
     * @param serviceClass the class of the service to load
     * @param <S>          the service type
     * @return the loaded service
     * @throws IllegalStateException if the service cannot be loaded
     */
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

    /**
     * Resolves the enclave type from topology configuration or environment.
     *
     * @param topoConf           the topology configuration
     * @param defaultEnclaveType the default enclave type to use if none is found
     * @return the resolved enclave type
     */
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
