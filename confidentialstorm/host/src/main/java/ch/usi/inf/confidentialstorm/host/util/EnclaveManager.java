package ch.usi.inf.confidentialstorm.host.util;

import org.apache.teaclave.javasdk.host.Enclave;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.apache.teaclave.javasdk.host.exception.EnclaveDestroyingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * Manages the lifecycle of an enclave and its associated service.
 *
 * @param <S> the type of the enclave service
 */
public class EnclaveManager<S> {
    private static final Logger LOG = LoggerFactory.getLogger(EnclaveManager.class);
    private final Class<S> serviceClass;
    private final EnclaveType defaultEnclaveType;
    private Enclave enclave;
    // This is done to allow developers to specify the enclave type at runtime via a
    // system property
    private EnclaveType activeEnclaveType;
    private S service;

    /**
     * Constructs a new EnclaveManager with default enclave type (TEE_SDK).
     *
     * @param serviceClass the class of the enclave service
     */
    public EnclaveManager(Class<S> serviceClass) {
        this(serviceClass, EnclaveType.TEE_SDK);
    }

    /**
     * Constructs a new EnclaveManager with specific enclave type.
     *
     * @param serviceClass the class of the enclave service
     * @param enclaveType  the enclave type to use
     */
    public EnclaveManager(Class<S> serviceClass, EnclaveType enclaveType) {
        LOG.info("Creating EnclaveManager for service {}", serviceClass.getName());
        this.serviceClass = serviceClass;
        this.defaultEnclaveType = enclaveType;
    }

    /**
     * Initializes the enclave and loads the service.
     *
     * @param topoConf the topology configuration
     */
    public void initializeEnclave(Map<String, Object> topoConf) {
        // create the enclave + initialize the service
        LOG.info("Preparing enclave for service {}...", serviceClass.getName());
        this.activeEnclaveType = Enclaves.resolveEnclaveType(topoConf, this.defaultEnclaveType);
        Objects.requireNonNull(this.activeEnclaveType, "Active enclave type cannot be null");

        try {
            this.enclave = Enclaves.createEnclave(activeEnclaveType);
            this.service = EnclaveServiceProxy.wrap(this.serviceClass, Enclaves.loadService(this.enclave, this.serviceClass));
            LOG.info("Confidential service {} initialized in enclave of type {}", serviceClass.getName(),
                    activeEnclaveType);
        } catch (Throwable e) {
            LOG.error("Failed to initialize confidential service {} in enclave of type {}", serviceClass.getName(),
                    activeEnclaveType, e);
            throw new RuntimeException(e); // bubble up
        }
    }

    /**
     * Destroys the enclave.
     */
    public void destroy() {
        if (enclave != null) {
            try {
                enclave.destroy();
            } catch (EnclaveDestroyingException ex) {
                LOG.warn("Failed to destroy enclave of type {}", activeEnclaveType, ex);
            }
        }
    }

    /**
     * Gets the enclave service instance.
     *
     * @return the service
     */
    public S getService() {
        return service;
    }

    /**
     * Gets the active enclave type.
     *
     * @return the enclave type
     */
    public EnclaveType getActiveEnclaveType() {
        // if not initialized yet, return the default one
        return activeEnclaveType != null ? activeEnclaveType : defaultEnclaveType;
    }
}
