package ch.usi.inf.confidentialstorm.host.util;

import org.apache.teaclave.javasdk.host.Enclave;
import org.apache.teaclave.javasdk.host.EnclaveType;

import org.apache.teaclave.javasdk.host.exception.EnclaveDestroyingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class EnclaveManager<S> {
    private final Class<S> serviceClass;
    private final EnclaveType defaultEnclaveType;
    private static final Logger LOG = LoggerFactory.getLogger(EnclaveManager.class);

    private Enclave enclave;
    // This is done to allow developers to specify the enclave type at runtime via a
    // system property
    private EnclaveType activeEnclaveType;
    private S service;

    public EnclaveManager(Class<S> serviceClass) {
        this(serviceClass, EnclaveType.TEE_SDK);
    }

    public EnclaveManager(Class<S> serviceClass, EnclaveType enclaveType) {
        LOG.info("Creating EnclaveManager for service {}", serviceClass.getName());
        this.serviceClass = serviceClass;
        this.defaultEnclaveType = enclaveType;
    }

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

    public void destroy() {
        if (enclave != null) {
            try {
                enclave.destroy();
            } catch (EnclaveDestroyingException ex) {
                LOG.warn("Failed to destroy enclave of type {}", activeEnclaveType, ex);
            }
        }
    }

    public S getService() {
        return service;
    }

    public EnclaveType getActiveEnclaveType() {
        // if not initialized yet, return the default one
        return activeEnclaveType != null ? activeEnclaveType : defaultEnclaveType;
    }
}
