package ch.usi.inf.confidentialstorm.host.base;

import ch.usi.inf.confidentialstorm.host.util.EnclaveManager;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.apache.teaclave.javasdk.host.exception.EnclaveDestroyingException;

/**
 * Holds the state for a confidential Storm component (Spout or Bolt).
 * This class manages the enclave lifecycle and Storm-specific identifiers.
 *
 * @param <C> the type of the Storm collector (OutputCollector or SpoutOutputCollector)
 * @param <S> the type of the Enclave service
 */
public final class ConfidentialComponentState<C, S> {
    private final Class<S> serviceClass;
    private final EnclaveType enclaveType;
    private C collector;
    private String componentId;
    private int taskId;
    private EnclaveManager<S> enclaveManager;


    /**
     * Constructs a new ConfidentialComponentState.
     *
     * @param serviceClass the class of the enclave service
     * @param enclaveType  the type of enclave to use
     */
    public ConfidentialComponentState(Class<S> serviceClass, EnclaveType enclaveType) {
        this.serviceClass = serviceClass;
        this.enclaveType = enclaveType;
    }

    /**
     * Initializes the enclave manager.
     */
    public void initialize() {
        this.enclaveManager = new EnclaveManager<>(this.serviceClass, this.enclaveType);
    }

    /**
     * Gets the Storm output collector.
     *
     * @return the collector
     */
    public C getCollector() {
        return collector;
    }

    /**
     * Sets the Storm output collector.
     *
     * @param collector the collector to set
     */
    public void setCollector(C collector) {
        this.collector = collector;
    }

    /**
     * Gets the Storm component ID.
     *
     * @return the component ID
     */
    public String getComponentId() {
        return componentId;
    }

    /**
     * Sets the Storm component ID.
     *
     * @param componentId the component ID to set
     */
    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    /**
     * Gets the Storm task ID.
     *
     * @return the task ID
     */
    public int getTaskId() {
        return taskId;
    }

    /**
     * Sets the Storm task ID.
     *
     * @param taskId the task ID to set
     */
    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    /**
     * Gets the enclave manager.
     *
     * @return the enclave manager
     */
    public EnclaveManager<S> getEnclaveManager() {
        return enclaveManager;
    }

    /**
     * Destroys the enclave and associated resources.
     *
     * @throws EnclaveDestroyingException if destruction fails
     */
    public void destroy() throws EnclaveDestroyingException {
        try {
            enclaveManager.destroy();
        } catch (Exception e) {
            throw new EnclaveDestroyingException("Failed to destroy enclave for component " + componentId, e);
        }
    }
}
