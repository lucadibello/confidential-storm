package ch.usi.inf.confidentialstorm.host.base;

import ch.usi.inf.confidentialstorm.host.util.EnclaveManager;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.apache.teaclave.javasdk.host.exception.EnclaveDestroyingException;

public final class ConfidentialComponentState<C, S> {
    private C collector;
    private String componentId;
    private int taskId;
    private EnclaveManager<S> enclaveManager;
    private final Class<S> serviceClass;
    private final EnclaveType enclaveType;


    public ConfidentialComponentState(Class<S> serviceClass, EnclaveType enclaveType)  {
        this.serviceClass = serviceClass;
        this.enclaveType = enclaveType;
    }

    public void initialize() {
        this.enclaveManager = new EnclaveManager<>(this.serviceClass, this.enclaveType);
    }

    public C getCollector() {
        return collector;
    }

    public void setCollector(C collector) {
        this.collector = collector;
    }

    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public EnclaveManager<S> getEnclaveManager() {
        return enclaveManager;
    }

    public void destroy() throws EnclaveDestroyingException {
        try {
            enclaveManager.destroy();
        } catch (Exception e) {
            throw new EnclaveDestroyingException("Failed to destroy enclave for component " + componentId, e);
        }
    }
}