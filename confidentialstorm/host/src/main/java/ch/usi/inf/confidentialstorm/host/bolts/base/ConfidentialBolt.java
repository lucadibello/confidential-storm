package ch.usi.inf.confidentialstorm.host.bolts.base;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.teaclave.javasdk.host.Enclave;
import org.apache.teaclave.javasdk.host.EnclaveFactory;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.apache.teaclave.javasdk.host.exception.EnclaveCreatingException;
import org.apache.teaclave.javasdk.host.exception.EnclaveDestroyingException;
import org.apache.teaclave.javasdk.host.exception.ServicesLoadingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public abstract class ConfidentialBolt<S> extends BaseRichBolt {
    private final Class<S> serviceClass;
    private final EnclaveType enclaveType;
    private final Logger log = LoggerFactory.getLogger(getClass());

    protected OutputCollector collector;
    private Enclave enclave;
    private S service;

    protected ConfidentialBolt(Class<S> serviceClass) {
        this(serviceClass, EnclaveType.TEE_SDK);
    }

    protected ConfidentialBolt(Class<S> serviceClass, EnclaveType enclaveType) {
        this.serviceClass = serviceClass;
        this.enclaveType = enclaveType;
    }

    @Override
    public final void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.enclave = createEnclave();
        this.service = loadService(enclave);
        afterPrepare(topoConf, context);
    }

    @Override
    public final void execute(Tuple input) {
        processTuple(input, service);
    }

    @Override
    public void cleanup() {
        beforeCleanup();
        if (enclave != null) {
            try {
                enclave.destroy();
            } catch (EnclaveDestroyingException e) {
                log.warn("Failed to destroy enclave cleanly", e);
            }
        }
        super.cleanup();
    }

    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        // hook for subclasses
    }

    protected void beforeCleanup() {
        // hook for subclasses
    }

    protected abstract void processTuple(Tuple input, S service);

    private Enclave createEnclave() {
        try {
            return EnclaveFactory.create(enclaveType);
        } catch (EnclaveCreatingException e) {
            throw new IllegalStateException("Unable to create enclave of type " + enclaveType, e);
        }
    }

    private S loadService(Enclave enclave) {
        try {
            Iterator<S> services = enclave.load(serviceClass);
            if (!services.hasNext()) {
                throw new IllegalStateException("No enclave service registered for " + serviceClass.getName());
            }
            return services.next();
        } catch (ServicesLoadingException e) {
            throw new IllegalStateException("Unable to load enclave service for " + serviceClass.getName(), e);
        }
    }
}
