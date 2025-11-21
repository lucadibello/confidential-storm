package ch.usi.inf.confidentialstorm.host.bolts.base;

import ch.usi.inf.confidentialstorm.host.util.EnclaveManager;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.apache.teaclave.javasdk.host.exception.EnclaveDestroyingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class ConfidentialBolt<S> extends BaseRichBolt {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    protected OutputCollector collector;
    private String componentId;
    private int taskId;
    private EnclaveManager<S> enclaveManager;

    protected ConfidentialBolt(Class<S> serviceClass) {
        this(serviceClass, EnclaveType.TEE_SDK);
    }

    protected ConfidentialBolt(Class<S> serviceClass, EnclaveType enclaveType) {
        // initialize enclave manager to streamline enclave and service loading
        this.enclaveManager = new EnclaveManager<>(serviceClass, enclaveType);
    }

    @Override
    public final void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.componentId = context.getThisComponentId();
        this.taskId = context.getThisTaskId();
        this.collector = collector;

        LOG.info("Preparing bolt {} (task {}) with enclave type {}", componentId, taskId);
        try {
            // initialize the enclave via EnclaveManager
            this.enclaveManager.initializeEnclave(topoConf);
            // execute hook for subclasses
            afterPrepare(topoConf, context);
        } catch (RuntimeException e) {
            LOG.error("Failed to prepare bolt {} (task {})", componentId, taskId, e);
            throw e;
        }
    }

    @Override
    public final void execute(Tuple input) {
        try {
            processTuple(input, this.enclaveManager.getService());
        } catch (RuntimeException e) {
            LOG.error("Bolt {} (task {}) failed processing tuple {}", componentId, taskId,
                    summarizeTuple(input), e);
            throw e;
        }
    }

    @Override
    public void cleanup() {
        // run hook for subclasses
        beforeCleanup();

        // destroy the enclave via EnclaveManager
        try {
            this.enclaveManager.destroy();
        } catch (EnclaveDestroyingException e) {
            LOG.error("Failed to destroy enclave for bolt {} (task {})", componentId, taskId, e);
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

    private String summarizeTuple(Tuple input) {
        if (input == null) {
            return "<null>";
        }
        try {
            return String.format("id=%s source=%s/%s fields=%s",
                    input.getMessageId(), input.getSourceComponent(), input.getSourceStreamId(), input.getFields());
        } catch (Exception ignored) {
            return input.toString();
        }
    }
}
