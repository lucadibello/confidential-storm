package ch.usi.inf.confidentialstorm.host.bolts.base;

import ch.usi.inf.confidentialstorm.host.base.ConfidentialComponentState;
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

    private static final Logger LOG = LoggerFactory.getLogger(ConfidentialBolt.class);

    protected final ConfidentialComponentState<OutputCollector, S> state;

    protected ConfidentialBolt(Class<S> serviceClass) {
        this(serviceClass, EnclaveType.TEE_SDK);
    }

    protected ConfidentialBolt(Class<S> serviceClass, EnclaveType enclaveType) {
        this.state = new ConfidentialComponentState<>(serviceClass, enclaveType);
    }

    @Override
    public final void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        state.setCollector(collector);
        state.setComponentId(context.getThisComponentId());
        state.setTaskId(context.getThisTaskId());

        LOG.info("Preparing bolt {} (task {}) with enclave type {}",
                state.getComponentId(), state.getTaskId(), state.getEnclaveManager().getActiveEnclaveType());
        try {
            // initialize the enclave via EnclaveManager
            state.getEnclaveManager().initializeEnclave(topoConf);
            // execute hook for subclasses
            afterPrepare(topoConf, context);
        } catch (Throwable e) {
            LOG.error("Failed to prepare bolt {} (task {})",
                    state.getComponentId(), state.getTaskId(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public final void execute(Tuple input) {
        try {
            processTuple(input, state.getEnclaveManager().getService());
        } catch (Throwable e) {
            LOG.error("Bolt {} (task {}) failed processing tuple {}",
                    state.getComponentId(), state.getTaskId(),
                    summarizeTuple(input), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        // run hook for subclasses
        beforeCleanup();

        // destroy the enclave via EnclaveManager
        try {
            state.destroy();
        } catch (EnclaveDestroyingException e) {
            LOG.error("Failed to destroy enclave for bolt {} (task {})",
                    state.getComponentId(), state.getTaskId(), e);
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

    protected OutputCollector getCollector() {
        return state.getCollector();
    }

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
