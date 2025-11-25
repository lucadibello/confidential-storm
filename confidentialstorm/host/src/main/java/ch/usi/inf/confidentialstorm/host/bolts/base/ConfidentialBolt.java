package ch.usi.inf.confidentialstorm.host.bolts.base;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.base.ConfidentialComponentState;
import ch.usi.inf.confidentialstorm.host.util.EnclaveErrorUtils;
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

    private final Class<S> serviceClass;
    private final EnclaveType enclaveType;
    protected transient ConfidentialComponentState<OutputCollector, S> state;

    protected ConfidentialBolt(Class<S> serviceClass) {
        this(serviceClass, EnclaveType.TEE_SDK);
    }

    protected ConfidentialBolt(Class<S> serviceClass, EnclaveType enclaveType) {
        this.serviceClass = serviceClass;
        this.enclaveType = enclaveType;
    }

    @Override
    public final void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.state = new ConfidentialComponentState<>(serviceClass, enclaveType);
        state.initialize();
        state.setCollector(collector);
        state.setComponentId(context.getThisComponentId());
        state.setTaskId(context.getThisTaskId());

        LOG.info("Preparing bolt {} (task {}) with enclave type {}",
                state.getComponentId(), state.getTaskId(), state.getEnclaveManager().getActiveEnclaveType());
        try {
            LOG.debug("Attempting to initialize enclave for bolt {} (task {})", state.getComponentId(), state.getTaskId());
            state.getEnclaveManager().initializeEnclave(topoConf);
            LOG.debug("Successfully initialized enclave for bolt {} (task {})", state.getComponentId(), state.getTaskId());
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
            Throwable root = EnclaveErrorUtils.unwrap(e);
            LOG.error("Bolt {} (task {}) failed processing tuple {} due to enclave error {}",
                    state.getComponentId(), state.getTaskId(),
                    summarizeTuple(input), EnclaveErrorUtils.format(root), root);
            try {
                state.getCollector().reportError(root);
                state.getCollector().fail(input);
            } catch (Throwable reportingError) {
                LOG.warn("Failed to report error for tuple {}", summarizeTuple(input), reportingError);
            }
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

    protected abstract void processTuple(Tuple input, S service) throws EnclaveServiceException;

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
