package ch.usi.inf.confidentialstorm.host.spouts.base;

import ch.usi.inf.confidentialstorm.common.api.SpoutMapperService;
import ch.usi.inf.confidentialstorm.host.base.ConfidentialComponentState;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.apache.teaclave.javasdk.host.exception.EnclaveDestroyingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class ConfidentialSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(ConfidentialSpout.class);

    protected transient ConfidentialComponentState<SpoutOutputCollector, SpoutMapperService> state;

    public ConfidentialSpout() {
        LOG.info("Creating Confidential Spout");
    }

    @Override
    public void open(Map<String, Object> topoConf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
        this.state = new ConfidentialComponentState<>(
                SpoutMapperService.class,
                EnclaveType.TEE_SDK
        );
        state.initialize();
        LOG.info("Opening Confidential Spout");
        state.setComponentId(context.getThisComponentId());
        state.setTaskId(context.getThisTaskId());
        state.setCollector(spoutOutputCollector);

        LOG.info("Preparing spout {} (task {}) with enclave type {}",
                state.getComponentId(), state.getTaskId(), state.getEnclaveManager().getActiveEnclaveType());
        try {
            LOG.debug("Attempting to initialize enclave for spout {} (task {})", state.getComponentId(), state.getTaskId());
            state.getEnclaveManager().initializeEnclave(topoConf);
            LOG.debug("Successfully initialized enclave for spout {} (task {})", state.getComponentId(), state.getTaskId());
            // execute hook for subclasses
            afterOpen(topoConf, context, spoutOutputCollector);
        } catch (Throwable e) {
            LOG.error("Failed to prepare spout {} (task {})",
                    state.getComponentId(), state.getTaskId(), e);
            throw new RuntimeException(e);
        }
    }

    protected void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        // hook for subclass
    }

    @Override
    public void close() {
        // run hook for subclasses
        beforeClose();

        // destroy the confidential component state
        try {
            this.state.destroy();
        } catch (EnclaveDestroyingException e) {
            LOG.error("Failed to destroy enclave for bolt {} (task {})",
                    this.state.getComponentId(), this.state.getTaskId(), e);
        }
    }

    protected void beforeClose() {
        // hook for subclass
    }

    protected SpoutMapperService getMapperService() {
        return state.getEnclaveManager().getService();
    }

    protected SpoutOutputCollector getCollector() {
        return state.getCollector();
    }

    protected String getComponentId() {
        return state.getComponentId();
    }

    @Override
    public void nextTuple() {
        try {
            // call hook for subclass
            executeNextTuple();
        } catch (Throwable e) {
            LOG.error("Error in nextTuple of spout {} (task {})",
                    state.getComponentId(), state.getTaskId(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Map.of();
    }

    protected abstract void executeNextTuple();
}
