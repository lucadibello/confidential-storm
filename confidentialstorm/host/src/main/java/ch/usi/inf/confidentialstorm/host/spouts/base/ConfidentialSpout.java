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

public abstract class ConfidentialSpout extends BaseRichSpout  {

    private static final Logger LOG = LoggerFactory.getLogger(ConfidentialSpout.class);

    protected final ConfidentialComponentState<SpoutOutputCollector, SpoutMapperService> state;

    public ConfidentialSpout() {
        this.state = new ConfidentialComponentState<>(SpoutMapperService.class, EnclaveType.TEE_SDK);
    }

    @Override
    public void open(Map<String, Object> topoConf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
        state.setComponentId(context.getThisComponentId());
        state.setTaskId(context.getThisTaskId());
        state.setCollector(spoutOutputCollector);

        LOG.info("Preparing bolt {} (task {}) with enclave type {}",
                state.getComponentId(), state.getTaskId(), state.getEnclaveManager().getActiveEnclaveType());
        try {
            // initialize the enclave via EnclaveManager
            state.getEnclaveManager().initializeEnclave(topoConf);
            // execute hook for subclasses
            afterOpen(topoConf, context); // hook for subclass
        } catch (RuntimeException e) {
            LOG.error("Failed to prepare spout {} (task {})",
                    state.getComponentId(), state.getTaskId(), e);
            throw e;
        }
    }

    protected abstract void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector);

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

        super.close();
    }

    protected void afterOpen(Map<String, Object> topoConf, TopologyContext context) {
        // hook for subclass
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

    @Override
    public void nextTuple() {
        try {
            // call hook for subclass
            executeNextTuple();
        } catch (RuntimeException e) {
            LOG.error("Error in nextTuple of spout {} (task {})",
                    state.getComponentId(), state.getTaskId(), e);
            throw e;
        }
    }

    protected abstract void executeNextTuple();
}
