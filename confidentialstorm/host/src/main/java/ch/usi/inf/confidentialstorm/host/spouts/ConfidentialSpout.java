package ch.usi.inf.confidentialstorm.host.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.base.ConfidentialComponentState;
import ch.usi.inf.confidentialstorm.host.profiling.ProfilerConfig;
import ch.usi.inf.confidentialstorm.host.profiling.ProfilerReport;
import ch.usi.inf.confidentialstorm.host.util.EnclaveErrorUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.apache.teaclave.javasdk.host.exception.EnclaveDestroyingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 * Generic confidential spout that loads an enclave service of the caller's choice.
 * Handles enclave lifecycle and error handling for confidential spouts.
 *
 * @param <S> the type of the enclave service used by this spout
 */
public abstract class ConfidentialSpout<S> extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(ConfidentialSpout.class);

    /**
     * Holds the confidential state of the component.
     */
    protected transient ConfidentialComponentState<SpoutOutputCollector, S> state;
    private final Class<S> serviceClass;
    private final EnclaveType enclaveType;

    /** Lifecycle CSV writer for profiler startup/shutdown events. */
    private transient PrintWriter lifecycleCsvWriter;

    /**
     * Constructs a new ConfidentialSpout with default enclave type (TEE_SDK).
     *
     * @param serviceClass the class of the enclave service
     */
    protected ConfidentialSpout(Class<S> serviceClass) {
        this(serviceClass, EnclaveType.TEE_SDK);
    }

    /**
     * Constructs a new ConfidentialSpout with specific enclave type.
     *
     * @param serviceClass the class of the enclave service
     * @param enclaveType  the enclave type to use
     */
    protected ConfidentialSpout(Class<S> serviceClass, EnclaveType enclaveType) {
        this.serviceClass = serviceClass;
        this.enclaveType = enclaveType;
    }

    @Override
    public void open(Map<String, Object> topoConf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
        this.state = new ConfidentialComponentState<>(serviceClass, enclaveType);
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
            // record lifecycle event after all initialization is complete
            writeLifecycleEvent("COMPONENT_STARTED");
        } catch (Throwable e) {
            LOG.error("Failed to prepare spout {} (task {})",
                    state.getComponentId(), state.getTaskId(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Hook method called after spout opening.
     *
     * @param conf      the topology configuration
     * @param context   the topology context
     * @param collector the spout output collector
     */
    protected void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        // hook for subclass
    }

    @Override
    public void close() {
        // record lifecycle event before any cleanup
        writeLifecycleEvent("COMPONENT_STOPPING");

        // run hook for subclasses
        beforeClose();

        // destroy the confidential component state
        try {
            if (this.state != null) {
                this.state.destroy();
            }
        } catch (EnclaveDestroyingException e) {
            LOG.error("Failed to destroy enclave for spout {} (task {})",
                    this.state.getComponentId(), this.state.getTaskId(), e);
        }

        // close lifecycle CSV writer
        if (lifecycleCsvWriter != null) {
            lifecycleCsvWriter.close();
            lifecycleCsvWriter = null;
        }
    }

    /**
     * Hook method called before spout closing.
     */
    protected void beforeClose() {
        // hook for subclass
    }

    /**
     * Writes a lifecycle event to the profiler CSV and log.
     * No-op when profiling is disabled (compile-time constant).
     */
    private void writeLifecycleEvent(String eventName) {
        if (!ProfilerConfig.ENABLED || state == null) return;

        String componentId = state.getComponentId();
        int taskId = state.getTaskId();

        LOG.info(ProfilerReport.toLifecycleLogLine(componentId, taskId, eventName));

        if (lifecycleCsvWriter == null) {
            File dir = new File(ProfilerConfig.OUTPUT_DIR);
            if (!dir.exists() && !dir.mkdirs()) {
                LOG.warn("[PROFILER] Failed to create output directory: {}", dir.getAbsolutePath());
                return;
            }
            File file = new File(dir, String.format("profiler-%s-task%d.csv", componentId, taskId));
            try {
                boolean needsHeader = !file.exists() || file.length() == 0;
                lifecycleCsvWriter = new PrintWriter(new FileWriter(file, true));
                if (needsHeader) {
                    ProfilerReport.writeCsvHeader(lifecycleCsvWriter);
                    lifecycleCsvWriter.flush();
                }
            } catch (IOException e) {
                LOG.warn("[PROFILER] Failed to open lifecycle CSV: {}", file.getAbsolutePath(), e);
                return;
            }
        }
        ProfilerReport.writeLifecycleCsvRow(lifecycleCsvWriter, componentId, taskId, eventName);
    }

    /**
     * Gets the enclave service instance.
     *
     * @return the service
     */
    protected S getService() {
        return state.getEnclaveManager().getService();
    }

    /**
     * Gets the Storm spout output collector.
     *
     * @return the collector
     */
    protected SpoutOutputCollector getCollector() {
        return state.getCollector();
    }

    /**
     * Gets the component ID.
     *
     * @return the component ID
     */
    protected String getComponentId() {
        return state.getComponentId();
    }

    @Override
    public void nextTuple() {
        try {
            // call hook for subclass
            executeNextTuple();
        } catch (Throwable e) {
            Throwable root = EnclaveErrorUtils.unwrap(e);
            LOG.error("Error in nextTuple of spout {} (task {})",
                    state.getComponentId(), state.getTaskId(), e);
            try {
                state.getCollector().reportError(root);
            } catch (Throwable reportingError) {
                LOG.error("Failed to report error from spout {} (task {})",
                        state.getComponentId(), state.getTaskId(), reportingError);
            }

            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Map.of();
    }

    /**
     * Abstract method to be implemented by subclasses to emit the next tuple.
     *
     * @throws EnclaveServiceException if an error occurs inside the enclave
     */
    protected abstract void executeNextTuple() throws EnclaveServiceException;
}
