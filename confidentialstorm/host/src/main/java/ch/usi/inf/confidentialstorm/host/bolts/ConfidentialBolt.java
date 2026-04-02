package ch.usi.inf.confidentialstorm.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.base.ConfidentialComponentState;
import ch.usi.inf.confidentialstorm.host.profiling.BoltProfiler;
import ch.usi.inf.confidentialstorm.host.profiling.ProfilerConfig;
import ch.usi.inf.confidentialstorm.host.util.EnclaveErrorUtils;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.apache.teaclave.javasdk.host.exception.EnclaveDestroyingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Base class for all confidential Storm bolts.
 * This class handles enclave initialization, tuple processing delegation,
 * and error handling.
 *
 * @param <S> the type of the enclave service used by this bolt
 */
public abstract class ConfidentialBolt<S> extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ConfidentialBolt.class);

    private final Class<S> serviceClass;
    private final EnclaveType enclaveType;
    /**
     * Holds the confidential state of the component.
     */
    protected transient ConfidentialComponentState<OutputCollector, S> state;

    /**
     * Profiler instance, created during {@code prepare()} when {@link ProfilerConfig#ENABLED} is true.
     * Subclasses access this via {@link #getProfiler()} to record ECALL timings, counters, and gauges.
     */
    private transient BoltProfiler profiler;

    /**
     * Constructs a new ConfidentialBolt with default enclave type (TEE_SDK).
     *
     * @param serviceClass the class of the enclave service
     */
    protected ConfidentialBolt(Class<S> serviceClass) {
        this(serviceClass, EnclaveType.TEE_SDK);
    }

    /**
     * Constructs a new ConfidentialBolt with specific enclave type.
     *
     * @param serviceClass the class of the enclave service
     * @param enclaveType  the enclave type to use
     */
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

            // log lifecycle event before/after initialization in order to capture enclave initialization time in profiler
            if (ProfilerConfig.ENABLED && profiler != null) {
                profiler.recordLifecycleEvent(ConfidentialBoltLifecycleEvent.COMPONENT_INITIALIZING_ENCLAVE);
            }
            state.getEnclaveManager().initializeEnclave(topoConf);
            if (ProfilerConfig.ENABLED && profiler != null) {
                profiler.recordLifecycleEvent(ConfidentialBoltLifecycleEvent.COMPONENT_ENCLAVE_INITIALIZED);
            }
            LOG.debug("Successfully initialized enclave for bolt {} (task {})", state.getComponentId(), state.getTaskId());
            // initialize profiler if enabled
            if (ProfilerConfig.ENABLED) {
                this.profiler = new BoltProfiler(state.getComponentId(), state.getTaskId());
            }
            // execute hook for subclasses
            afterPrepare(topoConf, context);
            // record lifecycle event after all initialization is complete
            if (ProfilerConfig.ENABLED && profiler != null) {
                profiler.recordLifecycleEvent(ConfidentialBoltLifecycleEvent.COMPONENT_STARTED);
            }
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
        // record lifecycle event before any cleanup
        if (ProfilerConfig.ENABLED && profiler != null) {
            profiler.recordLifecycleEvent(ConfidentialBoltLifecycleEvent.COMPONENT_STOPPING);
        }

        // run hook for subclasses
        beforeCleanup();

        // write profiler report before enclave destruction
        if (ProfilerConfig.ENABLED && profiler != null) {
            profiler.writeReport();
        }

        // destroy the enclave via EnclaveManager
        try {
            state.destroy();
        } catch (EnclaveDestroyingException e) {
            LOG.error("Failed to destroy enclave for bolt {} (task {})",
                    state.getComponentId(), state.getTaskId(), e);
        }

        super.cleanup();
    }

    /**
     * Hook method called after bolt preparation.
     *
     * @param topoConf the topology configuration
     * @param context  the topology context
     */
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        // hook for subclasses
    }

    /**
     * Hook method called before bolt cleanup.
     */
    protected void beforeCleanup() {
        // hook for subclasses
    }

    /**
     * Checks if a tuple is a system tick tuple.
     *
     * @param tuple the tuple to check
     * @return true if it is a tick tuple, false otherwise
     */
    protected boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    /**
     * Abstract method to process a tuple using the enclave service.
     *
     * @param input   the input tuple
     * @param service the enclave service
     * @throws EnclaveServiceException if an error occurs inside the enclave
     */
    protected abstract void processTuple(Tuple input, S service) throws EnclaveServiceException;

    /**
     * Gets the Storm output collector.
     *
     * @return the collector
     */
    protected OutputCollector getCollector() {
        return state.getCollector();
    }

    protected int getTaskId() {
        return state.getTaskId();
    }

    /**
     * Returns the profiler instance, or {@code null} if profiling is disabled.
     * Subclasses use this to record ECALL timings, counters, and gauges.
     */
    protected BoltProfiler getProfiler() {
        return profiler;
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
