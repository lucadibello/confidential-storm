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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public abstract class ConfidentialBolt<S> extends BaseRichBolt {
    private static final String ENCLAVE_TYPE_CONF_KEY = "confidentialstorm.enclave.type";

    private final Class<S> serviceClass;
    private final EnclaveType defaultEnclaveType;
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    protected OutputCollector collector;
    private Enclave enclave;
    private S service;
    private EnclaveType activeEnclaveType;
    private String componentId;
    private int taskId;

    protected ConfidentialBolt(Class<S> serviceClass) {
        this(serviceClass, EnclaveType.TEE_SDK);
    }

    protected ConfidentialBolt(Class<S> serviceClass, EnclaveType enclaveType) {
        this.serviceClass = serviceClass;
        this.defaultEnclaveType = enclaveType;
    }

    @Override
    public final void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.activeEnclaveType = resolveEnclaveType(topoConf);
        this.componentId = context.getThisComponentId();
        this.taskId = context.getThisTaskId();
        LOG.info("Preparing bolt {} (task {}) with enclave type {}", componentId, taskId, activeEnclaveType);
        try {
            this.collector = collector;
            this.enclave = createEnclave(activeEnclaveType);
            this.service = loadService(enclave);
            LOG.info("Bolt {} (task {}) initialized enclave service {}", componentId, taskId,
                    serviceClass.getSimpleName());
            afterPrepare(topoConf, context);
        } catch (RuntimeException e) {
            LOG.error("Failed to prepare bolt {} (task {})", componentId, taskId, e);
            throw e;
        }
    }

    @Override
    public final void execute(Tuple input) {
        try {
            processTuple(input, service);
        } catch (RuntimeException e) {
            LOG.error("Bolt {} (task {}) failed processing tuple {}", componentId, taskId,
                    summarizeTuple(input), e);
            throw e;
        }
    }

    @Override
    public void cleanup() {
        beforeCleanup();
        if (enclave != null) {
            try {
                enclave.destroy();
            } catch (EnclaveDestroyingException e) {
                LOG.warn("Failed to destroy enclave cleanly", e);
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

    private Enclave createEnclave(EnclaveType enclaveType) {
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

    private EnclaveType resolveEnclaveType(Map<String, Object> topoConf) {
        String override = extractOverride(topoConf);
        if (override == null) {
            return defaultEnclaveType;
        }
        try {
            return EnclaveType.valueOf(override.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            LOG.warn("Unknown enclave type '{}' for {}, falling back to {}", override, serviceClass.getSimpleName(),
                    defaultEnclaveType);
            return defaultEnclaveType;
        }
    }

    private String extractOverride(Map<String, Object> topoConf) {
        Object confValue = topoConf != null ? topoConf.get(ENCLAVE_TYPE_CONF_KEY) : null;
        if (confValue instanceof EnclaveType type) {
            return type.name();
        }
        if (confValue != null) {
            return Objects.toString(confValue, null);
        }
        String sys = System.getProperty(ENCLAVE_TYPE_CONF_KEY);
        if (sys != null && !sys.isBlank()) {
            return sys;
        }
        String envKey = ENCLAVE_TYPE_CONF_KEY.replace('.', '_').toUpperCase(Locale.ROOT);
        String env = System.getenv(envKey);
        if (env != null && !env.isBlank()) {
            return env;
        }
        return null;
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
