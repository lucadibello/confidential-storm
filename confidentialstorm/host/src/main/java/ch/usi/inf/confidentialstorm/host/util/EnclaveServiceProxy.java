package ch.usi.inf.confidentialstorm.host.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

/**
 * Proxy that wraps enclave service interfaces to detect fatal enclave failures.
 * <p>
 * By design, enclave service methods return null to indicate fatal errors within the enclave.
 * This proxy intercepts method calls and checks for null return values, throwing an exception
 * if a null is detected, avoiding silent failures.
 * <p>
 * NOTE: as this proxy uses reflection, it may introduce performance overhead. This proxy
 * can be enabled or disabled via the system property `confidentialstorm.enclave.proxy.enable`.
 */
public final class EnclaveServiceProxy {

    /**
     * The logger for this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(EnclaveServiceProxy.class);

    /**
     * Flag to enable or disable the proxy functionality.
     */
    private static final boolean ENABLE_PROXY = Boolean.parseBoolean(
            System.getProperty("confidentialstorm.enclave.proxy.enable", "false")
    );

    private EnclaveServiceProxy() {
        // no instances
    }

    /**
     * Wraps the given target service with a proxy that checks for null return values.
     *
     * @param serviceInterface the EnclaveService interface class to wrap
     * @param target           the target service implementation
     * @param <S>              the type of the service interface
     * @return a proxy instance that checks for null return values
     */
    @SuppressWarnings("unchecked")
    public static <S> S wrap(Class<S> serviceInterface, S target) {
        if (!ENABLE_PROXY) {
            return target;
        }
        Objects.requireNonNull(serviceInterface, "serviceInterface cannot be null");
        Objects.requireNonNull(target, "target service cannot be null");
        if (!serviceInterface.isInterface()) {
            throw new IllegalArgumentException("serviceInterface must be an interface");
        }
        InvocationHandler handler = new NullCheckingHandler<>(target);
        return (S) Proxy.newProxyInstance(
                serviceInterface.getClassLoader(),
                new Class<?>[]{serviceInterface},
                handler
        );
    }

    /**
     * Proxy handler that checks for null return values.
     *
     * @param delegate the target service implementation
     * @param <S>      the type of the service interface
     */
    private record NullCheckingHandler<S>(S delegate) implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // invoke actual service implementation and store result
            Object result = method.invoke(delegate, args);

            // if result is null (and the method was expected to return a value), throw exception
            // to avoid the silent failure
            if (result == null && method.getReturnType() != Void.TYPE) {
                String msg = "Enclave service returned null (fatal enclave error). Method: "
                        + method.getName();
                LOG.error(msg);
                throw new IllegalStateException(msg);
            }

            // return the actual result produced by the enclave service
            return result;
        }
    }
}
