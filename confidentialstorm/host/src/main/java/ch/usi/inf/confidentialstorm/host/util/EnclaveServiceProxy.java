package ch.usi.inf.confidentialstorm.host.util;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.exceptions.EnclaveInvocationFailure;
import org.apache.teaclave.javasdk.common.EnclaveInvocationContext;
import org.apache.teaclave.javasdk.common.EnclaveInvocationResult;
import org.apache.teaclave.javasdk.common.ServiceHandler;
import org.slf4j.Logger;

import java.lang.reflect.*;
import java.util.Objects;

/**
 * Dynamic proxy that wraps enclave services to enforce null checks and
 * unwrap/propagate enclave exceptions automatically for all service interfaces.
 */
final class EnclaveServiceProxy {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(EnclaveServiceProxy.class);
    private static final String TEACLAVE_PROXY_HANDLER = "org.apache.teaclave.javasdk.host.ProxyEnclaveInvocationHandler";
    private static final boolean PROPAGATE_EXCEPTIONS = Boolean.parseBoolean(System.getProperty("confidentialstorm.debug.exceptions.enabled", "false"));

    private EnclaveServiceProxy() { }

    /**
     * Wraps the given delegate with a dynamic proxy that enforces null return checks
     * @param serviceClass the service interface class
     * @param delegate the delegate instance
     * @return the wrapped proxy instance
     * @param <T> the service interface type
     */
    @SuppressWarnings("unchecked")
    public static <T> T wrap(Class<T> serviceClass, T delegate) {
        Objects.requireNonNull(serviceClass, "serviceClass cannot be null");
        Objects.requireNonNull(delegate, "delegate cannot be null");

        // wrap the delegate with a dynamic proxy that adds null checks and exception propagation
        InvocationHandler handler = (proxy, method, args) -> invoke(serviceClass, delegate, method, args);

        // create and return the proxy instance for the service interface
        // NOTE: seamlessly forwards all method calls to the invocation handler
        return (T) Proxy.newProxyInstance(
                serviceClass.getClassLoader(),
                new Class<?>[]{serviceClass},
                handler
        );
    }

    /**
     * Reimplementation the Teaclave Host SDK's invocation handler to add null checks and
     * improved exception propagation.
     *
     * @param serviceClass Service interface class
     * @param delegate The delegate instance
     * @param method The invoked method
     * @param args The method arguments
     * @return The method result
     * @throws Throwable if the invocation fails, throws the original exception (from within the enclave boundary)
     * or wraps it in EnclaveInvocationFailure
     */
    private static Object invoke(Class<?> serviceClass, Object delegate, Method method, Object[] args) throws Throwable {
        String operation = serviceClass.getSimpleName() + "." + method.getName();
        try {
            Object result = invokeWithExceptionPropagation(delegate, method, args, operation);
            if (method.getReturnType() != void.class && method.getReturnType() != Void.class && result == null) {
                throw new EnclaveInvocationFailure("Enclave returned null for operation '" + operation
                        + "'. This often indicates a serialization or enclave-side exception. "
                        + "Ensure the return type and exceptions are serializable and registered.");
            }
            return result;
        } catch (Throwable e) {
            Throwable root = EnclaveErrorUtils.unwrap(e);
            if (root instanceof RuntimeException && !(root instanceof EnclaveInvocationFailure)) {
                // propagate enclave-provided runtime exceptions directly
                throw root;
            }
            throw new EnclaveInvocationFailure("Enclave operation '" + operation + "' failed: "
                    + EnclaveErrorUtils.format(root), root);
        }
    }

    /**
     * The Teaclave Java SDK v0.1.0 {@code ProxyEnclaveInvocationHandler} drops enclave-side exceptions
     * unless they are declared on the service interface. This is problematic for runtime exceptions like
     * {@link NullPointerException} or {@link IllegalArgumentException} which are common in Java but not
     * *typically* declared.
     *
     * This means that runtime exceptions surface as {@code null} results on the host. To avoid
     * silent failures, invoke the enclave directly via reflection to retrieve the full
     * {@link EnclaveInvocationResult} and rethrow any remote exception.
     *
     * NOTE: This code is largely based on the Teaclave Java SDK v0.1.0's implementation of ProxyEnclaveInvocationHandler.
     *      It uses reflection to access internal fields and methods of the Teaclave SDK.
     *
     *      Reference: <a href="https://github.com/apache/teaclave-java-tee-sdk/blob/master/sdk/host/src/main/java/org/apache/teaclave/javasdk/host/ProxyEnclaveInvocationHandler.java">
     *          ProxyEnclaveInvocationHandler.java</a>
     */
    private static Object invokeWithExceptionPropagation(Object delegate, Method method, Object[] args, String operation) throws Throwable {
        // invoke the enclave directly to capture any remote exceptions
        EnclaveInvocationResult directResult = tryInvokeDirect(delegate, method, args);

        // ensure we got a direct result before falling back to the original invocation
        if (directResult != null) {
            // check for remote exception and rethrow if present
            Throwable remote = directResult.getException();
            if (remote != null) {
                LOG.debug("Enclave operation '{}' resulted in remote exception: {}", operation, EnclaveErrorUtils.format(remote));

                Throwable root = EnclaveErrorUtils.unwrap(remote);
                if (root instanceof EnclaveServiceException ese) {
                    throw ese;
                }

                // NOTE: for security purposes, we only propagate full exception details in debug mode
                // otherwise we may leak sensitive enclave information via exception messages or stack traces
                // in log files or error reports.

                if (PROPAGATE_EXCEPTIONS) {
                    // Debug mode: propagate full exception details
                    LOG.debug("Wrapping remote exception from enclave operation '{}' for propagation (debug mode): {}", operation, EnclaveErrorUtils.format(root));
                    EnclaveServiceException wrapped = new EnclaveServiceException(
                            operation,
                            root.getClass().getName(),
                            root.getMessage(),
                            root.getStackTrace()
                    );
                    wrapped.initCause(root);
                    throw wrapped;
                } else {
                    // Production mode: log details internally, throw a generic exception
                    LOG.error("Enclave operation '{}' failed with an internal exception. Full exception details will not be propagated.", operation, root);
                    throw new EnclaveServiceException(
                            operation,
                            "Internal enclave error", // Generic type
                            "An internal error occurred within the enclave.", // Generic message
                            null // NO stack trace
                    );
                }
            }
            // return the direct result if no exception occurred
            LOG.debug("Enclave operation '{}' completed successfully via direct invocation.", operation);
            return directResult.getResult();
        }

        // notify about fallback to original invocation (may lose enclave exceptions)
        LOG.warn("Falling back to original service invocation for operation '{}'. "
                + "This may lead to lost enclave exceptions if runtime exceptions are thrown.", operation);

        // NOTE: fallback to original service invocation if direct access is not possible
        return method.invoke(delegate, args);
    }

    private static EnclaveInvocationResult tryInvokeDirect(Object delegate, Method method, Object[] args) throws Throwable {
        if (!Proxy.isProxyClass(delegate.getClass())) {
            return null;
        }
        InvocationHandler handler = Proxy.getInvocationHandler(delegate);
        if (!TEACLAVE_PROXY_HANDLER.equals(handler.getClass().getName())) {
            // NOTE: if the handler is not the expected Teaclave proxy handler, we cannot proceed
            // thus we return null to indicate that direct invocation is not possible
            return null;
        }

        // access internal ServiceHandler/AbstractEnclave objects from ProxyEnclaveInvocationHandler via reflection
        ServiceHandler serviceHandler = (ServiceHandler) readField(handler, "serviceHandler");
        // NOTE: AbstractEnclave is not accessible directly (it's package-private), so we read it as Object
        // Refer to: <https://github.com/apache/teaclave-java-tee-sdk/blob/master/sdk/host/src/main/java/org/apache/teaclave/javasdk/host/AbstractEnclave.java>
        Object enclave = readField(handler, "enclave"); // NOTE: AbstractEnclave is not accessible directly
        if (serviceHandler == null || enclave == null) {
            throw new EnclaveInvocationFailure("Unable to access enclave proxy internals to propagate enclave exceptions.");
        }

        // build invocation context (same logic as in Teaclave SDK)
        String[] parameterTypes = args != null ? buildParameterTypeNames(method) : null;
        EnclaveInvocationContext context = args != null
                ? new EnclaveInvocationContext(serviceHandler, method.getName(), parameterTypes, args)
                : new EnclaveInvocationContext(serviceHandler, method.getName(), null, null);

        // invoke the enclave's InvokeEnclaveMethod directly via reflection
        Method invokeEnclaveMethod = findInvokeMethod(enclave.getClass());
        if (invokeEnclaveMethod == null) {
            throw new EnclaveInvocationFailure("Failed to locate enclave invocation entrypoint via reflection.");
        }

        // perform the invocation and return the result
        try {
            return (EnclaveInvocationResult) invokeEnclaveMethod.invoke(enclave, context);
        } catch (InvocationTargetException ite) {
            // unwrap and rethrow any invocation target exceptions
            throw EnclaveErrorUtils.unwrap(ite);
        }
    }

    /**
     * Utility method to read a private field via reflection
     * @param target the target object
     * @param fieldName the field name
     * @return the field value or null if not found
     */
    private static Object readField(Object target, String fieldName) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (ReflectiveOperationException e) {
            return null;
        }
    }

    /**
     * Utility method to find the InvokeEnclaveMethod in the given class or its superclasses
     * @param enclaveClass the enclave class to search
     * @return the Method instance or null if not found
     */
    private static Method findInvokeMethod(Class<?> enclaveClass) {
        Class<?> current = enclaveClass;
        while (current != null) {
            try {
                Method method = current.getDeclaredMethod("InvokeEnclaveMethod", EnclaveInvocationContext.class);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        return null;
    }

    /**
     * Utility method to build parameter type names for a method
     * @param method the method to analyze
     * @return an array of parameter type names as strings
     */
    private static String[] buildParameterTypeNames(Method method) {
        Class<?>[] params = method.getParameterTypes();
        String[] names = new String[params.length];
        for (int i = 0; i < params.length; i++) {
            names[i] = params[i].getName();
        }
        return names;
    }

    /**
     * Utility method to render a throwable's stack trace as a string
     * @param throwable the throwable to render
     * @return the stack trace as a string
     */
    private static String renderStackTrace(Throwable throwable) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement ste : throwable.getStackTrace()) {
            sb.append(ste).append(System.lineSeparator());
        }
        return sb.toString();
    }
}
