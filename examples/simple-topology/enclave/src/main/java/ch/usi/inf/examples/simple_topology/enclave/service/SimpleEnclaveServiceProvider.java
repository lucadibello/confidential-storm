package ch.usi.inf.examples.simple_topology.enclave.service;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.enclave.exception.EnclaveExceptionContext;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.simple_topology.common.api.SimpleEnclaveService;
import ch.usi.inf.examples.simple_topology.common.api.model.SimpleEnclaveRequest;
import ch.usi.inf.examples.simple_topology.common.api.model.SimpleEnclaveResponse;
import com.google.auto.service.AutoService;

@AutoService(SimpleEnclaveService.class)
public class SimpleEnclaveServiceProvider implements SimpleEnclaveService {

    protected final EnclaveExceptionContext exceptionCtx =
        EnclaveExceptionContext.getInstance();

    private final EnclaveLogger log;

    public SimpleEnclaveServiceProvider() {
        this.log = EnclaveLoggerFactory.getLogger(
            SimpleEnclaveServiceProvider.class
        );
    }

    @Override
    public SimpleEnclaveResponse query(SimpleEnclaveRequest request)
        throws EnclaveServiceException {
        try {
            String response = new StringBuilder(request.message())
                .reverse()
                .toString();
            log.info("Received message: {}", request.message());
            return new SimpleEnclaveResponse(response);
        } catch (Throwable ex) {
            exceptionCtx.handleException(ex);
            return null;
        }
    }
}
