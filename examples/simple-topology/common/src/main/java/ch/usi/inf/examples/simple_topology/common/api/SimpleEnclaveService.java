package ch.usi.inf.examples.simple_topology.common.api;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.examples.simple_topology.common.api.model.SimpleEnclaveRequest;
import ch.usi.inf.examples.simple_topology.common.api.model.SimpleEnclaveResponse;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface SimpleEnclaveService {
    SimpleEnclaveResponse query(SimpleEnclaveRequest request) throws EnclaveServiceException;
}
