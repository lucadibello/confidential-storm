package ch.usi.inf.confidentialstorm.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLoggerFactory;

import java.util.Collection;
import java.util.Objects;

public abstract class ConfidentialBoltService<T extends Record> {
    private static final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(ConfidentialBoltService.class);

    public abstract TopologySpecification.Component expectedSourceComponent();
    public abstract TopologySpecification.Component expectedDestinationComponent();

    public abstract Collection<EncryptedValue> valuesToVerify(T request);

    protected void verify(T request) throws SecurityException {
        // extract all critical values from the request
        Collection<EncryptedValue> values = valuesToVerify(request);

        TopologySpecification.Component destination = Objects.requireNonNull(expectedDestinationComponent(),
                "Expected destination component cannot be null");
        TopologySpecification.Component expectedSource = expectedSourceComponent();

        // verify each value
        for (EncryptedValue sealedValue : values) {
            try {
                // NOTE: if the source is null, it means that the value was created outside of ConfidentialStorm
                // hence, verifyRoute would verify only the destination component
                LOG.info("Verifying sealed value: {} from {} to {}", sealedValue, expectedSource, destination);
                SealedPayload.verifyRoute(sealedValue, expectedSource, destination);
            } catch (Exception e) {
                LOG.error("Sealed value verification failed for source {} destination {} value {}: {}",
                        expectedSource, destination, sealedValue, e.getMessage());
                LOG.error("Sealed value verification exception", e);
                throw new SecurityException("Sealed value verification failed", e);
            }
        }
    }
}
