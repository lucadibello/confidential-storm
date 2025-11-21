package ch.usi.inf.confidentialstorm.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;

import java.util.Collection;
import java.util.Objects;

public abstract class ConfidentialBoltService<T extends Record> {

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
                System.out.println("Verifying sealed value: " + sealedValue + " from " + expectedSource + " to " + destination);
                SealedPayload.verifyRoute(sealedValue, expectedSource, destination);
            } catch (Exception e) {
                System.err.println("Sealed value verification failed: " + e.getMessage());
                System.err.println("Sealed value: " + sealedValue);
                System.err.println("Expected source: " + expectedSource);
                System.err.println("Expected destination: " + destination);
                // print stack trace for debugging
                e.printStackTrace();
                throw new SecurityException("Sealed value verification failed", e);
            }
        }
    }
}
