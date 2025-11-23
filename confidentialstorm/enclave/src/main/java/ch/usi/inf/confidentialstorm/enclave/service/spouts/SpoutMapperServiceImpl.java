package ch.usi.inf.confidentialstorm.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.api.SpoutMapperService;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.model.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLoggerFactory;
import com.google.auto.service.AutoService;

import java.util.Objects;

@AutoService(SpoutMapperService.class)
public class SpoutMapperServiceImpl implements SpoutMapperService {
    private static final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(SpoutMapperServiceImpl.class);

    @Override
    public EncryptedValue setupRoute(TopologySpecification.Component component, EncryptedValue entry) {
        Objects.requireNonNull(component, "component cannot be null");
        Objects.requireNonNull(entry, "Encrypted entry cannot be null");

        // we want to verify that the entry is correctly sealed
        SealedPayload.verifyRoute(entry,
                TopologySpecification.Component.DATASET,
                TopologySpecification.Component.MAPPER);

        // get string body
        byte[] body = SealedPayload.decrypt(entry);

        TopologySpecification.Component downstreamComponent = TopologySpecification.requireSingleDownstream(component);

        // create new AAD with correct route names
        AADSpecification aad = AADSpecification.builder()
                .sourceComponent(component)
                .destinationComponent(downstreamComponent)
                .build();

        // seal again with new AAD routing information + return sealed entry
        return SealedPayload.encrypt(body, aad);
    }
}
