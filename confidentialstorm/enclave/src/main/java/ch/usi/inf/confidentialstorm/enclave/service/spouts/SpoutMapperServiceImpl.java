package ch.usi.inf.confidentialstorm.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.api.SpoutMapperService;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.model.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import com.google.auto.service.AutoService;

import java.util.Objects;

@AutoService(SpoutMapperService.class)
public class SpoutMapperServiceImpl implements SpoutMapperService {

    @Override
    public EncryptedValue setupRoute(String componentId, EncryptedValue entry) {
        Objects.requireNonNull(componentId, "componentId cannot be null");
        Objects.requireNonNull(entry, "Encrypted entry cannot be null");
        // we want to verify that the entry is correctly sealed
        SealedPayload.verifyRoute(entry,
                TopologySpecification.Component.DATASET,
                TopologySpecification.Component.MAPPER);

        // get string body
        byte[] body = SealedPayload.decrypt(entry);

        TopologySpecification.Component downstreamComponent = TopologySpecification.requireSingleDownstream(componentId);

        // create new AAD with correct route names
        AADSpecification aad = AADSpecification.builder()
                .sourceComponent(componentId)
                .destinationComponent(downstreamComponent)
                .build();

        // seal again with new AAD routing information + return sealed entry
        return SealedPayload.encrypt(body, aad);
    }
}
