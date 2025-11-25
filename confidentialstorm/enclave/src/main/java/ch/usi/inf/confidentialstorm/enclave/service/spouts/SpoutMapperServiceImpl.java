package ch.usi.inf.confidentialstorm.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.api.SpoutMapperService;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveExceptionUtil;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLoggerFactory;
import com.google.auto.service.AutoService;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@AutoService(SpoutMapperService.class)
public class SpoutMapperServiceImpl implements SpoutMapperService {
    private final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(SpoutMapperServiceImpl.class);
    private final AtomicLong sequenceCounter = new AtomicLong(0);
    private final String producerId = UUID.randomUUID().toString();
    private final SealedPayload sealedPayload;

    public SpoutMapperServiceImpl() {
        this(SealedPayload.fromConfig());
    }

    SpoutMapperServiceImpl(SealedPayload sealedPayload) {
        this.sealedPayload = Objects.requireNonNull(sealedPayload, "sealedPayload cannot be null");
    }

    @Override
    public EncryptedValue setupRoute(TopologySpecification.Component component, EncryptedValue entry) throws EnclaveServiceException {
        try {
            Objects.requireNonNull(component, "component cannot be null");
            Objects.requireNonNull(entry, "Encrypted entry cannot be null");

            LOG.info("Setting up route for component: " + component.name());
            // we want to verify that the entry is correctly sealed
            sealedPayload.verifyRoute(entry,
                    TopologySpecification.Component.DATASET,
                    TopologySpecification.Component.MAPPER
            );

            // get string body
            byte[] body = sealedPayload.decrypt(entry);

            TopologySpecification.Component downstreamComponent = TopologySpecification.requireSingleDownstream(component);

            long sequence = sequenceCounter.getAndIncrement();
            // create new AAD with correct route names
            AADSpecification aad = AADSpecification.builder()
                    .sourceComponent(component)
                    .destinationComponent(downstreamComponent)
                    .put("producer_id", producerId)
                    .put("seq", sequence)
                    .build();

            // seal again with new AAD routing information + return sealed entry
            return sealedPayload.encrypt(body, aad);
        } catch (Throwable e) {
            throw EnclaveExceptionUtil.wrap("SpoutMapperService.setupRoute", e);
        }
    }
}
