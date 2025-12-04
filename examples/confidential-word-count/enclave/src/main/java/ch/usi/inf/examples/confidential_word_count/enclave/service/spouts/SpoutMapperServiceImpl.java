package ch.usi.inf.examples.confidential_word_count.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.exception.EnclaveExceptionContext;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.confidentialstorm.common.api.SpoutMapperService;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import com.google.auto.service.AutoService;

import java.util.Objects;
import java.util.UUID;

@AutoService(SpoutMapperService.class)
public final class SpoutMapperServiceImpl implements SpoutMapperService {
    private final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(SpoutMapperServiceImpl.class);
    private final EnclaveExceptionContext exceptionCtx = EnclaveExceptionContext.getInstance();
    private final String producerId = UUID.randomUUID().toString();
    private final SealedPayload sealedPayload;
    private long sequenceCounter = 0L;

    @SuppressWarnings("unused")
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
            DecodedAAD inputAad = DecodedAAD.fromBytes(entry.associatedData());

            TopologySpecification.Component downstreamComponent = TopologySpecification.requireSingleDownstream(component);

            long sequence = sequenceCounter++;
            // create new AAD with correct route names
            AADSpecification aad = AADSpecification.builder()
                    .sourceComponent(component)
                    .destinationComponent(downstreamComponent)
                    // preserve input attributes such as user_id to enable user-level DP
                    .putAll(inputAad.attributes())
                    .put("producer_id", producerId)
                    .put("seq", sequence)
                    .build();

            // seal again with new AAD routing information + return sealed entry
            return sealedPayload.encrypt(body, aad);
        } catch (Throwable t) {
            exceptionCtx.handleException(t);
            return null;
        }
    }
}
