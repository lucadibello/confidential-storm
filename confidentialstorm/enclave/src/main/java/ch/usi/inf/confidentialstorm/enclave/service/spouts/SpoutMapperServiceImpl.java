package ch.usi.inf.confidentialstorm.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.api.SpoutMapperService;
import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.model.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import com.google.auto.service.AutoService;

@AutoService(SpoutMapperService.class)
public class SpoutMapperServiceImpl implements SpoutMapperService {

    @Override
    public EncryptedValue setupRoute(EncryptedValue entry) {
        // we want to verify that the entry is correctly sealed
        SealedPayload.verifyRoute(entry, "_DATASET", "_MAPPER");

        // get string body
        byte[] body = SealedPayload.decrypt(entry);

        // create new AAD with correct route names
        AADSpecification aad = AADSpecification.builder()
                .sourceComponent(SpoutMapperService.class)
                .destinationComponent(SplitSentenceService.class)
                .build();

        // seal again with new AAD routing information + return sealed entry
        return SealedPayload.encrypt(body, aad);
    }
}
