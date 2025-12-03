package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.wordcount;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.RoutingKeyDerivationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecificationBuilder;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.examples.confidential_word_count.common.api.WordCountService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.model.WordCountResponse;
import com.google.auto.service.AutoService;

import java.util.UUID;

@AutoService(WordCountService.class)
public class WordCountServiceImpl extends WordCountVerifier {
    private final String producerId = UUID.randomUUID().toString();
    private long sequenceCounter = 0L;

    @Override
    public WordCountResponse countImpl(WordCountRequest request) throws SealedPayloadProcessingException, CipherInitializationException, RoutingKeyDerivationException, AADEncodingException {
        // Decrypt the word from the request
        String word = sealedPayload.decryptToString(request.word());
        
        // Extract user_id from input AAD (from the word payload)
        DecodedAAD inputAad = DecodedAAD.fromBytes(request.word().associatedData());
        Object userId = inputAad.attributes().get("user_id");

        // Verify the routing key
        String expectedKey = sealedPayload.deriveRoutingKey(word);
        if (!expectedKey.equals(request.routingKey())) {
            throw new IllegalArgumentException("Routing key mismatch");
        }

        // Stateless: always emit 1
        long newCount = 1L;

        // Create AAD for both sealed values
        long sequence = sequenceCounter++;
        AADSpecificationBuilder aadBuilder = AADSpecification.builder()
                .sourceComponent(TopologySpecification.Component.WORD_COUNT)
                .destinationComponent(TopologySpecification.Component.HISTOGRAM_GLOBAL)
                .put("producer_id", producerId)
                .put("seq", sequence);

        if (userId != null) {
            aadBuilder.put("user_id", userId);
        }
        
        AADSpecification aad = aadBuilder.build();

        // Seal the word and the new count
        EncryptedValue sealedWord = sealedPayload.encryptString(word, aad);
        EncryptedValue sealedCount = sealedPayload.encryptString(Long.toString(newCount), aad);

        // Return the response including the sealed values
        return new WordCountResponse(expectedKey, sealedWord, sealedCount);
    }
}
