package ch.usi.inf.confidentialstorm.enclave.service.bolts.wordcount;

import ch.usi.inf.confidentialstorm.common.api.WordCountService;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountRequest;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.RoutingKeyDerivationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@AutoService(WordCountService.class)
public class WordCountServiceImpl extends WordCountVerifier {
    private final Map<String, Long> counter = new HashMap<>();
    private long sequenceCounter = 0L;
    private final String producerId = UUID.randomUUID().toString();

    @Override
    public WordCountResponse countImpl(WordCountRequest request) throws SealedPayloadProcessingException, CipherInitializationException, RoutingKeyDerivationException, AADEncodingException {
        // Decrypt the word from the request
        String word = sealedPayload.decryptToString(request.word());

        // Verify the routing key
        String expectedKey = sealedPayload.deriveRoutingKey(word);
        if (!expectedKey.equals(request.routingKey())) {
            throw new IllegalArgumentException("Routing key mismatch");
        }

        long newCount = counter.merge(word, 1L, Long::sum);

        // Create AAD for both sealed values
        long sequence = sequenceCounter++;
        AADSpecification aad = AADSpecification.builder()
                .sourceComponent(TopologySpecification.Component.WORD_COUNT)
                .destinationComponent(TopologySpecification.Component.HISTOGRAM_GLOBAL)
                .put("producer_id", producerId)
                .put("seq", sequence)
                .build();

        // Seal the word and the new count
        EncryptedValue sealedWord = sealedPayload.encryptString(word, aad);
        EncryptedValue sealedCount = sealedPayload.encryptString(Long.toString(newCount), aad);

        // Return the response including the sealed values
        return new WordCountResponse(expectedKey, sealedWord, sealedCount);
    }
}
