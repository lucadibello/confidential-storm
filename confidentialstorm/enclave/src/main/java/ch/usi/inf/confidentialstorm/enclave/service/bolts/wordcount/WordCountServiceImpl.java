package ch.usi.inf.confidentialstorm.enclave.service.bolts.wordcount;

import ch.usi.inf.confidentialstorm.common.api.HistogramService;
import ch.usi.inf.confidentialstorm.common.api.WordCountService;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountRequest;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountResponse;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.model.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

@AutoService(WordCountService.class)
public class WordCountServiceImpl extends WordCountVerifier {
    private final Map<String, Long> counter = new HashMap<>();

    @Override
    public WordCountResponse countImpl(WordCountRequest request) {
        // Decrypt the word from the request
        String word = SealedPayload.decryptToString(request.word());

        // Verify the routing key
        String expectedKey = SealedPayload.deriveRoutingKey(word);
        if (!expectedKey.equals(request.routingKey())) {
            throw new IllegalArgumentException("Routing key mismatch");
        }

        long newCount = counter.merge(word, 1L, Long::sum);

        // Create AAD for both sealed values
        AADSpecification aad = AADSpecification.builder()
                .sourceComponent(WordCountService.class.getName())
                .destinationComponent(HistogramService.class.getName())
                .build();

        // Seal the word and the new count
        EncryptedValue sealedWord = SealedPayload.encryptString(word, aad);
        EncryptedValue sealedCount = SealedPayload.encryptString(Long.toString(newCount), aad);

        // Return the response including the sealed values
        return new WordCountResponse(expectedKey, sealedWord, sealedCount);
    }
}
