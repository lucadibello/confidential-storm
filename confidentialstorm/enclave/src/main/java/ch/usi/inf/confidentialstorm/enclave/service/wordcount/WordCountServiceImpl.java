package ch.usi.inf.confidentialstorm.enclave.service.wordcount;

import ch.usi.inf.confidentialstorm.common.api.WordCountService;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountRequest;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountResponse;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

@AutoService(WordCountService.class)
public class WordCountServiceImpl implements WordCountService {
    private final Map<String, Long> counter = new HashMap<>();

    @Override
    public WordCountResponse count(WordCountRequest request) {
        String word = SealedPayload.decryptToString(request.word());
        String expectedKey = SealedPayload.deriveRoutingKey(word);
        if (!expectedKey.equals(request.routingKey())) {
            throw new IllegalArgumentException("Routing key mismatch");
        }

        long newCount = counter.merge(word, 1L, Long::sum);
        EncryptedValue sealedWord = SealedPayload.encryptString(word, Map.of(
                "type", "word",
                "stage", "wordcount"
        ));
        EncryptedValue sealedCount = SealedPayload.encryptString(Long.toString(newCount), Map.of(
                "type", "count",
                "stage", "wordcount",
                "key", expectedKey
        ));
        return new WordCountResponse(expectedKey, sealedWord, sealedCount);
    }
}
