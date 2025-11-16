package ch.usi.inf.confidentialstorm.enclave.service.split;

import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceRequest;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceResponse;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedWord;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

@AutoService(SplitSentenceService.class)
public class SplitSentenceServiceImpl implements SplitSentenceService {

    @Override
    public SplitSentenceResponse split(SplitSentenceRequest request) {
        // decrypt the payload
        String body = SealedPayload.decryptToString(request.body());

        System.out.println("HELLO FROM SPLIT SENTENCE SERVICE");
        System.out.println("Received sentence: " + body);

        // compute sensitive operation
        List<String> plainWords = Arrays.stream(body.split("\\W+"))
                .map(word -> word.toLowerCase(Locale.ROOT).trim())
                .filter(word -> !word.isEmpty())
                .collect(Collectors.toList());

        // NOTE: We need to encrypt each word separately as it will be handled alone
        // by the next services in the pipeline.
        List<EncryptedWord> encryptedWords = new ArrayList<>(plainWords.size());
        int position = 0;
        for (String plainWord : plainWords) {
            // for each word, derive a routing key + create dummy AAD
            String routingKey = SealedPayload.deriveRoutingKey(plainWord);
            Map<String, Object> aad = Map.of(
                    "type", "word",
                    "stage", "split",
                    "index", position++
            );
            // encrypt the word with its AAD
            EncryptedValue payload = SealedPayload.encryptString(plainWord, aad);

            // store encrypted word
            encryptedWords.add(new EncryptedWord(routingKey, payload));
        }

        // return response to bolt
        return new SplitSentenceResponse(encryptedWords);
    }
}
