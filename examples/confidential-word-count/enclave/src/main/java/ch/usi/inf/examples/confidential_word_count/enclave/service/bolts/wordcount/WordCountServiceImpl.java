package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.wordcount;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecificationBuilder;
import ch.usi.inf.examples.confidential_word_count.common.api.WordCountService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.*;
import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@AutoService(WordCountService.class)
public final class WordCountServiceImpl extends WordCountVerifier {
    private final String producerId = UUID.randomUUID().toString();
    private long sequenceCounter = 0L;
    private final Map<String, Long> buffer = new HashMap<>();

    @Override
    public WordCountAckResponse countImpl(WordCountRequest request) throws SealedPayloadProcessingException, CipherInitializationException {
        // Decrypt the word from the request
        String word = sealedPayload.decryptToString(request.word());
        
        // Update buffer
        buffer.merge(word, 1L, Long::sum);

        // Return acknowledgment to indicate successful buffering
        return new WordCountAckResponse();
    }

    @Override
    public WordCountFlushResponse flushImpl(WordCountFlushRequest request) throws SealedPayloadProcessingException, CipherInitializationException, AADEncodingException {
        // Prepare list to hold WordCountResponses
        List<WordCountResponse> responses = new ArrayList<>();

        // Create basic structure for AAD
        AADSpecificationBuilder aadBuilder = AADSpecification.builder()
                .sourceComponent(TopologySpecification.Component.WORD_COUNT)
                .destinationComponent(TopologySpecification.Component.HISTOGRAM_GLOBAL)
                .put("producer_id", producerId);

        // for each entry in the buffer, create a WordCountResponse with sealed word and sealed count
        for (Map.Entry<String, Long> entry : buffer.entrySet()) {
            String word = entry.getKey();
            Long count = entry.getValue();

            // Create AAD for both sealed values
            // NOTE: the `user_id` attribute is lost during aggregation (expected behavior)
            long sequence = sequenceCounter++;
            AADSpecification aad = aadBuilder.put("seq", sequence).build();

            // Seal the word and the new count
            EncryptedValue sealedWord = sealedPayload.encryptString(word, aad);
            EncryptedValue sealedCount = sealedPayload.encryptString(Long.toString(count), aad);

            // Create WordCountResponse and add to responses
            responses.add(new WordCountResponse(sealedWord, sealedCount));
        }

        // clear the buffer after flushing
        buffer.clear();

        // Return the flush response with all WordCountResponses
        return new WordCountFlushResponse(responses);
    }
}