package ch.usi.inf.confidentialstorm.enclave.service.bolts.split;

import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceRequest;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceResponse;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedWord;
import ch.usi.inf.confidentialstorm.common.crypto.model.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLoggerFactory;
import com.google.auto.service.AutoService;

import java.util.*;
import java.util.stream.Collectors;

@AutoService(SplitSentenceService.class)
public class SplitSentenceServiceImpl extends SplitSentenceVerifier {
    private static final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(SplitSentenceServiceImpl.class);

    @Override
    public SplitSentenceResponse splitImpl(SplitSentenceRequest request) {
        LOG.info("SplitSentenceServiceImpl: validated request received.");

        // decrypt the payload
        String body = SealedPayload.decryptToString(request.body());

        LOG.debug("HELLO FROM SPLIT SENTENCE SERVICE");
        LOG.info("Received sentence: {}", body);

        // compute sensitive operation
        //noinspection SimplifyStreamApiCallChains
        List<String> plainWords = Arrays.stream(body.split("\\W+"))
                .map(word -> word.toLowerCase(Locale.ROOT).trim())
                .filter(word -> !word.isEmpty())
                .collect(Collectors.toList());

        // NOTE: We need to encrypt each word separately as it will be handled alone
        // by the next services in the pipeline.
        List<EncryptedWord> encryptedWords = new ArrayList<>(plainWords.size());
        for (String plainWord : plainWords) {
            // for each word, derive a routing key (HMAC of the plaintext word)
            String routingKey = SealedPayload.deriveRoutingKey(plainWord);

            // Create new AAD specification (custom for each word)
            AADSpecification aad = AADSpecification.builder()
                    // NOTE: specify source and destination components for verification purposes
                    .sourceComponent(TopologySpecification.Component.SENTENCE_SPLIT)
                    .destinationComponent(TopologySpecification.Component.WORD_COUNT)
                    .build();

            // encrypt the word with its AAD
            EncryptedValue payload = SealedPayload.encryptString(plainWord, aad);

            // store encrypted word
            encryptedWords.add(new EncryptedWord(routingKey, payload));
        }

        // return response to bolt
        return new SplitSentenceResponse(encryptedWords);
    }
}
