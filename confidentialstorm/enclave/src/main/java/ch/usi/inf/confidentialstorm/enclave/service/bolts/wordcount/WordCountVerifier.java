package ch.usi.inf.confidentialstorm.enclave.service.bolts.wordcount;

import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.api.WordCountService;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountRequest;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountResponse;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;

import java.util.Collection;
import java.util.List;

public abstract class WordCountVerifier extends ConfidentialBoltService<WordCountRequest> implements WordCountService {

    @Override
    public WordCountResponse count(WordCountRequest request) {
        super.verify(request);
        return countImpl(request);
    }
    public abstract WordCountResponse countImpl(WordCountRequest request);

    @Override
    public Class<?> expectedSourceComponentName() {
        return SplitSentenceService.class;
    }

    @Override
    public Class<?> expectedDestinationComponentName() {
        return WordCountService.class;
    }

    @Override
    public Collection<EncryptedValue> valuesToVerify(WordCountRequest request) {
        return List.of(request.word());
    }
}
