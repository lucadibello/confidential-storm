package ch.usi.inf.confidentialstorm.enclave.split;

import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.model.SplitSentenceRequest;
import ch.usi.inf.confidentialstorm.common.model.SplitSentenceResponse;
import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@AutoService(SplitSentenceService.class)
public class SplitSentenceServiceImpl implements SplitSentenceService {

    @Override
    public SplitSentenceResponse split(SplitSentenceRequest request) {
        List<String> words = Arrays.stream(request.body().split("\\W+"))
                .map(word -> word.toLowerCase(Locale.ROOT).trim())
                .filter(word -> !word.isEmpty())
                .collect(Collectors.toList());
        return new SplitSentenceResponse(words);
    }
}
