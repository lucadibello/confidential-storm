package ch.usi.inf.confidentialstorm.enclave.wordcount;

import ch.usi.inf.confidentialstorm.common.api.WordCountService;
import ch.usi.inf.confidentialstorm.common.model.WordCountRequest;
import ch.usi.inf.confidentialstorm.common.model.WordCountResponse;
import com.google.auto.service.AutoService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@AutoService(WordCountService.class)
public class WordCountServiceImpl implements WordCountService {
    private final ConcurrentMap<String, Long> counter = new ConcurrentHashMap<>();

    @Override
    public WordCountResponse count(WordCountRequest request) {
        long newCount = counter.merge(request.word(), 1L, Long::sum);
        return new WordCountResponse(request.word(), newCount);
    }
}
