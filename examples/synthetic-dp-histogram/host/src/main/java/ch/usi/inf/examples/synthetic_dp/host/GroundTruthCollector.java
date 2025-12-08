package ch.usi.inf.examples.synthetic_dp.host;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class GroundTruthCollector {
    private static final Map<String, Long> COUNTS = new ConcurrentHashMap<>();

    private GroundTruthCollector() {
    }

    public static void record(String key, long delta) {
        COUNTS.merge(key, delta, Long::sum);
    }

    public static Map<String, Long> snapshot() {
        return Map.copyOf(COUNTS);
    }
}
