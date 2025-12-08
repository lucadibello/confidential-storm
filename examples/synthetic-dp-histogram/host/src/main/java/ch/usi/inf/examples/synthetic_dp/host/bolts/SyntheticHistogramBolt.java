package ch.usi.inf.examples.synthetic_dp.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticHistogramService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticSnapshotResponse;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticUpdateRequest;
import ch.usi.inf.examples.synthetic_dp.host.GroundTruthCollector;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyntheticHistogramBolt extends ConfidentialBolt<SyntheticHistogramService> {
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticHistogramBolt.class);
    private final String OUTPUT_FILE = "data/synthetic-report.txt";

    public SyntheticHistogramBolt() {
        super(SyntheticHistogramService.class);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return Map.of(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
    }

    @Override
    protected void processTuple(Tuple input, SyntheticHistogramService service) throws EnclaveServiceException {
        // if tick tuple (processing-time) produce snapshot and write report
        if (isTick(input)) {
            LOG.debug("Processing tick tuple - producing snapshot");
            SyntheticSnapshotResponse resp = service.snapshot();
            writeReport(resp.counts(), GroundTruthCollector.snapshot());
            // acknowledge tuple
            getCollector().ack(input);
            return;
        }

        // if data tuple (event-time) process update
        LOG.debug("Processing data tuple - updating histogram");
        EncryptedValue key = (EncryptedValue) input.getValueByField("key");
        EncryptedValue count = (EncryptedValue) input.getValueByField("count");
        EncryptedValue user = (EncryptedValue) input.getValueByField("user");
        LOG.debug("Updating histogram for encrypted key tuple");
        service.update(new SyntheticUpdateRequest(key, count, user));

        // acknowledge tuple
        getCollector().ack(input);
    }

    private boolean isTick(Tuple t) {
        return t.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && t.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void writeReport(Map<String, Long> dp, Map<String, Long> gt) {
        long l0 = dp.values().stream().filter(v -> v > 0).count();
        var union = new java.util.HashSet<String>();
        union.addAll(dp.keySet());
        union.addAll(gt.keySet());

        long lInf = union.stream()
                .mapToLong(k -> Math.abs(dp.getOrDefault(k, 0L) - gt.getOrDefault(k, 0L)))
                .max().orElse(0L);
        long l1 = union.stream()
                .mapToLong(k -> Math.abs(dp.getOrDefault(k, 0L) - gt.getOrDefault(k, 0L)))
                .sum();
        double l2 = Math.sqrt(union.stream()
                .mapToLong(k -> {
                    long diff = dp.getOrDefault(k, 0L) - gt.getOrDefault(k, 0L);
                    return diff * diff;
                }).sum());

        File file = new File(OUTPUT_FILE);
        file.getParentFile().mkdirs();

        try (PrintWriter out = new PrintWriter(new FileWriter(file, false))) {
            out.println("=== " + Instant.now() + " ===");
            out.printf("keys_retained=%d%n", l0);
            out.printf("l_inf=%d%n", lInf);
            out.printf("l_1=%d%n", l1);
            out.printf("l_2=%.2f%n", l2);
            LOG.info("Reported metrics: keys_retained={}, l_inf={}, l_1={}, l_2={}", l0, lInf, l1, l2);
        } catch (IOException e) {
            LOG.error("Error writing report", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // sink
    }
}
