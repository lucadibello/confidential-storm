package ch.usi.inf.examples.confidential_word_count.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import ch.usi.inf.examples.confidential_word_count.common.api.histogram.HistogramService;
import ch.usi.inf.examples.confidential_word_count.common.api.histogram.model.HistogramSnapshotResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.histogram.model.HistogramUpdateRequest;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HistogramBolt extends ConfidentialBolt<HistogramService> {
    private static final Logger LOG = LoggerFactory.getLogger(HistogramBolt.class);
    private final String OUTPUT_FILE = "data/histogram.txt";
    private ExecutorService io;
    private int boltId;

    public HistogramBolt() {
        super(HistogramService.class);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        super.afterPrepare(topoConf, context);
        this.boltId = context.getThisTaskId();
        this.io = Executors.newSingleThreadExecutor();
        LOG.info("[HistogramBolt {}] Prepared. Snapshot output: {}", boltId, OUTPUT_FILE);
    }

    @Override
    protected void beforeCleanup() {
        if (io != null) {
            io.shutdown();
        }
        super.beforeCleanup();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5); // update histogram every second
        return config;
    }

    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void writeSnapshot(Map<String, Long> snap) {
        File file = new File(OUTPUT_FILE);
        // Ensure the output directory exists
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            if (!parent.mkdirs() && !parent.exists()) {
                LOG.error("Could not create output directory: {}", parent.getAbsolutePath());
                return;
            }
        }
        try {
            // Create the file if it does not exist
            if (!file.exists() && !file.createNewFile()) {
                LOG.error("Could not create output file: {}", file.getAbsolutePath());
                return;
            }
            try (PrintWriter out = new PrintWriter(new FileWriter(file, false))) {
                out.println("=== " + Instant.now() + " ===");
                snap.forEach((k, v) -> out.println(k + ":" + v));
            }
        } catch (IOException e) {
            LOG.error("Error while exporting histogram snapshot:", e);
        }
    }

    @Override
    protected void processTuple(Tuple input, HistogramService service) throws EnclaveServiceException {
        // if tick tuple -> export histogram to file
        if (isTickTuple(input)) {
            LOG.info("[HistogramBolt {}] Received tick tuple. Exporting histogram snapshot...", boltId);
            HistogramSnapshotResponse snapshot = service.snapshot();

            Map<String, Long> snap = snapshot.counts();
            if (io != null) {
                io.submit(() -> writeSnapshot(snap));
            } else {
                // export snapshot to file
                writeSnapshot(snap);
            }
            LOG.info("[HistogramBolt {}] Exported histogram snapshot with {} entries.", boltId, snapshot.counts().size());
            return;
        }

        // in any case -> update the histogram with the new values
        EncryptedValue word = (EncryptedValue) input.getValueByField("word");
        EncryptedValue newCount = (EncryptedValue) input.getValueByField("count");
        service.update(new HistogramUpdateRequest(word, newCount));

        // acknowledge the tuple
        getCollector().ack(input);
        LOG.debug("[HistogramBolt {}] Acked histogram update", boltId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // sink bolt
    }
}
