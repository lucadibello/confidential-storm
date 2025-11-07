package ch.usi.inf.confidentialstorm.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.HistogramService;
import ch.usi.inf.confidentialstorm.common.model.HistogramUpdate;
import ch.usi.inf.confidentialstorm.host.bolts.base.ConfidentialBolt;
import org.apache.storm.Config;
import org.apache.storm.Constants;
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
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private final String OUTPUT_FILE = "data/histogram.txt";
    private ExecutorService io;

    public HistogramBolt() {
        super(HistogramService.class);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, org.apache.storm.task.TopologyContext context) {
        super.afterPrepare(topoConf, context);
        this.io = Executors.newSingleThreadExecutor();
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
    protected void processTuple(Tuple input, HistogramService service) {
        if (isTickTuple(input)) {
            var snapshot = service.snapshot();
            LOG.info("[HistogramBolt] Received tick tuple. Exporting histogram snapshot with {} entries.", snapshot.counts().size());
            Map<String, Long> snap = snapshot.counts();
            if (io != null) {
                io.submit(() -> writeSnapshot(snap));
            } else {
                writeSnapshot(snap);
            }
            return;
        }
        String word = input.getStringByField("word");
        Long newCount = input.getLongByField("count");
        service.update(new HistogramUpdate(word, newCount));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // sink bolt
    }
}
