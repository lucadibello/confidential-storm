package ch.usi.inf.examples.confidential_word_count.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.dp.AbstractDataPerturbationBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataPerturbationBolt extends AbstractDataPerturbationBolt {

    private final String OUTPUT_FILE = "data/histogram.txt";
    private ExecutorService io;

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
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
    protected void processSnapshot(Map<String, Long> histogramSnapshot) {
        if (io != null) {
            // asynchronously write the snapshot to avoid blocking the main processing thread
            io.submit(() -> writeSnapshot(histogramSnapshot));
        } else {
            LOG.error("IO executor service is not initialized. Cannot write snapshot.");
        }

        // pass the snapshot to the next bolt in the topology
        getCollector().emit(new Values(histogramSnapshot));
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
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("histogram"));
    }

    @Override
    protected EncryptedValue getUserIdEntry(Tuple input) {
        return (EncryptedValue) input.getValueByField("userId");
    }

    @Override
    protected EncryptedValue getWordEntry(Tuple input) {
        return (EncryptedValue) input.getValueByField("word");
    }

    @Override
    protected EncryptedValue getClampedCountEntry(Tuple input) {
        return (EncryptedValue) input.getValueByField("count");
    }
}
