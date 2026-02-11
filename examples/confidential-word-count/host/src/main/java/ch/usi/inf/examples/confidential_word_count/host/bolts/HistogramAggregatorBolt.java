package ch.usi.inf.examples.confidential_word_count.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.dp.AbstractHistogramAggregationBolt;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class HistogramAggregatorBolt extends AbstractHistogramAggregationBolt {

    private static final String OUTPUT_FILE = "data/histogram.txt";
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
    protected EncryptedValue getEncryptedPartialHistogram(Tuple input) {
        return (EncryptedValue) input.getValueByField("encryptedHistogram");
    }

    @Override
    protected void processCompleteHistogram(Map<String, Long> mergedHistogram) {
        if (io != null) {
            io.submit(() -> {
                // Collect into a NEW LinkedHashMap to preserve the sorted order
                Map<String, Long> sortedHistogram = mergedHistogram.entrySet().stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (e1, e2) -> e1,
                                LinkedHashMap::new
                        ));
                // write the sorted histogram snapshot to file
                writeSnapshot(sortedHistogram);
            });
        } else {
            LOG.error("IO executor service is not initialized. Cannot write snapshot.");
        }

        // emit the merged histogram downstream
        getCollector().emit(new Values(mergedHistogram));
    }

    private void writeSnapshot(Map<String, Long> snap) {
        File file = new File(OUTPUT_FILE);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            if (!parent.mkdirs() && !parent.exists()) {
                LOG.error("Could not create output directory: {}", parent.getAbsolutePath());
                return;
            }
        }
        try {
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
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("histogram"));
    }
}
