package ch.usi.inf.examples.synthetic_dp.host.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.examples.synthetic_dp.common.api.SyntheticDataService;
import ch.usi.inf.examples.synthetic_dp.common.api.model.SyntheticEncryptedRecord;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.examples.synthetic_dp.host.GroundTruthCollector;
import ch.usi.inf.confidentialstorm.host.spouts.ConfidentialSpout;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.spout.SpoutOutputCollector;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class SyntheticSpout extends ConfidentialSpout<SyntheticDataService> {
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticSpout.class);

    private ZipfDistribution zipf;
    private Random rng;
    private final int batchSize = 20_000;
    private final int numUsers = 100_000;
    private final Map<Long, Integer> userCounts = new ConcurrentHashMap<>();

    public SyntheticSpout() {
        super(SyntheticDataService.class);
    }

    @Override
    protected void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.zipf = new ZipfDistribution(100_000, 1.1);
        this.rng = new Random(42);
        LOG.info("SyntheticSpout prepared with batchSize={}, users={}, seed=42", batchSize, numUsers);
    }

    @Override
    protected void executeNextTuple() throws EnclaveServiceException {
        for (int i = 0; i < batchSize; i++) {
            long userId = rng.nextInt(numUsers);
            int keyId = zipf.sample();
            String key = "k" + keyId;

            int used = userCounts.merge(userId, 1, Integer::sum);
            if (used > DPConfig.MAX_CONTRIBUTIONS_PER_USER) {
                continue;
            }
            GroundTruthCollector.record(key, 1L);

            // Encrypt record using enclave service
            LOG.trace("Encrypting record for user {}: key={}", userId, key);
            SyntheticEncryptedRecord rec = getService().encryptRecord(key, "1", Long.toString(userId));
            if (rec == null) {
                continue;
            }
            EncryptedValue k = rec.key();
            EncryptedValue c = rec.count();
            EncryptedValue u = rec.userId();
            getCollector().emit(new Values(k, c, u));
        }

        // Log statistics periodically (every 100 batches)
        if (totalRecordsEmitted.get() % (BATCH_SIZE * 100) == 0) {
            logStatistics();
        }

        // Small delay between batches to avoid overwhelming the system
        if (SLEEP_MS > 0) {
            try {
                Thread.sleep(SLEEP_MS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count", "user"));
    }
}
