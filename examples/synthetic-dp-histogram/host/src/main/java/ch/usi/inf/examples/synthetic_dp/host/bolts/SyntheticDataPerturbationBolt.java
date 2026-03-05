package ch.usi.inf.examples.synthetic_dp.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.EncryptedDataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.dp.AbstractDataPerturbationBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SyntheticDataPerturbationBolt extends AbstractDataPerturbationBolt {

    // create logger for this class
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticDataPerturbationBolt.class);

    @Override
    protected boolean useEncryptedSnapshots() {
        return true;
    }

    @Override
    protected void processEncryptedSnapshot(EncryptedDataPerturbationSnapshot snapshot) {
        LOG.info("[DP-PARTIAL-OUTPUT-CHECK-TIMING] Sending encrypted histogram snapshot");
        getCollector().emit(new Values(snapshot.encryptedHistogram()));
    }

    @Override
    protected void processSnapshot(Map<String, Long> histogramSnapshot) {
        throw new UnsupportedOperationException("This bolt uses encrypted snapshots");
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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("encryptedHistogram"));
    }
}
