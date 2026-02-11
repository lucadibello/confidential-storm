package ch.usi.inf.examples.confidential_word_count.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.EncryptedDataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.dp.AbstractDataPerturbationBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class DataPerturbationBolt extends AbstractDataPerturbationBolt {

    @Override
    protected boolean useEncryptedSnapshots() {
        return true; // this will make sure to encrypt snapshots before sending them to the aggregation bolt!
    }

    @Override
    protected void processEncryptedSnapshot(EncryptedDataPerturbationSnapshot snapshot) {
        // Forward the encrypted partial histogram to the aggregation bolt
        getCollector().emit(new Values(snapshot.encryptedHistogram()));
    }

    @Override
    protected void processSnapshot(Map<String, Long> histogramSnapshot) {
        // NOTE: since this bolt is configured to use encrypted snapshots, this method will never be called.
        // If it is called, it means that something went wrong in the configuration of the bolt, so we throw an
        // exception to make sure that we notice it.
        throw new UnsupportedOperationException("This bolt uses encrypted snapshots");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("encryptedHistogram"));
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
