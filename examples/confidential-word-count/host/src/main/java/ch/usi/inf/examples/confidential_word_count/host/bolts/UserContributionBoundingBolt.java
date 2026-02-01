package ch.usi.inf.examples.confidential_word_count.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.dp.ContributionBoundingBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class UserContributionBoundingBolt extends ContributionBoundingBolt {

    @Override
    protected EncryptedValue getEncryptedPayload(Tuple input) {
        return (EncryptedValue) input.getValueByField("word");
    }

    @Override
    protected EncryptedValue getEncryptedCount(Tuple input) {
        return (EncryptedValue) input.getValueByField("count");
    }

    @Override
    protected EncryptedValue getEncryptedUserId(Tuple input) {
        return (EncryptedValue) input.getValueByField("userId");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Tuple format: (word, count, userId)
        declarer.declare(new Fields("word", "count", "userId"));
    }
}
