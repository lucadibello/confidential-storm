package ch.usi.inf.examples.confidential_word_count.host.bolts.tunnel;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.tunnel.AbstractCloakedTunnelReceiverBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Tunnel receiver bolt that re-emits individual (word, count, userId, routingKey) tuples
 * extracted from decrypted batches.
 * <p>
 * The routingKey was wrapped into an EncryptedValue by the sender for tunnel transport;
 * this bolt unwraps it back to a plain byte[].
 */
public class SplitBoundTunnelReceiverBolt extends AbstractCloakedTunnelReceiverBolt {

    @Override
    protected void emitRealTuple(Tuple anchor, List<EncryptedValue> fields) {
        // fields layout: [word, count, userId, routingKeyWrapped]
        EncryptedValue word = fields.get(0);
        EncryptedValue count = fields.get(1);
        EncryptedValue userId = fields.get(2);

        // Unwrap the routing key from its EncryptedValue transport container
        byte[] routingKey = fields.get(3).ciphertext();

        getCollector().emit(anchor, new Values(word, count, userId, routingKey));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Same output format as SplitSentenceBolt: (word, count, userId, routingKey)
        declarer.declare(new Fields("word", "count", "userId", "routingKey"));
    }
}
