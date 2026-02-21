package ch.usi.inf.examples.confidential_word_count.host.bolts.tunnel;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.tunnel.AbstractCloakedTunnelSenderBolt;
import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * Tunnel sender bolt inserted between SplitSentenceBolt and UserContributionBoundingBolt.
 * Buffers (word, count, userId, routingKey) tuples and emits fixed-size encrypted batches.
 * <p>
 * The routingKey (a plain byte[] hash) is wrapped into an EncryptedValue container for
 * transport through the tunnel's slot codec, then unwrapped by the receiver.
 */
public class SplitBoundTunnelSenderBolt extends AbstractCloakedTunnelSenderBolt {

    /** 12-byte zero nonce used to wrap the plaintext routing key for tunnel transport. */
    private static final byte[] TRANSPORT_NONCE = new byte[12];

    @Override
    protected List<EncryptedValue> extractTupleFields(Tuple input) {
        EncryptedValue word = (EncryptedValue) input.getValueByField("word");
        EncryptedValue count = (EncryptedValue) input.getValueByField("count");
        EncryptedValue userId = (EncryptedValue) input.getValueByField("userId");

        // Wrap the plain routing key bytes into an EncryptedValue container for slot transport
        byte[] routingKeyBytes = (byte[]) input.getValueByField("routingKey");
        EncryptedValue routingKeyWrapped = new EncryptedValue(new byte[0], TRANSPORT_NONCE, routingKeyBytes);

        return List.of(word, count, userId, routingKeyWrapped);
    }

    @Override
    protected int getTickFrequencySecs() {
        return 1;
    }
}
