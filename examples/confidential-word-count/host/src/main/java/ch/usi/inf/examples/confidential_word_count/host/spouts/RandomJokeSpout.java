package ch.usi.inf.examples.confidential_word_count.host.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.host.spouts.ConfidentialSpout;
import ch.usi.inf.examples.confidential_word_count.common.api.SpoutMapperService;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import ch.usi.inf.examples.confidential_word_count.host.util.JokeReader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomJokeSpout extends ConfidentialSpout<SpoutMapperService> {
    private static final long EMIT_DELAY_MS = 250;
    private static final Logger LOG = LoggerFactory.getLogger(RandomJokeSpout.class);
    private List<EncryptedValue> encryptedJokes;
    private Random rand;

    public RandomJokeSpout() {
        super(SpoutMapperService.class);
    }

    @Override
    protected void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        // we need to load the jokes in memory
        JokeReader jokeReader = new JokeReader();
        try {
            encryptedJokes = jokeReader.readAll("jokes.enc.json");
            LOG.info("[RandomJokeSpout {}] Loaded {} jokes from encrypted dataset", this.state.getTaskId(), encryptedJokes.size());
        } catch (IOException e) {
            LOG.error("[RandomJokeSpout {}] Failed to load jokes dataset", this.state.getTaskId(), e);
            throw new RuntimeException(e);
        }
        // save the collector for emitting tuples <Joke, String>
        this.rand = new Random();
        LOG.info("[RandomJokeSpout {}] Prepared with delay {} ms",
                this.state.getTaskId(), EMIT_DELAY_MS);
    }

    protected void beforeClose() {
        LOG.info("[RandomJokeSpout {}] Closing", this.state.getTaskId());
    }

    @Override
    public void executeNextTuple() throws EnclaveServiceException {
        // generate the next random joke
        int idx = rand.nextInt(encryptedJokes.size());
        EncryptedValue currentJokeEntry = encryptedJokes.get(idx);

        // use service to configure route for the joke entry (spout -> splitter)
        LOG.debug("[RandomJokeSpout {}] Testing route for joke index {}", this.state.getTaskId(), idx);
        EncryptedValue routedJokeEntry = getService().setupRoute(ComponentConstants.RANDOM_JOKE_SPOUT, currentJokeEntry);

        // NOTE: each joke is a JSON entry with fields: ["body", "category", "id", "rating", "user_id"]
        LOG.info("[RandomJokeSpout {}] Emitting joke {}", this.state.getTaskId(), routedJokeEntry);
        getCollector().emit(new Values(routedJokeEntry));

        // sleep for a while to avoid starving the topology
        try {
            Thread.sleep(EMIT_DELAY_MS);
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entry"));
    }
}
