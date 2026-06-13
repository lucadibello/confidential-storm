package ch.usi.inf.examples.confidential_word_count.host.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.spouts.ConfidentialSpout;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.SpoutPreprocessingService;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SealedJokeEntry;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SpoutPreprocessingRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SpoutPreprocessingResponse;
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

public class RandomJokeSpout extends ConfidentialSpout<SpoutPreprocessingService> {
    private static final long EMIT_DELAY_MS = 250;
    private static final Logger LOG = LoggerFactory.getLogger(RandomJokeSpout.class);
    private List<SealedJokeEntry> encryptedJokes;
    private Random rand;

    public RandomJokeSpout() {
        super(SpoutPreprocessingService.class);
    }

    @Override
    protected void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        // Load jokes into memory
        JokeReader jokeReader = new JokeReader();
        try {
            encryptedJokes = jokeReader.readAll("jokes.enc.json");
            LOG.info("[RandomJokeSpout {}] Loaded {} jokes from encrypted dataset", this.state.getTaskId(), encryptedJokes.size());
        } catch (IOException e) {
            LOG.error("[RandomJokeSpout {}] Failed to load jokes dataset", this.state.getTaskId(), e);
            throw new RuntimeException(e);
        }
        this.rand = new Random();
        LOG.info("[RandomJokeSpout {}] Prepared with delay {} ms",
                this.state.getTaskId(), EMIT_DELAY_MS);
    }

    protected void beforeClose() {
        LOG.info("[RandomJokeSpout {}] Closing", this.state.getTaskId());
    }

    @Override
    public void executeNextTuple() throws EnclaveServiceException {
        // Select random joke
        int idx = rand.nextInt(encryptedJokes.size());
        SealedJokeEntry currentJokeEntry = encryptedJokes.get(idx);

        // Setup routing using the spout service
        SpoutPreprocessingResponse routedJokeEntry = getService().setupRoute(
                new SpoutPreprocessingRequest(currentJokeEntry.payload(), currentJokeEntry.userId()));

        // Emit joke payload and user ID
        LOG.info("[RandomJokeSpout {}] Emitting new joke tuple", this.state.getTaskId());
        getCollector().emit(new Values(routedJokeEntry.payload(), routedJokeEntry.userId()));

        // Sleep to limit emission rate and prevent starvation
        try {
            Thread.sleep(EMIT_DELAY_MS);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Output fields: payload, userId
        declarer.declare(new Fields("payload", "userId"));
    }
}
