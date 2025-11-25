package ch.usi.inf.confidentialstorm.host.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.host.spouts.base.ConfidentialSpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.util.JokeReader;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomJokeSpout extends ConfidentialSpout {
  private static final long EMIT_DELAY_MS = 250;
  private List<EncryptedValue> encryptedJokes;
  private Random rand;
  private static final Logger LOG = LoggerFactory.getLogger(RandomJokeSpout.class);

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
  public void executeNextTuple() {
    // generate the next random joke
    int idx = rand.nextInt(encryptedJokes.size());
    EncryptedValue currentJoke = encryptedJokes.get(idx);

    // make test call to check what's crashing
    LOG.debug("[RandomJokeSpout {}] Testing route for joke index {}", this.state.getTaskId(), idx);
      EncryptedValue test = null;
      try {
          test = getMapperService().testRoute(TopologySpecification.Component.RANDOM_JOKE_SPOUT, currentJoke);
      } catch (SealedPayloadProcessingException | CipherInitializationException e) {
          LOG.error("[RandomJokeSpout {}] Route test failed", this.state.getTaskId(), e);
          throw new RuntimeException(e);
      }
      LOG.debug("[RandomJokeSpout {}] Route test produced {}", this.state.getTaskId(), test);

      EncryptedValue routedJoke = null;
      try {
          routedJoke = getMapperService().setupRoute(TopologySpecification.Component.RANDOM_JOKE_SPOUT, currentJoke);
      } catch (SealedPayloadProcessingException e) {
          throw new RuntimeException(e);
      } catch (CipherInitializationException e) {
          throw new RuntimeException(e);
      } catch (AADEncodingException e) {
          throw new RuntimeException(e);
      }

      LOG.info("[RandomJokeSpout {}] Emitting joke {}", this.state.getTaskId(), routedJoke);
    getCollector().emit(new Values(routedJoke));

    // sleep for a while to avoid starving the topology
    try {
      Thread.sleep(EMIT_DELAY_MS);
    } catch (InterruptedException e) {
      // do nothing
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("body"));
  }
}
