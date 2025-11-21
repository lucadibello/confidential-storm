package ch.usi.inf.confidentialstorm.host.spouts;

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
  private final Logger LOG = LoggerFactory.getLogger(getClass());

  @Override
  protected void afterOpen(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
    // we need to load the jokes in memory
    JokeReader jokeReader = new JokeReader();
    try {
      encryptedJokes = jokeReader.readAll("jokes.enc.json");
    } catch (IOException e) {
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
    EncryptedValue routedJoke = getMapperService().setupRoute(getComponentId(), currentJoke);

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
