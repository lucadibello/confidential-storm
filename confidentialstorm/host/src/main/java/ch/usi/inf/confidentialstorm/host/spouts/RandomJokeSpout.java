package ch.usi.inf.confidentialstorm.host.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.usi.inf.confidentialstorm.host.util.JokeReader;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomJokeSpout implements IRichSpout {
  private static final long EMIT_DELAY_MS = 250;
  private SpoutOutputCollector collector;
  private List<JokeReader.Joke> jokes;
  private Random rand;
  private int taskId;
  private final Logger LOG = LoggerFactory.getLogger(getClass());

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return Map.of();
  }

  @Override
  public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
    // we need to load the jokes in memory
    JokeReader jokeReader = new JokeReader();
    try {
        jokes = jokeReader.readAll("jokes.json");
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
    // save the collector for emitting tuples <Joke, String>
    this.collector = collector;
    this.rand = new Random();
    this.taskId = context.getThisTaskId();
    LOG.info("[RandomJokeSpout {}] Prepared with delay {} ms", taskId, EMIT_DELAY_MS);
  }

  @Override
  public void close() {
  }

  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void nextTuple() {
    // generate the next random joke
    int idx = rand.nextInt(jokes.size());
    JokeReader.Joke curr = jokes.get(idx);
    LOG.info("[RandomJokeSpout {}] Emitting joke {}", taskId, curr);
    collector.emit(new Values(curr.id, curr.category, curr.rating, curr.body));
    // sleep for a while to avoid starving the topology
    try {
        Thread.sleep(EMIT_DELAY_MS);
    } catch (InterruptedException e) {
        // do nothing
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id", "category", "rating", "body"));
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
    // Implementation here
  }

}
