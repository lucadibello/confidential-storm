package ch.usi.inf.confidentialstorm.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.model.SplitSentenceRequest;
import ch.usi.inf.confidentialstorm.common.model.SplitSentenceResponse;
import ch.usi.inf.confidentialstorm.host.bolts.base.ConfidentialBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SplitSentenceBolt extends ConfidentialBolt<SplitSentenceService> {
    private int boltId;
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public SplitSentenceBolt() {
        super(SplitSentenceService.class);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        super.afterPrepare(topoConf, context);
        this.boltId = context.getThisTaskId();
        LOG.info("[SplitSentenceBolt {}] Prepared with componentId {}", boltId, context.getThisComponentId());
    }

    @Override
    protected void processTuple(Tuple input, SplitSentenceService service) {
        String jokeBody = input.getStringByField("body");
        // request enclave to split the sentence
        SplitSentenceResponse response = service.split(new SplitSentenceRequest(jokeBody));
        LOG.info("[SplitSentenceBolt {}]: Found {} words in sentence {}", boltId, response.words().size(), jokeBody.substring(0, Math.min(50, jokeBody.length())) + (jokeBody.length() > 50 ? "..." : ""));

        // print each word found, each on the same line joined by a comma
        StringBuilder wordsStr = new StringBuilder();
        response.words().forEach(word -> wordsStr.append(word).append(", "));
        if (!wordsStr.isEmpty()) {
            wordsStr.setLength(wordsStr.length() - 2); // remove last comma and space
        }
        LOG.debug("[SplitSentenceBolt {}]: Words: {}", boltId, wordsStr);

        // pass results to the next bolt
        response.words().forEach(word -> collector.emit(input, new Values(word)));
        collector.ack(input);
    }

    @Override
    protected void beforeCleanup() {
        super.beforeCleanup();
        LOG.info("[SplitSentenceBolt {}] Cleaning up.", boltId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
