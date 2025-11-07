package ch.usi.inf.confidentialstorm.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.model.SplitSentenceRequest;
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
    }

    @Override
    protected void processTuple(Tuple input, SplitSentenceService service) {
        String jokeBody = input.getStringByField("body");
        var response = service.split(new SplitSentenceRequest(jokeBody));
        LOG.info("[SplitSentenceBolt {}]: Found {} words in sentence.", boltId, response.words().size());
        response.words().forEach(word -> collector.emit(input, new Values(word)));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
