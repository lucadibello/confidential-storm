package ch.usi.inf.examples.simple_topology.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import ch.usi.inf.examples.simple_topology.common.api.SimpleEnclaveService;
import ch.usi.inf.examples.simple_topology.common.api.model.SimpleEnclaveRequest;
import ch.usi.inf.examples.simple_topology.common.api.model.SimpleEnclaveResponse;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleBolt extends ConfidentialBolt<SimpleEnclaveService> {

    private Logger LOG = LoggerFactory.getLogger(SimpleBolt.class);

    public SimpleBolt() {
        super(SimpleEnclaveService.class);
    }

    @Override
    protected void processTuple(Tuple input, SimpleEnclaveService service)
        throws EnclaveServiceException {
        // extract `num` from tuple
        float num = input.getFloat(0);

        // send request to TEE
        LOG.debug("Sending request with number", num);
        SimpleEnclaveResponse response = service.query(
            new SimpleEnclaveRequest(
                String.format("I just got %f as random value", num)
            )
        );

        if (response == null) {
            LOG.error(
                "Received null response from TEE. An error must have occurred."
            );
            return;
        }

        // read response (object + string)
        LOG.info("Received response from TEE: {}", response.response());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // NOP -> sink node
    }
}
