package ch.usi.inf.confidentialstorm.host.bolts.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.HistogramAggregationService;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationRequest;
import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.model.HistogramAggregationResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Host-side bolt that receives encrypted partial histograms from DataPerturbation replicas
 * and delegates merging to an enclave-based {@link HistogramAggregationService}.
 * <p>
 * When all expected partials have been received, {@link #processCompleteHistogram} is called
 * with the merged result.
 */
public abstract class AbstractHistogramAggregationBolt extends ConfidentialBolt<HistogramAggregationService> {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractHistogramAggregationBolt.class);

    public AbstractHistogramAggregationBolt() {
        super(HistogramAggregationService.class);
    }

    /**
     * Extract the encrypted partial histogram from the input tuple.
     */
    protected abstract EncryptedValue getEncryptedPartialHistogram(Tuple input);

    /**
     * Called when all partial histograms have been merged for a round.
     *
     * @param mergedHistogram the complete merged histogram
     * @throws EnclaveServiceException if there is an error processing the histogram
     */
    protected abstract void processCompleteHistogram(Map<String, Long> mergedHistogram) throws EnclaveServiceException;

    @Override
    protected void processTuple(Tuple input, HistogramAggregationService service) throws EnclaveServiceException {
        EncryptedValue partial = getEncryptedPartialHistogram(input);

        HistogramAggregationRequest request = new HistogramAggregationRequest(partial);
        HistogramAggregationResponse response = service.mergePartial(request);

        if (response.complete()) {
            LOG.info("[HistogramAggregation] Round complete with {} keys", response.mergedHistogram().size());
            processCompleteHistogram(response.mergedHistogram());
        } else {
            LOG.debug("[HistogramAggregation] Partial received ({}/{})",
                    response.receivedCount(), response.expectedCount());
        }

        getCollector().ack(input);
    }
}
