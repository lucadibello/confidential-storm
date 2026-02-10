package ch.usi.inf.confidentialstorm.host.bolts.dp;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.DataPerturbationService;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationContributionEntryRequest;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.DataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import org.apache.storm.Config;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractDataPerturbationBolt extends ConfidentialBolt<DataPerturbationService> {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractDataPerturbationBolt.class);

    public AbstractDataPerturbationBolt() {
        super(DataPerturbationService.class);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String,Object> config = Objects.requireNonNullElse(super.getComponentConfiguration(), new HashMap<>());
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5); // configure tick tuples for scheduled snapshots
        return config;
    }

    @Override
    protected void processTuple(Tuple input, DataPerturbationService service) throws EnclaveServiceException {
        if (isTickTuple(input)) {
            // process tick tuple: return snapshot
            DataPerturbationSnapshot snapshot = service.getSnapshot();

            // do something else with the snapshot
            processSnapshot(snapshot.histogramSnapshot());
        } else {
            // update histogram via service API
            service.addContribution(new DataPerturbationContributionEntryRequest(
                    getUserIdEntry(input),
                    getWordEntry(input),
                    getClampedCountEntry(input)
            ));

            // acknowledge the tuple
            getCollector().ack(input);
        }
    }

    /**
     * Template method to extract the user ID entry from the input tuple.
     * This allows to customize the way the user ID is extracted.
     *
     * @param input the input tuple
     * @return the user ID entry as an encrypted value
     */
    protected abstract EncryptedValue getUserIdEntry(Tuple input);

    /**
     * Template method to extract the word entry from the input tuple.
     * This allows to customize the way the word is extracted.
     *
     * @param input the input tuple
     * @return the word entry as an encrypted value
     */
    protected abstract EncryptedValue getWordEntry(Tuple input);

    /**
     * Template method to extract the (clamped) count entry from the input tuple.
     * This allows to customize the way the count is extracted.
     *
     * @param input the input tuple
     * @return the clamped count entry as an encrypted value
     */
    protected abstract EncryptedValue getClampedCountEntry(Tuple input);


    /**
     * Template method to process the histogram snapshot obtained from the data perturbation service.
     *
     * @param histogramSnapshot the histogram snapshot as a map of word to count
     * @throws EnclaveServiceException if there is an error processing the snapshot
     */
    protected abstract void processSnapshot(Map<String, Long> histogramSnapshot) throws EnclaveServiceException;
}
