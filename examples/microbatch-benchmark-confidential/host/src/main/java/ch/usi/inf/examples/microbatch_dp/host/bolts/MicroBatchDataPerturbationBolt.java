package ch.usi.inf.examples.microbatch_dp.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.DataPerturbationService;
import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model.EncryptedDataPerturbationSnapshot;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.dp.AbstractDataPerturbationBolt;
import ch.usi.inf.examples.microbatch_dp.common.config.DPConfig;
import ch.usi.inf.examples.microbatch_dp.common.topology.BatchMarker;
import ch.usi.inf.examples.microbatch_dp.common.topology.ComponentConstants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Confidential micro-batch data perturbation bolt.
 *
 * <p>Reuses the framework's {@link AbstractDataPerturbationBolt} for the
 * encrypted addContribution path but neutralises its tick-based snapshot
 * release by setting an effectively-infinite tick interval. The micro-batch
 * benchmark times batches via BEGIN/END markers flowing on the dedicated
 * control stream; the aggregator records start/stop wall clocks on those
 * markers. Data-stream partial histograms are therefore never emitted in this
 * mode, which keeps timing strictly tied to control-marker delivery and
 * avoids consuming the DP epoch budget over the duration of the run.
 */
public class MicroBatchDataPerturbationBolt extends AbstractDataPerturbationBolt {

    private static final Logger LOG = LoggerFactory.getLogger(MicroBatchDataPerturbationBolt.class);
    private static final int TICK_DISABLED_SECS = Integer.MAX_VALUE / 4;

    private int maxEpochs;

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        this.maxEpochs = ((Number) topoConf.getOrDefault("dp.max.time.steps", DPConfig.maxTimeSteps())).intValue();
        super.afterPrepare(topoConf, context);
        LOG.info("[MicroBatchDataPerturbation] tick disabled (interval={}s), maxEpochs={}, CONFIG={}",
                TICK_DISABLED_SECS, maxEpochs, DPConfig.describe());
    }

    @Override
    protected int getMaxEpochs() {
        return maxEpochs;
    }

    @Override
    protected int getTickIntervalSecs() {
        return TICK_DISABLED_SECS;
    }

    @Override
    protected boolean useEncryptedSnapshots() {
        return true;
    }

    @Override
    protected void processEncryptedSnapshot(EncryptedDataPerturbationSnapshot snapshot) {
        getCollector().emit(new Values(snapshot.encryptedHistogram()));
    }

    @Override
    protected void processSnapshot(Map<String, Long> histogramSnapshot) {
        throw new UnsupportedOperationException("This bolt uses encrypted snapshots");
    }

    @Override
    protected EncryptedValue getUserIdEntry(Tuple input) {
        return (EncryptedValue) input.getValueByField("userId");
    }

    @Override
    protected EncryptedValue getWordEntry(Tuple input) {
        return (EncryptedValue) input.getValueByField("word");
    }

    @Override
    protected EncryptedValue getClampedCountEntry(Tuple input) {
        return (EncryptedValue) input.getValueByField("count");
    }

    @Override
    protected void processTuple(Tuple input, DataPerturbationService service) throws EnclaveServiceException {
        if (ComponentConstants.CONTROL_STREAM.equals(input.getSourceStreamId())) {
            getCollector().emit(ComponentConstants.CONTROL_STREAM, input,
                    new Values(input.getStringByField(BatchMarker.F_TYPE),
                            input.getIntegerByField(BatchMarker.F_BATCH_ID),
                            input.getDoubleByField(BatchMarker.F_SIZE_GB),
                            input.getLongByField(BatchMarker.F_RECORD_COUNT),
                            input.getLongByField(BatchMarker.F_BYTES_PER_TUPLE),
                            input.getLongByField(BatchMarker.F_T_NANOS),
                            input.getLongByField(BatchMarker.F_T_EPOCH_MS)));
            getCollector().ack(input);
            return;
        }
        super.processTuple(input, service);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("encryptedHistogram"));
        declarer.declareStream(ComponentConstants.CONTROL_STREAM, BatchMarker.fields());
    }
}
