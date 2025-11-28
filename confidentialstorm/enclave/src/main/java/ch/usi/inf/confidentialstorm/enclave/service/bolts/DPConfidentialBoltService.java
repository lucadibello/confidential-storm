package ch.usi.inf.confidentialstorm.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.enclave.dp.BinaryAggregationForest;

public abstract class DPConfidentialBoltService<T> extends ConfidentialBoltService<T> {

    protected final BinaryAggregationForest aggregationForest;

    public DPConfidentialBoltService(int nKeys, int numTriggers, Double L, Double eps, Double delta) {
        this.aggregationForest = new BinaryAggregationForest(nKeys, numTriggers, L, eps, delta);
    }
}
