package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.HistogramAggregationService;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp.AbstractHistogramAggregationServiceProvider;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

@AutoService(HistogramAggregationService.class)
public class HistogramAggregationServiceProvider extends AbstractHistogramAggregationServiceProvider {

    @Override
    protected int getExpectedReplicaCount() {
        return 2; // matches DataPerturbationBolt parallelism in the topology
    }

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return ComponentConstants.BOLT_DATA_PERTURBATION;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return ComponentConstants.BOLT_HISTOGRAM_AGGREGATION;
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.BOLT_HISTOGRAM_AGGREGATION;
    }
}
