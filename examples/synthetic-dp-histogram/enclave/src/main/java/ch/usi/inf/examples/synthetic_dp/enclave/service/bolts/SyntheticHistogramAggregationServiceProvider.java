package ch.usi.inf.examples.synthetic_dp.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.aggregation.HistogramAggregationService;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp.AbstractHistogramAggregationServiceProvider;
import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

@AutoService(HistogramAggregationService.class)
public class SyntheticHistogramAggregationServiceProvider extends AbstractHistogramAggregationServiceProvider {

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.BOLT_HISTOGRAM_AGGREGATION;
    }
}
