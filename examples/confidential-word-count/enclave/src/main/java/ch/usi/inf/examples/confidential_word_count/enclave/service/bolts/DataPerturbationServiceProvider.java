package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.DataPerturbationService;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp.AbstractDataPerturbationServiceProvider;
import ch.usi.inf.examples.confidential_word_count.common.config.DPConfig;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

@AutoService(DataPerturbationService.class)
public class DataPerturbationServiceProvider extends AbstractDataPerturbationServiceProvider {

    @Override
    public double getEpsilonK() {
        // privacy budget split (50% for contribution bounding, 50% for histogram perturbation)
        return DPConfig.EPSILON / 2.0;
    }

    @Override
    public double getEpsilonH() {
        // privacy budget split (50% for contribution bounding, 50% for histogram perturbation)
        return DPConfig.EPSILON / 2.0;
    }

    @Override
    public double getDeltaK() {
        // contribution bounding gets 2/3 of the delta budget, histogram perturbation gets 1/3 of the delta budget
        return (2.0 / 3.0) * DPConfig.DELTA;
    }

    @Override
    public double getDeltaH() {
        // contribution bounding gets 2/3 of the delta budget, histogram perturbation gets 1/3 of the delta budget
        return (1.0 / 3.0) * DPConfig.DELTA;
    }

    @Override
    public long getMu() {
        // the "mu" parameter for the Gaussian mechanism, which controls the amount of noise added to the histogram counts
        return 15L;
    }

    @Override
    public int getMaxTimeSteps() {
        /*
         * NOTE: This example runs for 120 seconds with releases every 5 seconds = 24 time steps.
         * However, using fewer time steps (10-12) reduces noise accumulation and improves utility.
         */
        return 12;
    }

    @Override
    public double getPerRecordClamp() {
        return DPConfig.PER_RECORD_CLAMP;
    }
    @Override
    public long getMaxUserContributions() {
        return DPConfig.MAX_CONTRIBUTIONS_PER_USER;
    }

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return ComponentConstants.BOLT_HISTOGRAM_AGGREGATION;
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.BOLT_DATA_PERTURBATION;
    }
}
