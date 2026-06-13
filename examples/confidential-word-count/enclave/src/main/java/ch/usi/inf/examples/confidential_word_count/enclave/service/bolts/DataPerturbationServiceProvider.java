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
        // Privacy budget split: 50% for contribution bounding, 50% for histogram perturbation
        return DPConfig.EPSILON / 2.0;
    }

    @Override
    public double getEpsilonH() {
        // Privacy budget split: 50% for contribution bounding, 50% for histogram perturbation
        return DPConfig.EPSILON / 2.0;
    }

    @Override
    public double getDeltaK() {
        // Delta budget split: 2/3 for contribution bounding, 1/3 for histogram perturbation
        return (2.0 / 3.0) * DPConfig.DELTA;
    }

    @Override
    public double getDeltaH() {
        // Delta budget split: 2/3 for contribution bounding, 1/3 for histogram perturbation
        return (1.0 / 3.0) * DPConfig.DELTA;
    }

    @Override
    public long getMu() {
        // Parameter for the Gaussian mechanism, controlling noise added to histogram counts
        return 15L;
    }

    @Override
    public int getMaxTimeSteps() {
        /*
         * Fewer time steps (10-12) reduce noise accumulation and improve utility compared to 24 time steps.
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
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.BOLT_DATA_PERTURBATION;
    }
}
