package ch.usi.inf.examples.synthetic_dp.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.perturbation.DataPerturbationService;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp.AbstractDataPerturbationServiceProvider;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

@AutoService(DataPerturbationService.class)
public class SyntheticDataPerturbationServiceProvider extends AbstractDataPerturbationServiceProvider {

    @Override
    public double getEpsilonK() {
        return DPConfig.EPSILON / 2.0;
    }

    @Override
    public double getEpsilonH() {
        return DPConfig.EPSILON / 2.0;
    }

    @Override
    public double getDeltaK() {
        return (2.0 / 3.0) * DPConfig.DELTA;
    }

    @Override
    public double getDeltaH() {
        return (1.0 / 3.0) * DPConfig.DELTA;
    }

    @Override
    public long getMu() {
        return DPConfig.mu();
    }

    @Override
    public int getMaxTimeSteps() {
        return DPConfig.maxTimeSteps();
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
