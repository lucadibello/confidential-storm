package ch.usi.inf.examples.synthetic_dp.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.bounding.UserContributionBoundingService;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp.AbstractUserContributionBoundingServiceProvider;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

@AutoService(UserContributionBoundingService.class)
public class SyntheticUserContributionBoundingServiceProvider
        extends AbstractUserContributionBoundingServiceProvider {

    @Override
    protected double getPerRecordClamp() {
        return DPConfig.PER_RECORD_CLAMP;
    }

    @Override
    protected long getMaxContributionsPerUser() {
        return DPConfig.MAX_CONTRIBUTIONS_PER_USER;
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING;
    }
}
