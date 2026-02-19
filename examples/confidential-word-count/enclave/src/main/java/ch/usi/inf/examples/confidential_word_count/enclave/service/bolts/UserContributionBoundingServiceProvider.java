package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.bounding.UserContributionBoundingService;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp.AbstractUserContributionBoundingServiceProvider;
import ch.usi.inf.examples.confidential_word_count.common.config.DPConfig;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

@AutoService(UserContributionBoundingService.class)
public class UserContributionBoundingServiceProvider
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
    public TopologySpecification.Component expectedSourceComponent() {
        return ComponentConstants.BOLT_SENTENCE_SPLIT;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return ComponentConstants.BOLT_DATA_PERTURBATION;
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING;
    }
}
