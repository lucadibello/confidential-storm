package ch.usi.inf.confidentialstorm.common.topology;

import java.util.List;

public interface TopologyProvider {
    List<TopologySpecification.Component> getDownstream(TopologySpecification.Component component);
}
