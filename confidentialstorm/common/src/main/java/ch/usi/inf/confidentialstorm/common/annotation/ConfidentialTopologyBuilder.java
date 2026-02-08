package ch.usi.inf.confidentialstorm.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a static method in a Topology class that returns a {@code org.apache.storm.topology.TopologyBuilder}.
 * This method is used by the topology exporter to extract and secure the topology graph.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ConfidentialTopologyBuilder {
}
