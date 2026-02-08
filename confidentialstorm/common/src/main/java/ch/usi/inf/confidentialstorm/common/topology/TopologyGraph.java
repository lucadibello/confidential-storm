package ch.usi.inf.confidentialstorm.common.topology;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * DTO for storing the topology graph structure.
 */
public class TopologyGraph implements Serializable {
    private Map<String, List<String>> adjacencyList;

    public TopologyGraph() {
    }

    public TopologyGraph(Map<String, List<String>> adjacencyList) {
        this.adjacencyList = adjacencyList;
    }

    public Map<String, List<String>> getAdjacencyList() {
        return adjacencyList;
    }

    public void setAdjacencyList(Map<String, List<String>> adjacencyList) {
        this.adjacencyList = adjacencyList;
    }
}
