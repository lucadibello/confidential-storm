package ch.usi.inf.confidentialstorm.enclave.dp;

import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;

import java.util.ArrayList;
import java.util.List;

public class BinaryAggregationForest {
    private final ArrayList<BinaryAggregationTree> trees = new ArrayList<>();

    public BinaryAggregationForest(int n_keys, int n, double eps, double delta, double L) {
        // compute rho given eps and delta
        double rho = DPUtil.cdpRho(eps, delta);
        // Compute sigma
        double sigma = DPUtil.calculateSigma(rho, n, L);

        // Create trees
        for (int i = 0; i < n_keys; i++) {
            trees.add(new BinaryAggregationTree(n, sigma));
        }
    }

    public List<Double> getTree(int key) {
        return trees.get(key).getTree();
    }

    public Double addToTree(int key, int index, Double value) {
        return trees.get(key).addToTree(index, value);
    }
}
