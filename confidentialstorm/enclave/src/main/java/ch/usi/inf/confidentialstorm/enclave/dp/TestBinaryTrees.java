package ch.usi.inf.confidentialstorm.enclave.dp;

import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLoggerFactory;

public class TestBinaryTrees {
    private static final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(TestBinaryTrees.class);

    public static void main(String[] args) {
        LOG.info("Testing Binary Aggregation Trees...");

        // initialize both trees
        int T = 1024; // number of leaf nodes in the tree
        double sigma = 0; // needed for reproducibility

        // create both kind of trees
        BinaryAggregationTree mine = new BinaryAggregationTree(T, sigma);
        BinaryAggregationTreeOriginal theirs = new BinaryAggregationTreeOriginal(T, sigma);

        // compute the max size that we can test given the tree height T
        // NOTE: the height of the tree is log2(T), and the max number of nodes is 2^log2(T) - 1 = T - 1
        int maxSize = T - 1;

        // fill both trees with the same data
        for (int i = 0; i < maxSize; i++) {
            double value = i + 1; // some deterministic data
            LOG.info("Adding value {} at index {}", value, i);

            double theirOutput = theirs.addToTree(i, value);
            LOG.debug("  Theirs output: {}", theirOutput);

            double mineOutput = mine.addToTree(i, value);
            LOG.debug("  Mine output: {}", mineOutput);

            // compare outputs
            if (Math.abs(mineOutput - theirOutput) > 1e-9)
                throw new RuntimeException("Outputs differ at index " + i + ": mine=" + mineOutput + ", theirs=" + theirOutput);
        }

        LOG.info("All outputs match!");
    }
}
