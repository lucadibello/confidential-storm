package ch.usi.inf.confidentialstorm.enclave.dp;

public class TestBinaryTrees {
    public static void main(String[] args) {
        System.out.println("Testing Binary Aggregation Trees...");

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
            System.out.println("Adding value " + value + " at index " + i);

            double theirOutput = theirs.addToTree(i, value);
            System.out.println("  Theirs output: " + theirOutput);

            double mineOutput = mine.addToTree(i, value);
            System.out.println("  Mine output: " + mineOutput);

            // compare outputs
            if (Math.abs(mineOutput - theirOutput) > 1e-9)
                throw new RuntimeException("Outputs differ at index " + i + ": mine=" + mineOutput + ", theirs=" + theirOutput);
        }

        System.out.println("All outputs match!");
    }
}
