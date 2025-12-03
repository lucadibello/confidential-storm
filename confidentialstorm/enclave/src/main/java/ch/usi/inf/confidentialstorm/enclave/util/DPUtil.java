package ch.usi.inf.confidentialstorm.enclave.util;

public class DPUtil {

    /**
     * Converts an (epsilon, delta)-DP guarantee into an equivalent rho-zCDP guarantee
     * (as used for the binary tree aggregation in the referenced DP streaming paper).
     *
     * @param eps   target epsilon (privacy loss parameter)
     * @param delta target delta (failure probability in (eps, delta)-DP)
     * @return rho parameter for zCDP satisfying the requested (eps, delta)-DP
     */
    public static double cdpRho(double eps, double delta) {
        if (eps < 0 || delta <= 0) {
            throw new IllegalArgumentException("epsilon must be non-negative and delta must be positive");
        }
        if (delta >= 1) return 0.0;

        double rho_min = 0.0;
        double rho_max = eps + 1;

        for (int i = 0; i < 1000; i++) {
            double rho = (rho_min + rho_max) / 2;
            if (cdpDelta(rho, eps) <= delta) {
                rho_min = rho;
            } else {
                rho_max = rho;
            }
        }
        return rho_min;
    }

    /**
     * Computes the delta that corresponds to a given rho-zCDP and epsilon bound,
     * per the zCDP to (epsilon, delta)-DP conversion in the DP streaming paper.
     *
     * @param rho zCDP privacy parameter
     * @param eps epsilon bound
     * @return the resulting delta value
     */
    private static double cdpDelta(double rho, double eps) {
        if (rho < 0 || eps < 0) {
            throw new IllegalArgumentException("rho and epsilon must be non-negative");
        }
        if (rho == 0) return 0.0;

        double amin = 1.01;
        double amax = (eps + 1) / (2 * rho) + 2;

        for (int i = 0; i < 1000; i++) {
            double alpha = (amin + amax) / 2;
            double derivative = (2 * alpha - 1) * rho - eps + Math.log1p(-1.0 / alpha);
            if (derivative < 0) {
                amin = alpha;
            } else {
                amax = alpha;
            }
        }

        double alpha = (amin + amax) / 2;
        double delta = Math.exp((alpha - 1) * (alpha * rho - eps) + alpha * Math.log1p(-1.0 / alpha)) / (alpha - 1.0);
        return Math.min(delta, 1.0);
    }

    /**
     * Computes the Gaussian sigma needed for binary tree aggregation under rho-zCDP
     * (see Algorithm 2 / binary tree aggregation in the paper).
     *
     * @param rho zCDP privacy parameter
     * @param T   number of releases/time steps supported by the tree
     * @param L   per-user L1 sensitivity (C * L_m in the paper)
     * @return standard deviation for the Gaussian noise
     */
    public static double calculateSigma(double rho, double T, double L) {
        return Math.sqrt((Math.log(T) * L * L) / ((2 * rho)));
    }
}
