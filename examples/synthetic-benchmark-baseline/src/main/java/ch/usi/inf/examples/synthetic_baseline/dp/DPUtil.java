package ch.usi.inf.examples.synthetic_baseline.dp;

import org.apache.commons.math3.util.FastMath;

/**
 * Utility class for Differential Privacy (DP) calculations.
 * (copy of confidentialstorm/enclave)
 */
public class DPUtil {

    /**
     * Converts an (epsilon, delta)-DP guarantee into an equivalent rho-zCDP guarantee.
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

    private static double cdpDelta(double rho, double eps) {
        if (rho < 0 || eps < 0) {
            throw new IllegalArgumentException("rho and epsilon must be non-negative");
        }
        if (rho == 0) return 0.0;

        double amin = 1.01;
        double amax = (eps + 1) / (2 * rho) + 2;

        for (int i = 0; i < 1000; i++) {
            double alpha = (amin + amax) / 2;
            double derivative = (2 * alpha - 1) * rho - eps + FastMath.log1p(-1.0 / alpha);
            if (derivative < 0) {
                amin = alpha;
            } else {
                amax = alpha;
            }
        }

        double alpha = (amin + amax) / 2;
        double delta = FastMath.exp((alpha - 1) * (alpha * rho - eps) + alpha * FastMath.log1p(-1.0 / alpha)) / (alpha - 1.0);
        return FastMath.min(delta, 1.0);
    }

    /**
     * Computes the Gaussian sigma needed for binary tree aggregation under rho-zCDP.
     */
    public static double calculateSigma(double rho, double T, double L) {
        double log2T = FastMath.log(T) / FastMath.log(2);
        return FastMath.sqrt((log2T * L * L) / ((2 * rho)));
    }

    /**
     * Returns the user-level L1 sensitivity C * L_m.
     */
    public static double l1Sensitivity(long maxContributionsPerUser, double perRecordClamp) {
        return maxContributionsPerUser * perRecordClamp;
    }
}
