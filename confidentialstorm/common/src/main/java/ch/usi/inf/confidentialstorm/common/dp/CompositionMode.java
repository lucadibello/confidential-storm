package ch.usi.inf.confidentialstorm.common.dp;

/**
 * Which $C$-fold composition theorem to use when deriving the per-round
 * key-selection budget of Algorithm 1 (Streaming Private Key Selection) from
 * the total key-selection budget $(\varepsilon_k, \delta_k)$.
 * <p>
 * A user may participate in at most $C$ rounds of key selection, so the total
 * budget must be composed over $C$ rounds. The three modes differ only in how
 * the per-round Gaussian-noise scale $\sigma_k$ is obtained:
 * <ul>
 * <li>{@link #DWORK_ANALYTICAL}: advanced-composition bound from
 * Dwork-Rothblum-Vadhan, applied to $(\varepsilon_k, \delta_k)$-DP per round.
 * Goes through $(\varepsilon, \delta)$-DP throughout and converts to $\rho$
 * only at the Gaussian-calibration step.</li>
 * <li>{@link #OPTIMAL_KOV}: Kairouz-Oh-Viswanath optimal $k$-fold composition
 * (the "empirically tighter variant of advanced composition" referenced in
 * Section 4.4 of the DP-SQLP paper). Same shape as {@link #DWORK_ANALYTICAL}
 * but with a tighter per-round $\varepsilon$.</li>
 * <li>{@link #ZCDP_LINEAR}: skip the $(\varepsilon, \delta)$-DP composition
 * entirely. Convert the total $(\varepsilon_k, \delta_k)$ directly to
 * $\rho_k$-zCDP and split linearly across the $C$ rounds
 * ($\rho_k^{(r)} = \rho_k / C$). This is strictly tighter than the other two
 * because the $(\varepsilon, \delta) \to \rho$ conversion at the
 * Gaussian-calibration step is lossy, and is therefore the recommended
 * default.</li>
 * </ul>
 */
public enum CompositionMode {
  /** Dwork-Rothblum-Vadhan advanced composition over $(\varepsilon, \delta)$-DP. */
  DWORK_ANALYTICAL,
  /** Kairouz-Oh-Viswanath optimal $k$-fold composition over $(\varepsilon, \delta)$-DP. */
  OPTIMAL_KOV,
  /** Direct $\rho$-zCDP conversion with linear split across rounds (tightest). */
  ZCDP_LINEAR
}
