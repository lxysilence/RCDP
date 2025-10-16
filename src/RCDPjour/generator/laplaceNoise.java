package RCDPjour.generator;
import java.util.Random;

public class laplaceNoise {
    private static final double PI = Math.PI;
    private static final Random random = new Random();

    /**
     * Generates a random point from the Laplace distribution.
     * @param lambda     The scale parameter of the Laplace distribution.
     * @return A random point from the Laplace distribution.
     */
    public static double generateLaplaceRandom(double lambda) {
        double u = random.nextDouble() - 0.5; // Generate a uniformly distributed random number between -0.5 and 0.5
        return  - lambda * Math.signum(u) * Math.log(1 - 2 * Math.abs(u)); // Apply inverse transform sampling to get Laplace random variable
    }


}
