import java.math.BigInteger;
import java.util.BitSet;

public class betterFrequencyEstimator {

    private int availableSpace;
    private float pr1;
    private float epsilon;
    private float pr2;
    private int bloomFilterSize;
    private int k;
    private int w;
    private int d;
    private BitSet bloomFilter;
    private int[][] CMSketch;
    private int[] primes;

    private int nextPrime(int number){
        BigInteger big = BigInteger.valueOf(number);
        big = big.nextProbablePrime();
        return Integer.parseInt(big.toString());
    }

    public betterFrequencyEstimator(int availableSpace, float pr1, float epsilon, float pr2)
            throws InsufficientMemoryException {
        this.availableSpace = availableSpace;
        this.pr1 = pr1;
        this.pr2 = pr2;
        this.epsilon = epsilon;
        this.bloomFilterSize = (int)Math.floor(400000.*Math.log(pr1)/Math.log(0.6185));
        this.k = (int)Math.ceil(Math.log(2)*bloomFilterSize/400000.);
        this.w = (int)Math.ceil(Math.E/epsilon);
        this.d = (int) Math.ceil(Math.log(1/(1-pr2)));
        int numberOfPrimes = Math.max(d,k);
        if (bloomFilterSize + 4*w*d + 4* numberOfPrimes > availableSpace){
            throw new InsufficientMemoryException();
        }
        this.bloomFilter = new BitSet(bloomFilterSize);
        this.CMSketch = new int[d][w];
        this.primes = new int[numberOfPrimes];
        int temp = Math.max(bloomFilterSize,w);
        for (int i = 0; i < numberOfPrimes; i++) {
            temp = nextPrime(temp);
            primes[i] = temp;
        }
    }
    void addArrival(int key){
        for (int i = 0; i < k; i++) {
            bloomFilter.set((primes[i]*key) % bloomFilterSize < 0 ? (primes[i]*key) % bloomFilterSize +bloomFilterSize : (primes[i]*key) % bloomFilterSize,true);
        }
        for (int i = 0; i < d; i++) {
            CMSketch[i][(primes[i]*key) % w < 0 ? (primes[i]*key) % w +w : (primes[i]*key) % w] += 1;
        }
    }

    int getFreqEstimation(int key){
        boolean isInBloomFilter = true;
        for (int i = 0; i < k; i++) {
            isInBloomFilter = isInBloomFilter && bloomFilter.get((primes[i]*key) % bloomFilterSize < 0 ? (primes[i]*key) % bloomFilterSize +bloomFilterSize : (primes[i]*key) % bloomFilterSize);
        }
        if (!isInBloomFilter)
            return 0;
        int temp = Integer.MAX_VALUE;
        for (int i = 0; i < d; i++) {
            temp = Math.min(temp,CMSketch[i][(primes[i]*key) % w < 0 ? (primes[i]*key) % w +w : (primes[i]*key) % w]);
        }
        return temp;
    }

    public class InsufficientMemoryException extends  RuntimeException{}
}