import java.math.BigInteger;
import java.util.ArrayList;
import java.util.BitSet;

class rangeBF {
    private double pr;
    private ArrayList<BitSet> filters;
    private int[] primes;
    private int bloomFilterSize;
    private int k;

    public rangeBF(double pr){
        this.pr = pr;
        this.filters = new ArrayList<>();
        this.bloomFilterSize = (int)Math.floor(400000.*Math.log(pr)/Math.log(0.6185));
        this.k = (int)Math.ceil(Math.log(2)*bloomFilterSize/400000.);
        for (int i = 0; i < 32; i++) {
            filters.add(addLayer(i));
        }
        this.primes = new int[k];
        int temp = bloomFilterSize;
        for (int i = 0; i < k; i++) {
            temp = nextPrime(temp);
            primes[i] = temp;
        }
    }

    void insertValue(int key){
        for (int i = 0; i < 32; i++) {
            insertInFilter(i,key);
        }
    }

    boolean existsInRange(int l, int r){
        ArrayList<int[]> intervals = new ArrayList<>();
        getIntervals(l,r,intervals);
        for (int[] interval : intervals){
            if (checkFilter(interval[0],interval[1]))
                return true;
        }
        return false;
    }

    private BitSet addLayer(int i){
        if (Math.pow(2,32-i)<bloomFilterSize)
            return new BitSet((int)Math.pow(2,32-i));
        return new BitSet(bloomFilterSize);
    }

    private void insertInFilter(int i, int key){
        int newKey = key;
        if (i>0)
            newKey = key / 2 + Integer.MAX_VALUE / 2;
        if (Math.pow(2,32-i)<bloomFilterSize) {
            filters.get(i).set((int) Math.floor(newKey / Math.pow(2, i-1)), true);
        }
        else{
            if (i>0)
                newKey = (int)Math.floor(newKey/Math.pow(2,i-1));
            for (int j = 0; j < k; j++) {
                filters.get(i).set((primes[j]*newKey) % bloomFilterSize < 0 ? (primes[j]*newKey) % bloomFilterSize +bloomFilterSize : (primes[j]*newKey) % bloomFilterSize, true);
            }
        }
    }

    private int nextPrime(int number){
        BigInteger big = BigInteger.valueOf(number);
        big = big.nextProbablePrime();
        return Integer.parseInt(big.toString());
    }

    private static void getIntervals(int l, int r, ArrayList<int[]> intervals) {
        int[] interval = new int[2];
        int i = 0;
        double x = (l - 1) / Math.pow(2, i);

        while (x == Math.floor(x) && (x+1)*Math.pow(2,i) <= r) {
            i++;
            x = (l - 1) / Math.pow(2, i);
        }
        i--;
        x = (l - 1) / Math.pow(2, i);
        interval[0] = (int)x;
        interval[1] = i;
        intervals.add(interval);
        if ((int) ((x+1)*Math.pow(2,i)) != r){
            getIntervals((int) ((x+1)*Math.pow(2,i)+1), r, intervals);
        }
    }

    private boolean checkFilter(int key, int level){
        boolean isInFilter = true;
        int newKey = key;
        if (level>0)
            newKey = key / 2 + Integer.MAX_VALUE / 2;
        if (Math.pow(2,32-level)<bloomFilterSize)
           isInFilter = filters.get(level).get((int) Math.floor(newKey/Math.pow(2,level-1)));
        else {
            for (int i = 0; i < primes.length; i++) {
                if (level > 0)
                    newKey = (int)Math.floor(newKey/Math.pow(2,level-1));
                isInFilter = isInFilter && filters.get(level).get((primes[i]*newKey) % bloomFilterSize < 0 ? (primes[i]*newKey) % bloomFilterSize +bloomFilterSize : (primes[i]*newKey) % bloomFilterSize);
            }
        }
        return isInFilter;
    }
}
