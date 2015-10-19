package org.wso2.siddhi.bloomfilter;

public class BloomFilterUtil {

    private BloomFilterUtil() {
    }

    /**
     * Calculates the optimal size of the bloom filter in bits, given
     * noOfElements and falsePositiveRate
     *
     * @param noOfElements
     *            Expected number of elements inserted in the bloom filter
     * @param falsePositiveRate
     *            Tolerable false positive rate
     * @return Optimal Size of the bloom filter in bits
     */
    public static int optimalBloomFilterSize(long noOfElements, double falsePositiveRate) {
        return (int) Math.ceil(-1 * (noOfElements * Math.log(falsePositiveRate)) /
                Math.pow(Math.log(2), 2));
    }

    /**
     * Calculate the optimal number of hash functions for the bloom filter using
     * bloom filter size and no of elements inserted
     *
     * @param bloomFilterSize
     *            Bloom Filter vector size
     * @param noOfElements
     *            Expected number of elements inserted to the filter
     * @return Optimal number of hash functions
     */

    public static int optimalNoOfHash(int bloomFilterSize, long noOfElements) {
        return (int) Math.round(bloomFilterSize / noOfElements * Math.log(2));
    }

}