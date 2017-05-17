/**
 *
 */
package com.taobao.top.analysis.util.bloom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author fangweng
 * @email fangweng@taobao.com
 * @date 2011-3-16
 * <p>
 * Implements a Bloom filter, as defined by Bloom in 1970. The Bloom
 * filter is a data structure that was introduced in 1970 and that has
 * been adopted by the networking research community in the past decade
 * thanks to the bandwidth efficiencies that it offers for the
 * transmission of set membership information between networked hosts. A
 * sender encodes the information into a bit vector, the Bloom filter,
 * that is more compact than a conventional representation. Computation
 * and space costs for construction are linear in the number of elements.
 * The receiver uses the filter to test whether various elements are
 * members of the set. Though the filter will occasionally return a false
 * positive, it will never return a false negative. When creating the
 * filter, the sender can choose its desired point in a trade-off between
 * the false positive rate and the size.
 */
public class ByteBloomFilter implements BloomFilter {

    public static final int VERSION = 1;
    protected int byteSize;
    protected final int hashCount;
    protected Hash hash;
    protected AtomicInteger keyCount = new AtomicInteger(0);
    protected int maxKeys;
    protected ByteBuffer bloom;

    private static final byte[] bitvals = {(byte) 0x01, (byte) 0x02,
            (byte) 0x04, (byte) 0x08, (byte) 0x10, (byte) 0x20, (byte) 0x40,
            (byte) 0x80};

    /**
     * Loads bloom filter meta data from file input.
     *
     * @param meta
     * @throws IllegalArgumentException
     */
    public ByteBloomFilter(ByteBuffer meta) throws IllegalArgumentException {
        int version = meta.getInt();
        if (version != VERSION)
            throw new IllegalArgumentException("Bad version");

        this.byteSize = meta.getInt();
        this.hashCount = meta.getInt();
        this.keyCount = new AtomicInteger(meta.getInt());
        this.maxKeys = this.keyCount.intValue();

        this.hash = MurmurHash.getInstance();
        sanityCheck();

        allocBloom();
    }

    public ByteBloomFilter(int maxKeys, float errorRate,
                           int foldFactor) throws IllegalArgumentException {
        /*
		 * Bloom filters are very sensitive to the number of elements inserted
		 * into them. For HBase, the number of entries depends on the size of
		 * the data stored in the column. Currently the default region size is
		 * 256MB, so entry count ~= 256MB / (average value size for column).
		 * Despite this rule of thumb, there is no efficient way to calculate
		 * the entry count after compactions. Therefore, it is often easier to
		 * use a dynamic bloom filter that will add extra space instead of
		 * allowing the error rate to grow.
		 * 
		 * ( http://www.eecs.harvard.edu/~michaelm/NEWWORK/postscripts/
		 * BloomFilterSurvey.pdf )
		 * 
		 * m denotes the number of bits in the Bloom filter (bitSize) n denotes
		 * the number of elements inserted into the Bloom filter (maxKeys) k
		 * represents the number of hash functions used (nbHash) e represents
		 * the desired false positive rate for the bloom (err)
		 * 
		 * If we fix the error rate (e) and know the number of entries, then the
		 * optimal bloom size m = -(n * ln(err) / (ln(2)^2) ~= n * ln(err) /
		 * ln(0.6185)
		 * 
		 * The probability of false positives is minimized when k = m/n ln(2).
		 */
        int bitSize = (int) Math.ceil(maxKeys
                * (Math.log(errorRate) / Math.log(0.6185)));
        int functionCount = (int) Math.ceil(Math.log(2) * (bitSize / maxKeys));

        // increase byteSize so folding is possible
        int byteSize = (bitSize + 7) / 8;
        int mask = (1 << foldFactor) - 1;
        if ((mask & byteSize) != 0) {
            byteSize >>= foldFactor;
            ++byteSize;
            byteSize <<= foldFactor;
        }

        this.byteSize = byteSize;
        this.hashCount = functionCount;
        this.hash = MurmurHash.getInstance();
        this.keyCount = new AtomicInteger(0);
        this.maxKeys = maxKeys;

        sanityCheck();

        allocBloom();
    }

    @Override
    public void setHash(Hash hash) {
        this.hash = hash;
    }

    void sanityCheck() throws IllegalArgumentException {
        if (this.byteSize <= 0) {
            throw new IllegalArgumentException("maxValue must be > 0");
        }

        if (this.hashCount <= 0) {
            throw new IllegalArgumentException(
                    "Hash function count must be > 0");
        }

        if (this.hash == null) {
            throw new IllegalArgumentException("hashType must be known");
        }

        if (this.keyCount.intValue() < 0) {
            throw new IllegalArgumentException("must have positive keyCount");
        }
    }

    void bloomCheck(ByteBuffer bloom) throws IllegalArgumentException {
        if (this.byteSize != bloom.limit()) {
            throw new IllegalArgumentException(
                    "Configured bloom length should match actual length");
        }
    }

    @Override
    public void allocBloom() {
        //不做并发保护
        if (this.bloom != null) {
            throw new IllegalArgumentException("can only create bloom once.");
        }
        this.bloom = ByteBuffer.allocate(this.byteSize);
        assert this.bloom.hasArray();
    }

    @Override
    public void add(byte[] buf) {
        add(buf, 0, buf.length);
    }

    @Override
    public void add(byte[] buf, int offset, int len) {
		/*
		 * For faster hashing, use combinatorial generation
		 * https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
		 */
        int hash1 = this.hash.hash(buf, offset, len, 0);
        int hash2 = this.hash.hash(buf, offset, len, hash1);

        for (int i = 0; i < this.hashCount; i++) {
            int hashLoc = Math.abs((hash1 + i * hash2) % (this.byteSize * 8));
            set(hashLoc);
        }

        this.keyCount.incrementAndGet();
    }


    @Override
    public boolean contains(byte[] buf) {
        return contains(buf, 0, buf.length);
    }

    @Override
    public boolean contains(byte[] buf, int offset, int length) {
        if (bloom.limit() != this.byteSize) {
            throw new IllegalArgumentException(
                    "Bloom does not match expected size");
        }

        int hash1 = this.hash.hash(buf, offset, length, 0);
        int hash2 = this.hash.hash(buf, offset, length, hash1);

        for (int i = 0; i < this.hashCount; i++) {
            int hashLoc = Math.abs((hash1 + i * hash2) % (this.byteSize * 8));
            if (!get(hashLoc, bloom)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Set the bit at the specified index to 1.
     *
     * @param pos index of bit
     */
    void set(int pos) {
        int bytePos = pos / 8;
        int bitPos = pos % 8;
        byte curByte = bloom.get(bytePos);
        curByte |= bitvals[bitPos];
        bloom.put(bytePos, curByte);
    }

    /**
     * Check if bit at specified index is 1.
     *
     * @param pos index of bit
     * @return true if bit at specified index is 1, false if 0.
     */
    static boolean get(int pos, ByteBuffer theBloom) {
        int bytePos = pos / 8;
        int bitPos = pos % 8;
        byte curByte = theBloom.get(bytePos);
        curByte &= bitvals[bitPos];
        return (curByte != 0);
    }

    @Override
    public int getKeyCount() {
        return this.keyCount.intValue();
    }

    @Override
    public int getMaxKeys() {
        return this.maxKeys;
    }

    @Override
    public int getByteSize() {
        return this.byteSize;
    }

    @Override
    public void compactBloom() {
        // see if the actual size is exponentially smaller than expected.
        if (this.keyCount.intValue() > 0 && this.bloom.hasArray()) {
            int pieces = 1;
            int newByteSize = this.byteSize;
            int newMaxKeys = this.maxKeys;

            // while exponentially smaller & folding is lossless
            while ((newByteSize & 1) == 0 && newMaxKeys > (this.keyCount.intValue() << 1)) {
                pieces <<= 1;
                newByteSize >>= 1;
                newMaxKeys >>= 1;
            }

            // if we should fold these into pieces
            if (pieces > 1) {
                byte[] array = this.bloom.array();
                int start = this.bloom.arrayOffset();
                int end = start + newByteSize;
                int off = end;
                for (int p = 1; p < pieces; ++p) {
                    for (int pos = start; pos < end; ++pos) {
                        array[pos] |= array[off++];
                    }
                }
                // folding done, only use a subset of this array
                this.bloom.rewind();
                this.bloom.limit(newByteSize);
                this.bloom = this.bloom.slice();
                this.byteSize = newByteSize;
                this.maxKeys = newMaxKeys;
            }
        }
    }

    /**
     * Writes just the bloom filter to the output array
     *
     * @param out OutputStream to place bloom
     * @throws IOException Error writing bloom array
     */
    public void writeBloom(final DataOutput out) throws IOException {
        if (!this.bloom.hasArray()) {
            throw new IOException(
                    "Only writes ByteBuffer with underlying array.");
        }
        out.write(bloom.array(), bloom.arrayOffset(), bloom.limit());
    }

    @Override
    public Writable getMetaWriter() {
        return new MetaWriter();
    }

    @Override
    public Writable getDataWriter() {
        return new DataWriter();
    }

    private class MetaWriter implements Writable {
        protected MetaWriter() {
        }

        @Override
        public void readFields(DataInput arg0) throws IOException {
            throw new IOException("Cant read with this class.");
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(VERSION);
            out.writeInt(byteSize);
            out.writeInt(hashCount);
            out.writeInt(keyCount.intValue());
        }
    }

    private class DataWriter implements Writable {
        protected DataWriter() {
        }

        @Override
        public void readFields(DataInput arg0) throws IOException {
            throw new IOException("Cant read with this class.");
        }

        @Override
        public void write(DataOutput out) throws IOException {
            writeBloom(out);
        }
    }

}
