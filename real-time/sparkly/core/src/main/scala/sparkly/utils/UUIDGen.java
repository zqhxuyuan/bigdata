package sparkly.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * The goods are here: www.ietf.org/rfc/rfc4122.txt.
 */
public class UUIDGen
{
    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    private static final long START_EPOCH = -12219292800000L;
    private static final long clockSeqAndNode = makeClockSeqAndNode();

    /*
     * The min and max possible lsb for a UUID.
     * Note that his is not 0 and all 1's because Cassandra TimeUUIDType
     * compares the lsb parts as a signed byte array comparison. So the min
     * value is 8 times -128 and the max is 8 times +127.
     *
     * Note that we ignore the uuid variant (namely, MIN_CLOCK_SEQ_AND_NODE
     * have variant 2 as it should, but MAX_CLOCK_SEQ_AND_NODE have variant 0).
     * I don't think that has any practical consequence and is more robust in
     * case someone provides a UUID with a broken variant.
     */
    private static final long MIN_CLOCK_SEQ_AND_NODE = 0x8080808080808080L;
    private static final long MAX_CLOCK_SEQ_AND_NODE = 0x7f7f7f7f7f7f7f7fL;

    // placement of this singleton is important.  It needs to be instantiated *AFTER* the other statics.
    private static final UUIDGen instance = new UUIDGen();

    private long lastNanos;

    private UUIDGen()
    {
        // make sure someone didn't whack the clockSeqAndNode by changing the order of instantiation.
        if (clockSeqAndNode == 0) throw new RuntimeException("singleton instantiation is misplaced.");
    }

    /**
     * Creates a type 1 UUID (time-based UUID).
     *
     * @return a UUID instance
     */
    public static UUID getTimeUUID()
    {
        return new UUID(instance.createTimeSafe(), clockSeqAndNode);
    }

    /**
     * Creates a type 1 UUID (time-based UUID) with the timestamp of @param when, in milliseconds.
     *
     * @return a UUID instance
     */
    public static UUID getTimeUUID(long when)
    {
        return new UUID(createTime(fromUnixTimestamp(when)), clockSeqAndNode);
    }

    public static UUID getTimeUUID(long when, long clockSeqAndNode)
    {
        return new UUID(createTime(fromUnixTimestamp(when)), clockSeqAndNode);
    }

    /** creates a type 1 uuid from raw bytes. */
    public static UUID getUUID(ByteBuffer raw)
    {
        return new UUID(raw.getLong(raw.position()), raw.getLong(raw.position() + 8));
    }

    /** decomposes a uuid into raw bytes. */
    public static byte[] decompose(UUID uuid)
    {
        long most = uuid.getMostSignificantBits();
        long least = uuid.getLeastSignificantBits();
        byte[] b = new byte[16];
        for (int i = 0; i < 8; i++)
        {
            b[i] = (byte)(most >>> ((7-i) * 8));
            b[8+i] = (byte)(least >>> ((7-i) * 8));
        }
        return b;
    }

    /**
     * Returns a 16 byte representation of a type 1 UUID (a time-based UUID),
     * based on the current system time.
     *
     * @return a type 1 UUID represented as a byte[]
     */
    public static byte[] getTimeUUIDBytes()
    {
        return createTimeUUIDBytes(instance.createTimeSafe());
    }

    /**
     * Returns the smaller possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    public static UUID minTimeUUID(long timestamp)
    {
        return new UUID(createTime(fromUnixTimestamp(timestamp)), MIN_CLOCK_SEQ_AND_NODE);
    }

    /**
     * Returns the biggest possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    public static UUID maxTimeUUID(long timestamp)
    {
        // unix timestamp are milliseconds precision, uuid timestamp are 100's
        // nanoseconds precision. If we ask for the biggest uuid have unix
        // timestamp 1ms, then we should not extend 100's nanoseconds
        // precision by taking 10000, but rather 19999.
        long uuidTstamp = fromUnixTimestamp(timestamp + 1) - 1;
        return new UUID(createTime(uuidTstamp), MAX_CLOCK_SEQ_AND_NODE);
    }

    /**
     * @param uuid
     * @return milliseconds since Unix epoch
     */
    public static long unixTimestamp(UUID uuid)
    {
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

    /**
     * @param uuid
     * @return microseconds since Unix epoch
     */
    public static long microsTimestamp(UUID uuid)
    {
        return (uuid.timestamp() / 10) + START_EPOCH * 1000;
    }

    /**
     * @param timestamp milliseconds since Unix epoch
     * @return
     */
    private static long fromUnixTimestamp(long timestamp) {
        return (timestamp - START_EPOCH) * 10000;
    }

    /**
     * Converts a milliseconds-since-epoch timestamp into the 16 byte representation
     * of a type 1 UUID (a time-based UUID).
     *
     * <p><i><b>Deprecated:</b> This method goes again the principle of a time
     * UUID and should not be used. For queries based on timestamp, minTimeUUID() and
     * maxTimeUUID() can be used but this method has questionable usefulness. This is
     * only kept because CQL2 uses it (see TimeUUID.fromStringCQL2) and we
     * don't want to break compatibility.</i></p>
     *
     * <p><i><b>Warning:</b> This method is not guaranteed to return unique UUIDs; Multiple
     * invocations using identical timestamps will result in identical UUIDs.</i></p>
     *
     * @param timeMillis
     * @return a type 1 UUID represented as a byte[]
     */
    public static byte[] getTimeUUIDBytes(long timeMillis)
    {
        return createTimeUUIDBytes(instance.createTimeUnsafe(timeMillis));
    }

    /**
     * Converts a 100-nanoseconds precision timestamp into the 16 byte representation
     * of a type 1 UUID (a time-based UUID).
     *
     * To specify a 100-nanoseconds precision timestamp, one should provide a milliseconds timestamp and
     * a number 0 <= n < 10000 such that n*100 is the number of nanoseconds within that millisecond.
     *
     * <p><i><b>Warning:</b> This method is not guaranteed to return unique UUIDs; Multiple
     * invocations using identical timestamps will result in identical UUIDs.</i></p>
     *
     * @return a type 1 UUID represented as a byte[]
     */
    public static byte[] getTimeUUIDBytes(long timeMillis, int nanos)
    {
        if (nanos >= 10000)
            throw new IllegalArgumentException();
        return createTimeUUIDBytes(instance.createTimeUnsafe(timeMillis, nanos));
    }

    private static byte[] createTimeUUIDBytes(long msb)
    {
        long lsb = clockSeqAndNode;
        byte[] uuidBytes = new byte[16];

        for (int i = 0; i < 8; i++)
            uuidBytes[i] = (byte) (msb >>> 8 * (7 - i));

        for (int i = 8; i < 16; i++)
            uuidBytes[i] = (byte) (lsb >>> 8 * (7 - i));

        return uuidBytes;
    }

    /**
     * Returns a milliseconds-since-epoch value for a type-1 UUID.
     *
     * @param uuid a type-1 (time-based) UUID
     * @return the number of milliseconds since the unix epoch
     * @throws IllegalArgumentException if the UUID is not version 1
     */
    public static long getAdjustedTimestamp(UUID uuid)
    {
        if (uuid.version() != 1)
            throw new IllegalArgumentException("incompatible with uuid version: "+uuid.version());
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

    private static long makeClockSeqAndNode()
    {
        long clock = new Random(System.currentTimeMillis()).nextLong();

        long lsb = 0;
        lsb |= 0x8000000000000000L;                 // variant (2 bits)
        lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
        lsb |= makeNode();                          // 6 bytes
        return lsb;
    }

    // needs to return two different values for the same when.
    // we can generate at most 10k UUIDs per ms.
    private synchronized long createTimeSafe()
    {
        long nanosSince = (System.currentTimeMillis() - START_EPOCH) * 10000;
        if (nanosSince > lastNanos)
            lastNanos = nanosSince;
        else
            nanosSince = ++lastNanos;

        return createTime(nanosSince);
    }

    /** @param when time in milliseconds */
    private long createTimeUnsafe(long when)
    {
        return createTimeUnsafe(when, 0);
    }

    private long createTimeUnsafe(long when, int nanos)
    {
        long nanosSince = ((when - START_EPOCH) * 10000) + nanos;
        return createTime(nanosSince);
    }

    private static long createTime(long nanosSince)
    {
        long msb = 0L;
        msb |= (0x00000000ffffffffL & nanosSince) << 32;
        msb |= (0x0000ffff00000000L & nanosSince) >>> 16;
        msb |= (0xffff000000000000L & nanosSince) >>> 48;
        msb |= 0x0000000000001000L; // sets the version to 1.
        return msb;
    }

    private static long makeNode()
    {
        Collection<InetAddress> localAddresses = getAllLocalAddresses();
        if (localAddresses.isEmpty())
            throw new RuntimeException("Cannot generate the node component of the UUID because cannot retrieve any IP addresses.");

        // ideally, we'd use the MAC address, but java doesn't expose that.
        byte[] hash = hash(localAddresses);
        long node = 0;
        for (int i = 0; i < Math.min(6, hash.length); i++)
            node |= (0x00000000000000ff & (long)hash[i]) << (5-i)*8;
        assert (0xff00000000000000L & node) == 0;

        // Since we don't use the mac address, the spec says that multicast
        // bit (least significant bit of the first octet of the node ID) must be 1.
        return node | 0x0000010000000000L;
    }

    private static byte[] hash(Collection<InetAddress> data)
    {
        try
        {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            for(InetAddress addr : data)
                messageDigest.update(addr.getAddress());

            return messageDigest.digest();
        }
        catch (NoSuchAlgorithmException nsae)
        {
            throw new RuntimeException("MD5 digest algorithm is not available", nsae);
        }
    }

    private static Collection<InetAddress> getAllLocalAddresses()
    {
        Set<InetAddress> localAddresses = new HashSet<>();
        try
        {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            if (nets != null)
            {
                while (nets.hasMoreElements())
                    localAddresses.addAll(Collections.list(nets.nextElement().getInetAddresses()));
            }
        }
        catch (SocketException e)
        {
            throw new AssertionError(e);
        }
        return localAddresses;
    }
}