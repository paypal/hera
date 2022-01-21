
public class SQLHashGen
{
    private static long fnv(byte[] buf, int offset, int len, long seed) {
        for(int i = offset; i < offset + len; ++i) {
            seed ^= (long)Byte.toUnsignedInt(buf[i]);
            seed += (seed << 1) + (seed << 4) + (seed << 5) + (seed << 7) + (seed << 8) + (seed << 40);
        }

        return seed;
    }

    public static long fnv64HashCode(String str) {
        Object var1 = null;

        byte[] buf;
        try {
            buf = str.getBytes("UTF-8");
        } catch (Exception var8) {
            buf = str.getBytes();
        }

        long h = fnv(buf, 0, buf.length, -3750763034362895579L);
        int hi = (int)Long.rotateRight(h, 32);
        int lo = (int)h;
        long finalHash = (long)(lo ^ hi);
        return 4294967295L & finalHash;
    }

    public static void main(String args[])
    {
        System.out.print(fnv64HashCode(args[0]));
    }
}
