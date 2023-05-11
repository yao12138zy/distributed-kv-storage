package shared.Hash;

import com.google.common.primitives.UnsignedBytes;

import java.util.Comparator;

public class SortByteArray implements Comparator<byte[]> {
    @Override
    public int compare(byte[] o1, byte[] o2) {
        return UnsignedBytes.lexicographicalComparator().compare(o1, o2);
    }
}
