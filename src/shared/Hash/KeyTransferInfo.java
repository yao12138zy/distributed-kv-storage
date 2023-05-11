package shared.Hash;

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.List;

public class KeyTransferInfo {
    // contains the range of keys need to sent to <server_dest>
    public String sender; // each format <identifier>,<hex_start>, <hex_end>
    public String receiver;
    public byte[] range_start;
    public byte[] range_end;

    public KeyTransferInfo(String sender, String receiver, byte[] range_start, byte[] range_end) {
        this.sender = sender;
        this.receiver = receiver;
        this.range_start = range_start;
        this.range_end = range_end;
    }

    @Override
    public String toString() {
        return "KeyTransferInfo{" +
                "sender='" + sender + '\'' +
                ", receiver='" + receiver + '\'' +
                ", range_start=" + DatatypeConverter.printHexBinary(range_start) +
                ", range_end=" + DatatypeConverter.printHexBinary(range_end) +
                '}';
    }
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        KeyTransferInfo that = (KeyTransferInfo) o;
        return sender.equals(that.sender) &&
                receiver.equals(that.receiver) &&
                range_start.equals(that.range_start) &&
                range_end.equals(that.range_end);
    }
}
