package shared.messages.serialization;

import shared.KVMessageFactory;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;

public class KVMMessageUTFSerializer implements IKVMessageSerializer{
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    @Override
    public byte[] serialize(IKVMessage msg) {
        try{
            ByteArrayOutputStream byteArrayoutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream( byteArrayoutputStream );
            //System.out.println(msg.getStatus().toString());
            //System.out.println(msg.getKey().toString());
            //System.out.println(msg.getValue().toString());
            objectOutputStream.writeUTF(msg.getKey());
            objectOutputStream.writeUTF(msg.getValue());
            objectOutputStream.writeUTF(msg.getStatus().toString());
            objectOutputStream.close();
            byte[] packed = addCtrChars(byteArrayoutputStream.toByteArray());
            /*
            System.out.println("packed size:" + packed.length);
            String byteString = "";
            for (int i = 0; i < packed.length; i++) {
                byteString += packed[i] + " ";
            }
            System.out.println("packed value:" + byteString);
            IKVMessage test = deserialize(packed);
            System.out.println("packed deserialize test:" + test.getValue());
             */
            return packed;
        }
        catch (Exception e){
            System.out.println("SerializedERR:[" + e.getMessage() + "]");
        }
        return new byte[0];
    }

    @Override
    public IKVMessage deserialize(byte[] byteArray) {
        try{
            ObjectInputStream ois = new ObjectInputStream(
                    new ByteArrayInputStream(  byteArray ) );
            String key  = ois.readUTF();
            String value  = ois.readUTF();
            String statusTypeString  = ois.readUTF();
            IKVMessage.StatusType statusType = IKVMessage.StatusType.valueOf(statusTypeString);
            ois.close();
            return KVMessageFactory.createMessage(statusType, key, value);
        }
        catch (Exception e){
            System.out.println("DeSerializedERR:[" + e.getMessage() + "]ByteArraySize:[" + byteArray.length + "]");
            System.out.println("String probe:[" + byteArray2String(byteArray) + "]");
            System.out.println("String probe2:[" + (new String(byteArray, StandardCharsets.UTF_8)) + "]");
        }
        return null;
    }
    private String byteArray2String(byte[] byteArray) {
        StringBuilder sb = new StringBuilder(byteArray.length * 3);
        for (int i = 0; i < byteArray.length; i++) {
            sb.append(byteArray[i] + " ");
        }
        return sb.toString();
    }
    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{ RETURN, LINE_FEED };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
        return tmp;
    }
}
