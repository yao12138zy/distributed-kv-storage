package shared.messages.buffer;

import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.serialization.IKVMessageSerializer;
import shared.services.KVService;

import java.io.IOException;
import java.io.OutputStream;

public class KVMessageCrLfBufferWriter implements IKVMessageBufferWriter {
    private static final Logger logger = Logger.getRootLogger();
    private OutputStream output;
    public KVMessageCrLfBufferWriter(OutputStream output) {
        this.output = output;
    }
    @Override
    public void sendMessage(IKVMessage msg) throws IOException {
        IKVMessageSerializer serializer = KVService.getInstance().getKvMessageSerializer();
        byte[] msgBytes = serializer.serialize(msg);
        byte[] packedMsgBytes = addEOF(msgBytes);
        //System.out.println("[DEBUG]CrLfBufferWriter:" + byteArray2String(packedMsgBytes));
        output.write(packedMsgBytes, 0, packedMsgBytes.length);
        output.flush();
        /*
        logger.info("SEND \t<("
                + msg.getStatus() + ") '"
                + msg.getKey().trim() + "'"+ "[" +
                msg.getValue()
                +"]");

         */
    }
    private byte[] addEOF(byte[] msgBytes){
        final byte C_CR = 13;
        final byte C_LF = 10;
        final byte[] EOF = new byte[] {C_CR, C_LF};
        byte[] tmp = new byte[msgBytes.length + EOF.length];
        System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
        System.arraycopy(EOF, 0, tmp, msgBytes.length, EOF.length);
        return tmp;
    }
    private String byteArray2String(byte[] byteArray) {
        StringBuilder sb = new StringBuilder();
        for (byte b : byteArray) sb.append(b).append(" ");
        return sb.toString();
    }
}
