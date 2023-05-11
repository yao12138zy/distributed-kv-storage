package shared.messages.buffer;

import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.serialization.KVMMessageUTFSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;


/*
* The existence of this class is questionable
* Maybe we do not need the interface, or be cautious about the input arguments
* */
public class KVMessageCrLfBufferReader implements IKVMessageBufferReader {
    private static final Logger logger = Logger.getRootLogger();
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    private final Socket clientSocket;
    private final InputStream input;
    public KVMessageCrLfBufferReader(Socket clientSocket, InputStream input) {
        this.clientSocket = clientSocket;
        this.input = input;
    }
    @Override
    public IKVMessage readNextMessage() throws IOException {
        final byte C_CR = 13;
        final byte C_LF = 10;
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        byte read = (byte)input.read();
        byte prevRead = 0;
        boolean reading = true;

        while ((prevRead != C_CR || read != C_LF) && read != -1 && reading) {
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            prevRead = read;
            read = (byte) input.read();
        }

        if (read == -1){
            throw new IOException("Reader Pipe Closed");
        }

        if(msgBytes == null){
            tmp = new byte[index - 1];
            System.arraycopy(bufferBytes, 0, tmp, 0, index - 1);
        } else {
            tmp = new byte[msgBytes.length + index - 1];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index - 1);
        }

        msgBytes = tmp;

        /* build final String */
        KVMessage msg = new KVMessage(msgBytes);
        /*
        logger.info("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">:(" + msg.getStatus() + ") '"
                + msg.getKey().trim() + "'"+ "[" +
                msg.getValue()
                +"]");
         */
        return msg;
    }
    private String byteArray2String(byte[] byteArray) {
        StringBuilder sb = new StringBuilder();
        for (byte b : byteArray) sb.append(b).append(" ");
        return sb.toString();
    }
}
