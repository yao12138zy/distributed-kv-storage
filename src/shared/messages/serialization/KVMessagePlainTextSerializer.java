package shared.messages.serialization;

import org.apache.log4j.Logger;
import shared.KVMessageFactory;
import shared.messages.IKVMessage;

import java.nio.charset.StandardCharsets;

public class KVMessagePlainTextSerializer implements IKVMessageSerializer{
    private static final Logger logger = Logger.getRootLogger();
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    @Override
    public byte[] serialize(IKVMessage msg) {
        try{
            IKVMessage.StatusType status = msg.getStatus();
            byte[] byteArray;
            String msgText;
            if (status == IKVMessage.StatusType.GET) {
                msgText = "get " + msg.getKey();
            }
            else if (status == IKVMessage.StatusType.GET_SUCCESS) {
                msgText = IKVMessage.StatusType.GET_SUCCESS + " " + msg.getKey() + " " + msg.getValue();
            }
            else if (status == IKVMessage.StatusType.GET_ERROR) {
                msgText = IKVMessage.StatusType.GET_ERROR + " " + msg.getKey() + " " + msg.getValue();
            }
            else if (status == IKVMessage.StatusType.PUT) {
                msgText = "put " + msg.getKey() + " " + msg.getValue();
            }
            else if (status == IKVMessage.StatusType.PUT_SUCCESS) {
                msgText = IKVMessage.StatusType.PUT_SUCCESS + " " + msg.getKey() + " " + msg.getValue();
            }
            else if (status == IKVMessage.StatusType.PUT_ERROR) {
                msgText = IKVMessage.StatusType.PUT_ERROR + " " + msg.getKey();
            }
            else if (status == IKVMessage.StatusType.PUT_UPDATE) {
                msgText = IKVMessage.StatusType.PUT_UPDATE + " " + msg.getKey() + " " + msg.getValue();
            }
            else if (status == IKVMessage.StatusType.DELETE_SUCCESS) {
                msgText = IKVMessage.StatusType.DELETE_SUCCESS + " " + msg.getKey();
            }
            else if (status == IKVMessage.StatusType.DELETE_ERROR) {
                msgText = IKVMessage.StatusType.DELETE_ERROR + " " + msg.getKey();
            }
            else if (status == IKVMessage.StatusType.FAILED) {
                msgText = IKVMessage.StatusType.FAILED + " key " + msg.getValue();
            }
            else if (status == IKVMessage.StatusType.KEYRANGE) {
                msgText = "keyrange";
            }
            else if (status == IKVMessage.StatusType.KEYRANGE_READ) {
                msgText = "keyrange_read";
            }
            else {
                msgText = status.toString() + " " + msg.getKey() + " " + msg.getValue();
            }
            msgText = msgText.trim();

            //logger.debug("[KVMessagePlainTextSerializer]Serialize:" + msgText);
            byteArray = msgText.getBytes(StandardCharsets.UTF_8);
            //System.out.println("[Serializer]:" + msgText);
            //System.out.println("[SerializerByte]:" + byteArray2String(byteArray));
            return byteArray;
        }
        catch (Exception e){
            logger.error("[KVMessagePlainTextSerializer][serialize]ERROR:" + e.getMessage());
            if (msg != null){
                logger.error(String.format("[KVMessagePlainTextSerializer]MsgType:%s Key:%s Value:%s",
                        msg.getStatus().toString(),
                        msg.getKey(),
                        msg.getValue()));
            }
            else {
                logger.error(String.format("[KVMessagePlainTextSerializer]Msg is null"));
            }
        }
        return new byte[0];
    }

    @Override
    public IKVMessage deserialize(byte[] byteArray) {
        String msgText = "";
        try{
            msgText = (new String(byteArray, StandardCharsets.UTF_8)).trim();
            //logger.debug("[KVMessagePlainTextSerializer]Deserialize:" + msgText);
            //System.out.println("[MSG]:" + msgText);
            //System.out.println("[MSG_ARRAY]:" + byteArray2String(byteArray));
            String[] tokens = msgText.split("\\s+");
            StringBuilder sbValue = new StringBuilder();
            if (tokens.length > 2) {
                for(int i = 2; i < tokens.length; i++) {
                    sbValue.append(tokens[i]);
                    if (i != tokens.length -1 ) {
                        sbValue.append(" ");
                    }
                }
            }
            String statusText = tokens[0];
            String key = "";
            if (tokens.length > 1)
                key = tokens[1];
            String value = sbValue.toString();
            //logger.debug("[KVMessagePlainTextSerializer][deserialize]MsgKey:" + key);
            //logger.debug("[KVMessagePlainTextSerializer][deserialize]MsgValue:" + value);
            IKVMessage msg;
            if (statusText.equals("put")){
                msg = KVMessageFactory.createPutMessage(key, value);
            }
            else if (statusText.equals("get")) {
                msg = KVMessageFactory.createGetMessage(key);
            }
            else if (statusText.equals(IKVMessage.StatusType.PUT_SUCCESS.toString())){
                msg = KVMessageFactory.createPutSuccessMessage(key, value);
            }
            else if (statusText.equals(IKVMessage.StatusType.PUT_UPDATE.toString())){
                msg = KVMessageFactory.createPutUpdateMessage(key, value);
            }
            else if (statusText.equals(IKVMessage.StatusType.PUT_ERROR.toString())){
                msg = KVMessageFactory.createPutErrorMessage(key);
            }
            else if (statusText.equals(IKVMessage.StatusType.GET_SUCCESS.toString())){
                msg = KVMessageFactory.createGetSuccessMessage(key, value);
            }
            else if (statusText.equals(IKVMessage.StatusType.GET_ERROR.toString())){
                msg = KVMessageFactory.createGetErrorMessage(key);
            }
            else if (statusText.equals(IKVMessage.StatusType.DELETE_SUCCESS.toString())){
                msg = KVMessageFactory.createDeleteSuccessMessage(key);
            }
            else if (statusText.equals(IKVMessage.StatusType.DELETE_ERROR.toString())){
                msg = KVMessageFactory.createDeleteErrorMessage(key);
            }
            else if (statusText.equals(IKVMessage.StatusType.FAILED.toString())){
                msg = KVMessageFactory.createFailedMessage(value);
            }
            else if (statusText.equals("keyrange")) {
                msg = KVMessageFactory.createKeyRangeMessage();
            }
            else if (statusText.equals("keyrange_read")) {
                msg = KVMessageFactory.createKeyRangeReadMessage();
            }
            else {
                try {
                    IKVMessage.StatusType statusType = IKVMessage.StatusType.valueOf(statusText);
                    msg = KVMessageFactory.createMessage(statusType, key, value);
                }
                catch (IllegalArgumentException e) {
                    msg = KVMessageFactory.createFailedMessage(value);
                }
            }
            return msg;

        }
        catch (Exception e){
            logger.error("[KVMessagePlainTextSerializer][deserialize]ERROR:" + e.getMessage() + "");
            logger.error("[KVMessagePlainTextSerializer][deserialize]ByteArrayLength:" + byteArray.length);
            logger.error("[KVMessagePlainTextSerializer][deserialize]MsgText:" + msgText);
        }
        return null;
    }
    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{ RETURN, LINE_FEED };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);

        return tmp;
    }
    private byte[] toByteArray(String s){
        byte[] bytes = s.getBytes();
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }
    private String byteArray2String(byte[] byteArray) {
        StringBuilder sb = new StringBuilder();
        for (byte b : byteArray) sb.append(b).append(" ");
        return sb.toString();
    }
}
