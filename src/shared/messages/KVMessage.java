package shared.messages;

import jdk.jshell.Snippet;
import shared.messages.serialization.IKVMessageSerializer;
import shared.services.KVService;

public class KVMessage implements IKVMessage{


    private StatusType statusType;
    private String key = "";
    private String value = "";

    public KVMessage(byte[] bytes){
        KVService service = KVService.getInstance();
        IKVMessageSerializer serializer = service.getKvMessageSerializer();
        IKVMessage msg = serializer.deserialize(bytes);
        this.statusType = msg.getStatus();
        this.key = msg.getKey();
        this.value = msg.getValue();
    }

    public KVMessage(StatusType st) {
        this.statusType = st;
    }

    public KVMessage(StatusType statusType, String key, String value) {
        this.statusType = statusType;
        this.key = key;
        this.value = value;
    }

    @Override
    public String getKey() {
        return this.key;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public StatusType getStatus() {
        return this.statusType;
    }

    public void setStatusType(StatusType statusType) {
        this.statusType = statusType;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "KVMessage{" +
                "statusType=" + statusType +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
