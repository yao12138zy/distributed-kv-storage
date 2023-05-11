package shared.messages.serialization;

import shared.messages.IKVMessage;

public interface IKVMessageSerializer {
    public byte[] serialize(IKVMessage msg);
    public IKVMessage deserialize(byte[] byteArray);
}
