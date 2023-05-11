package shared.messages.buffer;

import shared.messages.IKVMessage;

import java.io.IOException;

public interface IKVMessageBufferWriter {
    public void sendMessage(IKVMessage msg) throws IOException;
}
