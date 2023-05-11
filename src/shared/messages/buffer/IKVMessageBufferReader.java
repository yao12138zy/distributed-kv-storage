package shared.messages.buffer;

import shared.messages.IKVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public interface IKVMessageBufferReader {
    public IKVMessage readNextMessage() throws IOException;
}
