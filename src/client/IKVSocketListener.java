package client;

import shared.messages.IKVMessage;

public interface IKVSocketListener {
    public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};

    public void handleNewMessage(IKVMessage msg) throws InterruptedException;

    public void handleStatus(SocketStatus status);
}
