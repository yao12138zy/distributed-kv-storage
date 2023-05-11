package testing;

import app_kvServer.KVServer;
import client.KVStore;
import shared.messages.IKVMessage;

import java.io.IOException;

public class ConcurrentAccess implements Runnable{

    private KVStore kvClient;
    private KVServer kvServer;
    private String value;
    public ConcurrentAccess(KVStore kvClient, String value) {
        this.kvClient = kvClient;
        this.value = value;
    }
    @Override
    public void run() {
        String key = "testKey";
        IKVMessage response;
        try {
            response = kvClient.put(key, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (response.getStatus() == IKVMessage.StatusType.PUT_ERROR) {
            throw new RuntimeException();
        }
    }
}
