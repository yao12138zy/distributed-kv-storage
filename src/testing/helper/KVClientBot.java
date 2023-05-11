package testing.helper;

import app_kvServer.KVServer;
import client.KVStore;
import shared.messages.IKVMessage;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.StreamSupport;

public class KVClientBot implements Runnable {
    private KVStore kvClient;
    private int clientId;
    private KVBotTestingPlan plan;
    public KVClientBot(KVStore kvClient, int clientId, KVBotTestingPlan plan) {
        this.kvClient = kvClient;
        this.clientId = clientId;
        this.plan = plan;
    }
    @Override
    public void run() {
        IKVMessage response;
        String value = "Client";
        Instant start = Instant.now();
        try {
            for (int i = 0; i < plan.getPutOperations(); i++) {
                kvClient.put("key" + i, value);
            }
            for (int i = 0; i < plan.getGetOperations(); i++) {
                kvClient.get("key" + i);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Instant finish = Instant.now();

        long timeElapsed = Duration.between(start, finish).toMillis();
        //System.out.println("Client," + clientId + ",puts," + plan.getPutOperations() + ",gets," +plan.getGetOperations() + ",duration," + timeElapsed);
        plan.reportLog("Client," + clientId + ",puts," + plan.getPutOperations() + ",gets," +plan.getGetOperations() + ",duration," + timeElapsed);
        plan.report(timeElapsed);
    }

}
