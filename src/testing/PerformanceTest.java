package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.ObjectFactory;
import testing.helper.KVBotTestingPlan;
import testing.helper.KVClientBot;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class PerformanceTest extends TestCase {
    private KVStore kvClient;
    private KVClientBot[] kvBots;
    private KVBotTestingPlan testingPlan;
    public void setUp() {
        kvClient = new KVStore(ObjectFactory.createHashRing(),"localhost", 8182);
        kvBots = new KVClientBot[50];
        testingPlan = new KVBotTestingPlan(20, 80);
        try {
            for (int i = 0; i < kvBots.length; i++) {
                KVStore client = new KVStore(ObjectFactory.createHashRing(), "localhost", 8182);
                client.connect();
                kvBots[i] = new KVClientBot(client, i, testingPlan);
            }
        } catch (Exception e) {
        }

    }

    @Test
    public void testThroughput() {

        Exception ex = null;
        try {
            testingPlan.reset(kvBots.length);
            testingPlan.setPlan(80, 20);
            for (int i = 0; i < kvBots.length; i++) {
                new Thread(kvBots[i]).start();
            }
            testingPlan.waitAll();
            System.out.println("Get," + testingPlan.getGetOperations() + ",put," +
                    testingPlan.getPutOperations() + ",average duration," + testingPlan.getAverageDuration());

            testingPlan.reset(kvBots.length);
            testingPlan.setPlan(50, 50);
            for (int i = 0; i < kvBots.length; i++) {
                new Thread(kvBots[i]).start();
            }
            testingPlan.waitAll();
            System.out.println("Get," + testingPlan.getGetOperations() + ",put," +
                    testingPlan.getPutOperations() + ",average duration," + testingPlan.getAverageDuration());
            testingPlan.reset(kvBots.length);
            testingPlan.setPlan(20, 80);
            for (int i = 0; i < kvBots.length; i++) {
                new Thread(kvBots[i]).start();
            }
            testingPlan.waitAll();
            System.out.println("Get," + testingPlan.getGetOperations() + ",put," +
                    testingPlan.getPutOperations() + ",average duration," + testingPlan.getAverageDuration());
            Instant start = Instant.now();
            for (int i = 0; i < 80; i++) {
                kvClient.put(String.format("key-%d", i), Integer.toString(i));
            }
            for (int i=0; i<20; i++) {
                kvClient.get(String.format("key-%d", i));
            }
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("[TEST RESULT] 80 puts & 20 gets: " + Long.toString(timeElapsed));

            start = Instant.now();
            for (int i = 0; i < 50; i++) {
                kvClient.put(String.format("key-%d", i), Integer.toString(i));
            }
            for (int i=0; i<50; i++) {
                kvClient.get(String.format("key-%d", i));
            }
            finish = Instant.now();
            timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("[TEST RESULT] 50 puts & 50 gets: " + Long.toString(timeElapsed));

            start = Instant.now();
            for (int i = 0; i < 20; i++) {
                kvClient.put(String.format("key-%d", i), Integer.toString(i));
            }
            for (int i=0; i<80; i++) {
                kvClient.get(String.format("key-%d", i));
            }
            finish = Instant.now();
            timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("[TEST RESULT] 20 puts & 80 gets: " + Long.toString(timeElapsed));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertTrue(ex == null);
    }
}
