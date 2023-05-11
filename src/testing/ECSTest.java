package testing;

import app_kvECS.IECSClient;
import app_kvServer.ECSConnection;
import app_kvServer.IKVServer;
import app_kvServer.storage.IKVStorage;
import app_kvServer.storage.KVFileStorage;
import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.Hash.HashRing;
import shared.Hash.IHashRing;
import shared.KVMessageFactory;
import shared.ObjectFactory;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ECSTest extends TestCase {
    /*
    Lock sequential = new ReentrantLock();

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        sequential.lock();
    }

    @Override
    protected void tearDown() throws Exception {
        sequential.unlock();
        super.tearDown();
    }
    @Test
    public void testServerJoinLeave() {

        Exception ex = null;
        IECSClient ecs = ObjectFactory.createECSClient("localhost", 9077);
        new Thread(ecs).start();
        IKVMessage put1 = KVMessageFactory.createFailedMessage("undefined"),
                put2 = KVMessageFactory.createFailedMessage("undefined"),
                put3 = KVMessageFactory.createFailedMessage("undefined"),
                get1 = KVMessageFactory.createFailedMessage("undefined"),
                get2 = KVMessageFactory.createFailedMessage("undefined"),
                get3 = KVMessageFactory.createFailedMessage("undefined"),
                get4 = KVMessageFactory.createFailedMessage("undefined"),
                get5 = KVMessageFactory.createFailedMessage("undefined"),
                get6 = KVMessageFactory.createFailedMessage("undefined");
        try {
            IKVStorage storage1 = new KVFileStorage("ecs_test_kv1.dat");
            storage1.clearStorage();
            IKVServer server1 = (ObjectFactory.createKVServerObject(8082, "localhost", 10, "FIFO", storage1));
            final ECSConnection ecsConn1 = new ECSConnection("localhost", 9077, server1, "localhost:8082");
            ecsConn1.connect();

            IKVStorage storage2 = new KVFileStorage("ecs_test_kv2.dat");
            storage2.clearStorage();
            IKVServer server2 = (ObjectFactory.createKVServerObject(8083, "localhost", 10, "FIFO", storage2));
            final ECSConnection ecsConn2 = new ECSConnection("localhost", 9077, server2, "localhost:8083");
            ecsConn2.connect();

            IKVStorage storage3 = new KVFileStorage("ecs_test_kv3.dat");
            storage3.clearStorage();
            IKVServer server3 = (ObjectFactory.createKVServerObject(8084, "localhost", 10, "FIFO", storage3));
            final ECSConnection ecsConn3 = new ECSConnection("localhost", 9077, server3, "localhost:8084");
            ecsConn3.connect();
            Thread.sleep(1000);
            KVStore kvClient = new KVStore(ObjectFactory.createHashRing(), "localhost", 8082);
            put1 = kvClient.put("asd", "asd");
            put2 = kvClient.put("asd1", "asd1");
            put3 = kvClient.put("7", "7");
            Thread.sleep(2000);
            get1 = kvClient.get("asd");
            get2 = kvClient.get("asd1");
            get3 = kvClient.get("7");
            ecsConn2.close(false);
            Thread.sleep(2000);
            ecsConn3.close(false);
            Thread.sleep(2000);
            //ecsConn3.close();
            //Thread.sleep(1000);
            //server3.close();
            //Thread.sleep(1000);
            get4 = kvClient.get("asd");
            get5 = kvClient.get("asd1");
            get6 = kvClient.get("7");
            ecsConn1.close(false);

        }
        catch (InterruptedException | IOException e) {
            ex = e;
            System.out.println(e.getMessage());
        }
        boolean success = ex == null
                && put1.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && put2.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && put3.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && get1.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get2.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get3.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get4.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get5.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get6.getStatus() == IKVMessage.StatusType.GET_SUCCESS;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("put1:" + put1);
            System.out.println("put2:" + put2);
            System.out.println("put3:" + put3);
            System.out.println("get1:" + get1);
            System.out.println("get2:" + get2);
            System.out.println("get3:" + get3);
            System.out.println("get4:" + get4);
            System.out.println("get5:" + get5);
            System.out.println("get6:" + get6);

        }
        assertTrue(success);
    }

    @Test
    public void testServerKill1() {

        Exception ex = null;
        IECSClient ecs = ObjectFactory.createECSClient("localhost", 9078);
        new Thread(ecs).start();
        IKVMessage put1 = KVMessageFactory.createFailedMessage("undefined"),
                put2 = KVMessageFactory.createFailedMessage("undefined"),
                put3 = KVMessageFactory.createFailedMessage("undefined"),
                get1 = KVMessageFactory.createFailedMessage("undefined"),
                get2 = KVMessageFactory.createFailedMessage("undefined"),
                get3 = KVMessageFactory.createFailedMessage("undefined"),
                get4 = KVMessageFactory.createFailedMessage("undefined"),
                get5 = KVMessageFactory.createFailedMessage("undefined"),
                get6 = KVMessageFactory.createFailedMessage("undefined");
        try {
            IKVStorage storage1 = new KVFileStorage("ecs_test_kv1.dat");
            storage1.clearStorage();
            IKVServer server1 = (ObjectFactory.createKVServerObject(9091, "localhost", 10, "FIFO", storage1));
            final ECSConnection ecsConn1 = new ECSConnection("localhost", 9078, server1, "localhost:9091");
            ecsConn1.connect();

            IKVStorage storage2 = new KVFileStorage("ecs_test_kv2.dat");
            storage2.clearStorage();
            IKVServer server2 = (ObjectFactory.createKVServerObject(9092, "localhost", 10, "FIFO", storage2));
            final ECSConnection ecsConn2 = new ECSConnection("localhost", 9078, server2, "localhost:9092");
            ecsConn2.connect();

            IKVStorage storage3 = new KVFileStorage("ecs_test_kv3.dat");
            storage3.clearStorage();
            IKVServer server3 = (ObjectFactory.createKVServerObject(9093, "localhost", 10, "FIFO", storage3));
            final ECSConnection ecsConn3 = new ECSConnection("localhost", 9078, server3, "localhost:9093");
            ecsConn3.connect();

            IKVStorage storage4 = new KVFileStorage("ecs_test_kv4.dat");
            storage4.clearStorage();
            IKVServer server4 = (ObjectFactory.createKVServerObject(9094, "localhost", 10, "FIFO", storage4));
            final ECSConnection ecsConn4 = new ECSConnection("localhost", 9078, server4, "localhost:9094");
            ecsConn4.connect();
            Thread.sleep(1000);

            KVStore kvClient = new KVStore(ObjectFactory.createHashRing(), "localhost", 9091);
            put1 = kvClient.put("asd", "asd");
            put2 = kvClient.put("asd1", "asd1");
            put3 = kvClient.put("7", "7");
            Thread.sleep(2000);
            get1 = kvClient.get("asd");
            get2 = kvClient.get("asd1");
            get3 = kvClient.get("7");
            ecsConn2.close(true);
            Thread.sleep(1000);
            get4 = kvClient.get("asd");
            get5 = kvClient.get("asd1");
            get6 = kvClient.get("7");
            ecsConn1.close(true);
            ecsConn3.close(true);
            ecsConn4.close(true);

        }
        catch (InterruptedException | IOException e) {
            ex = e;
            System.out.println(e.getMessage());
        }
        boolean success = ex == null
                && put1.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && put2.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && put3.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && get1.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get2.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get3.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get4.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get5.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get6.getStatus() == IKVMessage.StatusType.GET_SUCCESS;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("put1:" + put1);
            System.out.println("put2:" + put2);
            System.out.println("put3:" + put3);
            System.out.println("get1:" + get1);
            System.out.println("get2:" + get2);
            System.out.println("get3:" + get3);
            System.out.println("get4:" + get4);
            System.out.println("get5:" + get5);
            System.out.println("get6:" + get6);

        }
        assertTrue(success);
    }

    @Test
    public void testServerKill2() {

        Exception ex = null;
        IECSClient ecs = ObjectFactory.createECSClient("localhost", 9079);
        new Thread(ecs).start();
        IKVMessage put1 = KVMessageFactory.createFailedMessage("undefined"),
                put2 = KVMessageFactory.createFailedMessage("undefined"),
                put3 = KVMessageFactory.createFailedMessage("undefined"),
                get1 = KVMessageFactory.createFailedMessage("undefined"),
                get2 = KVMessageFactory.createFailedMessage("undefined"),
                get3 = KVMessageFactory.createFailedMessage("undefined"),
                get4 = KVMessageFactory.createFailedMessage("undefined"),
                get5 = KVMessageFactory.createFailedMessage("undefined"),
                get6 = KVMessageFactory.createFailedMessage("undefined");
        try {
            IKVStorage storage1 = new KVFileStorage("ecs_test_kv1.dat");
            storage1.clearStorage();
            IKVServer server1 = (ObjectFactory.createKVServerObject(9091, "localhost", 10, "FIFO", storage1));
            final ECSConnection ecsConn1 = new ECSConnection("localhost", 9079, server1, "localhost:9091");
            ecsConn1.connect();

            IKVStorage storage2 = new KVFileStorage("ecs_test_kv2.dat");
            storage2.clearStorage();
            IKVServer server2 = (ObjectFactory.createKVServerObject(9092, "localhost", 10, "FIFO", storage2));
            final ECSConnection ecsConn2 = new ECSConnection("localhost", 9079, server2, "localhost:9092");
            ecsConn2.connect();

            IKVStorage storage3 = new KVFileStorage("ecs_test_kv3.dat");
            storage3.clearStorage();
            IKVServer server3 = (ObjectFactory.createKVServerObject(9093, "localhost", 10, "FIFO", storage3));
            final ECSConnection ecsConn3 = new ECSConnection("localhost", 9079, server3, "localhost:9093");
            ecsConn3.connect();

            IKVStorage storage4 = new KVFileStorage("ecs_test_kv4.dat");
            storage4.clearStorage();
            IKVServer server4 = (ObjectFactory.createKVServerObject(9094, "localhost", 10, "FIFO", storage4));
            final ECSConnection ecsConn4 = new ECSConnection("localhost", 9079, server4, "localhost:9094");
            ecsConn4.connect();
            Thread.sleep(1000);

            KVStore kvClient = new KVStore(ObjectFactory.createHashRing(), "localhost", 9091);
            put1 = kvClient.put("asd", "asd");
            put2 = kvClient.put("asd1", "asd1");
            put3 = kvClient.put("7", "7");
            Thread.sleep(2000);
            get1 = kvClient.get("asd");
            get2 = kvClient.get("asd1");
            get3 = kvClient.get("7");
            ecsConn2.close(true);
            Thread.sleep(2000);
            ecsConn1.close(true);
            Thread.sleep(2000);
            get4 = kvClient.get("asd");
            get5 = kvClient.get("asd1");
            get6 = kvClient.get("7");

            ecsConn3.close(true);
            ecsConn4.close(true);

        }
        catch (InterruptedException | IOException e) {
            ex = e;
            System.out.println(e.getMessage());
        }
        boolean success = ex == null
                && put1.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && put2.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && put3.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && get1.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get2.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get3.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get4.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get5.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get6.getStatus() == IKVMessage.StatusType.GET_SUCCESS;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("put1:" + put1);
            System.out.println("put2:" + put2);
            System.out.println("put3:" + put3);
            System.out.println("get1:" + get1);
            System.out.println("get2:" + get2);
            System.out.println("get3:" + get3);
            System.out.println("get4:" + get4);
            System.out.println("get5:" + get5);
            System.out.println("get6:" + get6);

        }
        assertTrue(success);
    }
    @Test
    public void testServerKill3() {

        Exception ex = null;
        IECSClient ecs = ObjectFactory.createECSClient("localhost", 9080);
        new Thread(ecs).start();
        IKVMessage put1 = KVMessageFactory.createFailedMessage("undefined"),
                put2 = KVMessageFactory.createFailedMessage("undefined"),
                put3 = KVMessageFactory.createFailedMessage("undefined"),
                get1 = KVMessageFactory.createFailedMessage("undefined"),
                get2 = KVMessageFactory.createFailedMessage("undefined"),
                get3 = KVMessageFactory.createFailedMessage("undefined"),
                get4 = KVMessageFactory.createFailedMessage("undefined"),
                get5 = KVMessageFactory.createFailedMessage("undefined"),
                get6 = KVMessageFactory.createFailedMessage("undefined");
        try {
            IKVStorage storage1 = new KVFileStorage("ecs_test_kv1.dat");
            storage1.clearStorage();
            IKVServer server1 = (ObjectFactory.createKVServerObject(9091, "localhost", 10, "FIFO", storage1));
            final ECSConnection ecsConn1 = new ECSConnection("localhost", 9080, server1, "localhost:9091");
            ecsConn1.connect();

            IKVStorage storage2 = new KVFileStorage("ecs_test_kv2.dat");
            storage2.clearStorage();
            IKVServer server2 = (ObjectFactory.createKVServerObject(9092, "localhost", 10, "FIFO", storage2));
            final ECSConnection ecsConn2 = new ECSConnection("localhost", 9080, server2, "localhost:9092");
            ecsConn2.connect();

            IKVStorage storage3 = new KVFileStorage("ecs_test_kv3.dat");
            storage3.clearStorage();
            IKVServer server3 = (ObjectFactory.createKVServerObject(9093, "localhost", 10, "FIFO", storage3));
            final ECSConnection ecsConn3 = new ECSConnection("localhost", 9080, server3, "localhost:9093");
            ecsConn3.connect();

            IKVStorage storage4 = new KVFileStorage("ecs_test_kv4.dat");
            storage4.clearStorage();
            IKVServer server4 = (ObjectFactory.createKVServerObject(9094, "localhost", 10, "FIFO", storage4));
            final ECSConnection ecsConn4 = new ECSConnection("localhost", 9080, server4, "localhost:9094");
            ecsConn4.connect();
            Thread.sleep(1000);

            KVStore kvClient = new KVStore(ObjectFactory.createHashRing(), "localhost", 9091);
            put1 = kvClient.put("asd", "asd");
            put2 = kvClient.put("asd1", "asd1");
            put3 = kvClient.put("7", "7");
            Thread.sleep(2000);
            get1 = kvClient.get("asd");
            get2 = kvClient.get("asd1");
            get3 = kvClient.get("7");
            ecsConn2.close(true);
            Thread.sleep(2000);
            ecsConn1.close(true);
            Thread.sleep(2000);
            ecsConn3.close(true);
            Thread.sleep(2000);
            get4 = kvClient.get("asd");
            get5 = kvClient.get("asd1");
            get6 = kvClient.get("7");
            ecsConn4.close(true);

        }
        catch (InterruptedException | IOException e) {
            ex = e;
            System.out.println(e.getMessage());
        }
        boolean success = ex == null
                && put1.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && put2.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && put3.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && get1.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get2.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get3.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get4.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get5.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get6.getStatus() == IKVMessage.StatusType.GET_SUCCESS;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("put1:" + put1);
            System.out.println("put2:" + put2);
            System.out.println("put3:" + put3);
            System.out.println("get1:" + get1);
            System.out.println("get2:" + get2);
            System.out.println("get3:" + get3);
            System.out.println("get4:" + get4);
            System.out.println("get5:" + get5);
            System.out.println("get6:" + get6);

        }
        assertTrue(success);
    }
    @Test
    public void testServerKill3AndRespawn1() {

        Exception ex = null;
        IECSClient ecs = ObjectFactory.createECSClient("localhost", 9081);
        new Thread(ecs).start();
        IKVMessage put1 = KVMessageFactory.createFailedMessage("undefined"),
                put2 = KVMessageFactory.createFailedMessage("undefined"),
                put3 = KVMessageFactory.createFailedMessage("undefined"),
                get1 = KVMessageFactory.createFailedMessage("undefined"),
                get2 = KVMessageFactory.createFailedMessage("undefined"),
                get3 = KVMessageFactory.createFailedMessage("undefined"),
                get4 = KVMessageFactory.createFailedMessage("undefined"),
                get5 = KVMessageFactory.createFailedMessage("undefined"),
                get6 = KVMessageFactory.createFailedMessage("undefined");
        try {
            IKVStorage storage1 = new KVFileStorage("ecs_test_kv1.dat");
            storage1.clearStorage();
            IKVServer server1 = (ObjectFactory.createKVServerObject(9091, "localhost", 10, "FIFO", storage1));
            final ECSConnection ecsConn1 = new ECSConnection("localhost", 9081, server1, "localhost:9091");
            ecsConn1.connect();

            IKVStorage storage2 = new KVFileStorage("ecs_test_kv2.dat");
            storage2.clearStorage();
            IKVServer server2 = (ObjectFactory.createKVServerObject(9092, "localhost", 10, "FIFO", storage2));
            final ECSConnection ecsConn2 = new ECSConnection("localhost", 9081, server2, "localhost:9092");
            ecsConn2.connect();

            IKVStorage storage3 = new KVFileStorage("ecs_test_kv3.dat");
            storage3.clearStorage();
            IKVServer server3 = (ObjectFactory.createKVServerObject(9093, "localhost", 10, "FIFO", storage3));
            final ECSConnection ecsConn3 = new ECSConnection("localhost", 9081, server3, "localhost:9093");
            ecsConn3.connect();

            IKVStorage storage4 = new KVFileStorage("ecs_test_kv4.dat");
            storage4.clearStorage();
            IKVServer server4 = (ObjectFactory.createKVServerObject(9094, "localhost", 10, "FIFO", storage4));
            final ECSConnection ecsConn4 = new ECSConnection("localhost", 9081, server4, "localhost:9094");
            ecsConn4.connect();
            Thread.sleep(1000);

            KVStore kvClient = new KVStore(ObjectFactory.createHashRing(), "localhost", 9091);
            put1 = kvClient.put("asd", "asd");
            put2 = kvClient.put("asd1", "asd1");
            put3 = kvClient.put("7", "7");
            Thread.sleep(2000);
            get1 = kvClient.get("asd");
            get2 = kvClient.get("asd1");
            get3 = kvClient.get("7");
            ecsConn2.close(true);
            Thread.sleep(2000);
            ecsConn1.close(true);
            Thread.sleep(2000);
            ecsConn3.close(true);
            Thread.sleep(2000);
            IKVStorage storage5 = new KVFileStorage("ecs_test_kv5.dat");
            storage5.clearStorage();
            IKVServer server5 = (ObjectFactory.createKVServerObject(9095, "localhost", 10, "FIFO", storage5));
            final ECSConnection ecsConn5 = new ECSConnection("localhost", 9081, server5, "localhost:9095");
            ecsConn5.connect();
            Thread.sleep(2000);
            get4 = kvClient.get("asd");
            get5 = kvClient.get("asd1");
            get6 = kvClient.get("7");
            ecsConn4.close(true);
            ecsConn5.close(true);

        }
        catch (InterruptedException | IOException e) {
            ex = e;
            System.out.println(e.getMessage());
        }
        boolean success = ex == null
                && put1.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && put2.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && put3.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && get1.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get2.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get3.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get4.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get5.getStatus() == IKVMessage.StatusType.GET_SUCCESS
                && get6.getStatus() == IKVMessage.StatusType.GET_SUCCESS;
        if (!success) {
            if (ex != null)
                System.out.println(ex.getMessage());
            System.out.println("put1:" + put1);
            System.out.println("put2:" + put2);
            System.out.println("put3:" + put3);
            System.out.println("get1:" + get1);
            System.out.println("get2:" + get2);
            System.out.println("get3:" + get3);
            System.out.println("get4:" + get4);
            System.out.println("get5:" + get5);
            System.out.println("get6:" + get6);

        }
        assertTrue(success);
    }

     */
}
