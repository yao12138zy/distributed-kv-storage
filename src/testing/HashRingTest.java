package testing;

import junit.framework.TestCase;
import org.apache.log4j.helpers.SyslogQuietWriter;
import org.junit.Test;
import shared.Hash.HashRing;
import shared.Hash.IHashRing;
import shared.Hash.KeyTransferInfo;
import shared.Hash.SortByteArray;
import shared.messages.IKVMessage;

import javax.xml.bind.DatatypeConverter;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;

public class HashRingTest extends TestCase {
    @Test
    public void testSerialization() {

        Exception ex = null;
        String serializedText1 = "1", serializedText2 = "2";
        try {
            IHashRing hashRing = new HashRing();
            hashRing.addServer("127.0.0.1:8082");
            hashRing.addServer("127.0.0.1:8083");
            hashRing.addServer("127.0.0.1:8084");
            serializedText1 = hashRing.serialize();
            IHashRing hashRing2 = new HashRing();
            hashRing2.deserialize(serializedText1);
            serializedText2 = hashRing2.serialize();
        }
        catch (NoSuchAlgorithmException e) {
            ex = e;
        }

        assertTrue(ex == null && serializedText1 != null && serializedText1.equals(serializedText2));
    }

    @Test
    public void testHashRegionInclusiveCases() {

        Exception ex = null;
        boolean test1 = false, test2 = false, test3 = false;
        try {
            IHashRing hashRing = new HashRing();
            hashRing.addServer("127.0.0.1:8082");
            hashRing.addServer("127.0.0.1:8083");
            hashRing.addServer("127.0.0.1:8084");

            test1 = "127.0.0.1:8082".equals(hashRing.getServerByKey("127.0.0.1:8082"));
            test2 = "127.0.0.1:8083".equals(hashRing.getServerByKey("127.0.0.1:8083"));
            test3 = "127.0.0.1:8084".equals(hashRing.getServerByKey("127.0.0.1:8084"));
        }
        catch (NoSuchAlgorithmException e) {
            ex = e;
        }

        assertTrue(ex == null && test1 && test2 && test3);
    }

    @Test
    public void testHashRegion() {

        Exception ex = null;
        boolean test1 = false, test2 = false, test3 = false, test4 = false;
        try {
            IHashRing hashRing = new HashRing();
            hashRing.addServer("127.0.0.1:8082");
            hashRing.addServer("127.0.0.1:8083");
            hashRing.addServer("127.0.0.1:8084");
            /*
            [SELF], [PREDECESSOR], [VALUE]
            [SELF] > [PREDECESSOR] unless wrapped
            to [SELF], from [PREDECESSOR], [VALUE]
            91D8C8E93642CCA5C0F2BE69D199EE22,85D27949E81CBE4C3D605C59CD7D7DC3,127.0.0.1:8082;
            85D27949E81CBE4C3D605C59CD7D7DC3,6A93711A8C3F9C2641DFC71C359A1ABE,127.0.0.1:8084;
            6A93711A8C3F9C2641DFC71C359A1ABE,91D8C8E93642CCA5C0F2BE69D199EE22,127.0.0.1:8083; [WRAPPED]
            7815696ecbf1c96e6894b779456d330e:asd  (belongs to 8084)
            f5b3b9b303f5a0594272f99d191bbf45:asd1 (belongs to 8083)
            a67995ad3ec084cb38d32725fd73d9a3:asd2 (belongs to 8083)
            8f14e45fceea167a5a36dedd4bea2543:7    (belongs to 8082)
             */
            test1 = "127.0.0.1:8084".equals(hashRing.getServerByKey("asd"));
            test2 = "127.0.0.1:8083".equals(hashRing.getServerByKey("asd1"));
            test3 = "127.0.0.1:8083".equals(hashRing.getServerByKey("asd2"));
            test4 = "127.0.0.1:8082".equals(hashRing.getServerByKey("7"));
        }
        catch (NoSuchAlgorithmException e) {
            ex = e;
        }

        assertTrue(ex == null && test1 && test2 && test3 && test4);
    }

    @Test
    public void testServerExist() {

        Exception ex = null;
        boolean test1 = false, test2 = false, test3 = false, test4 = false;
        try {
            IHashRing hashRing = new HashRing();
            hashRing.addServer("127.0.0.1:8082");
            hashRing.addServer("127.0.0.1:8083");
            hashRing.addServer("127.0.0.1:8084");
            test1 = hashRing.serverExist("127.0.0.1:8082");
            test2 = hashRing.serverExist("127.0.0.1:8083");
            test3 = hashRing.serverExist("127.0.0.1:8084");
            test4 = hashRing.serverExist("127.0.0.1:8085");
        }
        catch (NoSuchAlgorithmException e) {
            ex = e;
        }

        assertTrue(ex == null && test1 && test2 && test3 && !test4);
    }

    @Test
    public void testRemoveServer() {

        Exception ex = null;
        boolean test1 = false, test2 = false;
        try {
            IHashRing hashRing1 = new HashRing();
            hashRing1.addServer("127.0.0.1:8082");
            hashRing1.addServer("127.0.0.1:8083");
            hashRing1.addServer("127.0.0.1:8084");
            String text1 = hashRing1.serialize();
            IHashRing hashRing2 = new HashRing();

            hashRing2.addServer("127.0.0.1:8084");
            hashRing2.addServer("127.0.0.1:8085");
            hashRing2.addServer("127.0.0.1:8082");
            hashRing2.addServer("127.0.0.1:8083");
            String text2 = hashRing2.serialize();
            hashRing2.removeServer("127.0.0.1:8085");
            String text3 = hashRing2.serialize();
            test1 = !text1.equals(text2);
            test2 = text1.equals(text3);
        }
        catch (NoSuchAlgorithmException e) {
            ex = e;
        }

        assertTrue(ex == null && test1 && test2);
    }

    @Test
    public void testKeyTransferInfo() {
        Exception ex = null;
        String stdResult = "[KeyTransferInfo{sender='server2', receiver='server6', range_start=0000000F, range_end=000000CC}, KeyTransferInfo{sender='server4', receiver='server5', range_start=000000CC, range_end=E1110000}, KeyTransferInfo{sender='server2', receiver='server5', range_start=000000CC, range_end=000F0000}, KeyTransferInfo{sender='server3', receiver='server5', range_start=000000CC, range_end=E0000000}]";
        String result = null;
        try {
            HashRing hashRing = new HashRing();
            TreeMap<byte[], String> tmap = new TreeMap<byte[], String>(new SortByteArray());
            tmap.put(DatatypeConverter.parseHexBinary("0000000f"), "server1");
            tmap.put(DatatypeConverter.parseHexBinary("000f0000"), "server2");
            tmap.put(DatatypeConverter.parseHexBinary("e0000000"), "server3");
            tmap.put(DatatypeConverter.parseHexBinary("ffff0000"), "server4");
            hashRing.setTreeMap(tmap);

            HashRing hashRing2 = new HashRing();
            hashRing2.setTreeMap(tmap);

            tmap.put(DatatypeConverter.parseHexBinary("e1110000"),"server5");
            tmap.put(DatatypeConverter.parseHexBinary("000000cc"),"server6");
            tmap.remove(DatatypeConverter.parseHexBinary("000f0000"));
            tmap.remove(DatatypeConverter.parseHexBinary("e0000000"));
            hashRing.setTreeMap(tmap);
            result = String.valueOf(hashRing2.getTransferInfo(hashRing.serialize()));

        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && stdResult.equals(result));

    }

    @Test
    public void testReplicaThreeServers() {
        Exception ex = null;
        String stdResult = "";
        String result = null;
        boolean[] expected = new boolean[2];
        Arrays.fill(expected, false);
        try {
            HashRing metadata = new HashRing();
            metadata.addServer("127.0.0.1:9091");
            metadata.addServer("127.0.0.1:9092");
            metadata.addServer("127.0.0.1:9093");
            metadata.addServer("127.0.0.1:9094");
            metadata.removeServer("127.0.0.1:9091");
            ArrayList<String> replicas = metadata.getReplicas("127.0.0.1:9092");
            expected[0] = replicas.contains("127.0.0.1:9094");
            expected[1] = replicas.contains("127.0.0.1:9093");

        } catch (Exception e) {
            ex = e;
        }
        boolean success = ex == null;
        for (boolean r : expected) {
            success &= r;
        }
        if (!success) {
            if (ex != null) {
                System.out.println(ex.getMessage());
                ex.printStackTrace();
            }
            for (int i = 0; i < expected.length; i++) {
                if (!expected[i]) {
                    System.out.println("expected[" + i + "]:false");
                }
            }
        }
        assertTrue(success);

    }

    @Test
    public void testReplicaFourServers() {
        Exception ex = null;
        String stdResult = "";
        String result = null;
        boolean[] expected = new boolean[4];
        Arrays.fill(expected, false);
        try {
            HashRing metadata = new HashRing();
            metadata.addServer("127.0.0.1:9091");
            metadata.addServer("127.0.0.1:9092");
            metadata.addServer("127.0.0.1:9093");
            metadata.addServer("127.0.0.1:9094");

            ArrayList<String> replicas = metadata.getReplicas("127.0.0.1:9092");
            expected[0] = replicas.contains("127.0.0.1:9091");
            expected[1] = replicas.contains("127.0.0.1:9094");
            expected[2] = !replicas.contains("127.0.0.1:9093");
            expected[3] = !replicas.contains("127.0.0.1:9092");

        } catch (Exception e) {
            ex = e;
        }
        boolean success = ex == null;
        for (boolean r : expected) {
            success &= r;
        }
        if (!success) {
            if (ex != null) {
                System.out.println(ex.getMessage());
                ex.printStackTrace();
            }
            for (int i = 0; i < expected.length; i++) {
                if (!expected[i]) {
                    System.out.println("expected[" + i + "]:false");
                }
            }
        }
        assertTrue(success);

    }

    @Test
    public void testReplicaOnlyOne() {
        Exception ex = null;
        String stdResult = "";
        String result = null;
        boolean[] expected = new boolean[2];
        Arrays.fill(expected, false);
        try {
            HashRing metadata = new HashRing();
            metadata.addServer("127.0.0.1:9092");
            metadata.addServer("127.0.0.1:9093");

            ArrayList replicas = metadata.getReplicas("127.0.0.1:9092");
            expected[0] = replicas.contains("127.0.0.1:9093");
            expected[1] = !replicas.contains("127.0.0.1:9092");

        } catch (Exception e) {
            ex = e;
        }
        boolean success = ex == null;
        for (boolean r : expected) {
            success &= r;
        }
        if (!success) {
            if (ex != null) {
                System.out.println(ex.getMessage());
                ex.printStackTrace();
            }
            for (int i = 0; i < expected.length; i++) {
                if (!expected[i]) {
                    System.out.println("expected[" + i + "]:false");
                }
            }
        }
        assertTrue(success);

    }

    @Test
    public void testReplicaEmpty() {
        Exception ex = null;
        String stdResult = "";
        String result = null;
        boolean[] expected = new boolean[1];
        Arrays.fill(expected, false);
        try {
            HashRing metadata = new HashRing();
            metadata.addServer("127.0.0.1:9091");
            metadata.addServer("127.0.0.1:9092");
            metadata.addServer("127.0.0.1:9093");
            metadata.addServer("127.0.0.1:9094");
            metadata.removeServer("127.0.0.1:9091");
            metadata.removeServer("127.0.0.1:9094");
            metadata.removeServer("127.0.0.1:9093");
            ArrayList<String> replicas = metadata.getReplicas("127.0.0.1:9092");
            expected[0] = replicas.size() ==  0;

        } catch (Exception e) {
            ex = e;
        }
        boolean success = ex == null;
        for (boolean r : expected) {
            success &= r;
        }
        if (!success) {
            if (ex != null) {
                System.out.println(ex.getMessage());
                ex.printStackTrace();
            }
            for (int i = 0; i < expected.length; i++) {
                if (!expected[i]) {
                    System.out.println("expected[" + i + "]:false");
                }
            }
        }
        assertTrue(success);

    }


    @Test
    public void testSyncData() {
        Exception ex = null;
        String stdResult = "";
        String result = null;
        try {
            HashRing fourServers = new HashRing();
            fourServers.addServer("127.0.0.1:9091");
            fourServers.addServer("127.0.0.1:9092");
            fourServers.addServer("127.0.0.1:9093");
            fourServers.addServer("127.0.0.1:9094");
            HashRing threeServers = new HashRing();
            threeServers.deserialize(fourServers.serialize());
            threeServers.removeServer("127.0.0.1:9092");


            System.out.println("4 servers -> 3 servers");
            ArrayList<KeyTransferInfo> transferInfos = fourServers.syncData(threeServers.serialize());
            for (KeyTransferInfo info : transferInfos) {
                System.out.println(info);
            }
            HashRing twoServers = new HashRing();
            twoServers.deserialize(threeServers.serialize());
            twoServers.removeServer("127.0.0.1:9093");
            threeServers.syncData(twoServers.serialize());
            System.out.println("3 servers -> 2 servers");
            ArrayList<KeyTransferInfo> transferInfos2 = fourServers.syncData(twoServers.serialize());
            for (KeyTransferInfo info : transferInfos2) {
                System.out.println(info);
            }

        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && true);

    }

    @Test
    public void testKeyInRange() {
        Exception ex = null;
        boolean test1 = false, test2 = false, test3 = false, test4 = false, test5 = false, test6 = false, test7 = false;
        try {
            HashRing hashRing = new HashRing();
            // c4ca4238a0b923820dcc509a6f75849b:1
            test1 = hashRing.keyInRange("1",
                    DatatypeConverter.parseHexBinary("00000000000000000000000000000000"),
                    DatatypeConverter.parseHexBinary("ffffffffffffffffffffffffffffffff"));
            test2 = !hashRing.keyInRange("1",
                    DatatypeConverter.parseHexBinary("00000000000000000000000000000000"),
                    DatatypeConverter.parseHexBinary("bfffffffffffffffffffffffffffffff"));
            // Test "end]"
            test3 = hashRing.keyInRange("1",
                    DatatypeConverter.parseHexBinary("00000000000000000000000000000000"),
                    DatatypeConverter.parseHexBinary("c4ca4238a0b923820dcc509a6f75849b"));
            // Test "(start"
            test4 = !hashRing.keyInRange("1",
                    DatatypeConverter.parseHexBinary("c4ca4238a0b923820dcc509a6f75849b"),
                    DatatypeConverter.parseHexBinary("ffffffffffffffffffffffffffffffff"));
            // Wrapped case
            test5 = hashRing.keyInRange("1",
                    DatatypeConverter.parseHexBinary("bfffffffffffffffffffffffffffffff"),
                    DatatypeConverter.parseHexBinary("11111111111111111111111111111111"));
            // Test wrapped "(start"
            test6 = !hashRing.keyInRange("1",
                    DatatypeConverter.parseHexBinary("c4ca4238a0b923820dcc509a6f75849b"),
                    DatatypeConverter.parseHexBinary("11111111111111111111111111111111"));
            // Test wrapped "end]"
            test7 = hashRing.keyInRange("1",
                    DatatypeConverter.parseHexBinary("efffffffffffffffffffffffffffffff"),
                    DatatypeConverter.parseHexBinary("c4ca4238a0b923820dcc509a6f75849b"));
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && test1 && test2 && test3 && test4 && test5 && test6 && test7);

    }
}
