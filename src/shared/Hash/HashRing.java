package shared.Hash;


import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class HashRing implements IHashRing {

    /**
     *  key for hashRing starts from prev position and ends with self.
     * i.e. if first server is 0x00ff, then the server takes care of 0x00ff - next one;
     */
    private TreeMap<byte[], String> hashRing = new TreeMap<byte[], String>(new SortByteArray());
    private MessageDigest md = MessageDigest.getInstance("MD5");

    public HashRing() throws NoSuchAlgorithmException {

    }

    /** functions for testing  */
    public void setTreeMap(TreeMap<byte[],String> tmap) {
        this.hashRing = (TreeMap<byte[], String>) tmap.clone();
    }


    @Override
    public List<String> affectedServers(String newServer) {
        List<String> affectedList = new ArrayList<String>();

        if (hashRing.isEmpty()) { // first server
            return affectedList;
        } else if (hashRing.size() == 1) {
            affectedList.add(hashRing.firstEntry().getValue());
            return affectedList;
        }
        byte[] newKey = md.digest(newServer.getBytes());
        byte[] lowerKey = hashRing.lowerKey(newKey);
        byte[] higherKey = hashRing.higherKey(newKey);
        if (lowerKey == null) {
            affectedList.add(hashRing.lastEntry().getValue());
        } else {
            affectedList.add(hashRing.get(lowerKey));
        }
        if (higherKey == null) {
            affectedList.add(hashRing.firstEntry().getValue());
        } else {
            affectedList.add(hashRing.get(higherKey));
        }
        return affectedList;
    }

    @Override
    public List<String> addServer(String key) {
        List<String> affectedList = affectedServers(key);
        try {
            byte[] hashedKey = md.digest(key.getBytes());
            //System.out.println(hashedKey);
            hashRing.put(hashedKey, key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return affectedList;
    }

    @Override
    public List<String> removeServer(String key) {
        List<String> affectedList = affectedServers(key);
        try {
            byte[] hashedKey = md.digest(key.getBytes());
            hashRing.remove(hashedKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return affectedList;
    }

    @Override
    public boolean serverExist(String key) {
        return hashRing.containsKey(md.digest(key.getBytes()));
    }

    @Override
    public TreeMap<byte[], String> getHashRingMap() {
        return this.hashRing;
    }

    @Override
    public boolean keyInRange(String key, byte[] start, byte[] end) {
        boolean result = false;
        try {
            byte[] hashedKey = md.digest(key.getBytes());
            if ((UnsignedBytes.lexicographicalComparator().compare(end,start)) >= 0) {
                result = (UnsignedBytes.lexicographicalComparator().compare(hashedKey,start) > 0)
                        && (UnsignedBytes.lexicographicalComparator().compare(hashedKey,end) <= 0);
            } else {
                result = (UnsignedBytes.lexicographicalComparator().compare(hashedKey,start) > 0)
                        || (UnsignedBytes.lexicographicalComparator().compare(hashedKey,end) <= 0);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /*
    public boolean keyTest(byte[] key, byte[] start, byte[] end) {
        boolean result = false;
        try {
            if ((UnsignedBytes.lexicographicalComparator().compare(end,start)) >= 0) {
                result = (UnsignedBytes.lexicographicalComparator().compare(key,start) > 0)
                        && (UnsignedBytes.lexicographicalComparator().compare(key,end) <= 0);
            } else {
                result = (UnsignedBytes.lexicographicalComparator().compare(key,start) > 0)
                        || (UnsignedBytes.lexicographicalComparator().compare(key,end) <= 0);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
    */

    @Override
    public String getServerByKey(String client_pair) {
        byte[] hashedKey = md.digest(client_pair.getBytes());
        //System.out.println(DatatypeConverter.printHexBinary(hashedKey));
        Map.Entry<byte[], String> entry = hashRing.ceilingEntry(hashedKey);
        if (entry == null) {
            return hashRing.firstEntry().getValue();
        } else {
            return entry.getValue();
        }
    }

    public String serialize() {
        String output = "";
        String start_pos; // exclude
        String end_pos;  // inclusive
        for (Map.Entry<byte[], String> entry : hashRing.entrySet()) {
            if (entry.getKey() == hashRing.firstKey()) {
                start_pos = DatatypeConverter.printHexBinary(hashRing.lastKey());
                end_pos = DatatypeConverter.printHexBinary(entry.getKey());
            } else {
                start_pos = DatatypeConverter.printHexBinary(hashRing.lowerKey(entry.getKey()));
                end_pos = DatatypeConverter.printHexBinary(entry.getKey());
            }
            output += String.format("%s,%s,%s;", start_pos, end_pos, entry.getValue());
        }
        return output;
    }


    public TreeMap<byte[], String> deserialize(String ser_ring) {
        ser_ring = ser_ring.trim();
        //System.out.println(ser_ring);
        hashRing.clear();
        String[] entries = ser_ring.split(";");
        //System.out.println(entries);
        //System.out.println("end");
        for (String entry : entries) {
            //System.out.println(entry);
            String[] slices = entry.split(",");
            //System.out.println(String.format("One server: %s,%s,%s\n",slices[0],slices[1],slices[2]));
            byte[] end_pos = DatatypeConverter.parseHexBinary(slices[1]);
            String value = slices[2];
            hashRing.put(end_pos, value);
        }
        return this.hashRing;
    }

    private Map.Entry<byte[], String> getLowerEntry(TreeMap<byte[], String> hashRing, byte[] key) {
        Map.Entry<byte[], String> result = hashRing.lowerEntry(key);
        if (result == null) {
            result = hashRing.lastEntry();
        }
        return result;
    }
    private Map.Entry<byte[], String> getHigherEntry(TreeMap<byte[], String> hashRing, byte[] key) {
        Map.Entry<byte[], String> result = hashRing.higherEntry(key);
        if (result == null) {
            result = hashRing.firstEntry();
        }
        return result;
    }

    @Override
    public ArrayList<KeyTransferInfo> getTransferInfo(String newMetaData) throws NoSuchAlgorithmException {
        HashRing ring = new HashRing();
        TreeMap<byte[], String> newHashRing = ring.deserialize(newMetaData);
        Set<Map.Entry<byte[], String> > newSet = newHashRing.entrySet();
        Set<Map.Entry<byte[], String> > oldSet= this.hashRing.entrySet();
        ArrayList<KeyTransferInfo> info_list = new ArrayList<>();

        Set<Map.Entry<byte[], String> > handleNodes =  Sets.difference(Sets.union(newSet, oldSet),Sets.intersection(newSet, oldSet));
        for (Map.Entry<byte[], String> node : handleNodes) {
            if (newSet.contains(node) && !oldSet.contains(node)) { // newly added
                Map.Entry<byte[], String> sender = getHigherEntry(this.hashRing, node.getKey());
                byte[] byte_range_start = getLowerEntry(newHashRing, node.getKey()).getKey();
                info_list.add(new KeyTransferInfo(sender.getValue(), node.getValue(), byte_range_start, node.getKey()));
            } else if (!newSet.contains(node) && oldSet.contains(node)){ // old ones removed
                Map.Entry<byte[], String> receiver = getHigherEntry(newHashRing, node.getKey());
                byte[] byte_range_start = getLowerEntry(newHashRing, node.getKey()).getKey();
                info_list.add(new KeyTransferInfo(node.getValue(), receiver.getValue(), byte_range_start, node.getKey()));
            }
        }
        return info_list;
    }
    public ArrayList<byte[]> getReplicaServers(byte[] node) {
        ArrayList<byte[]> nodes = new ArrayList<>();
        //Map.Entry<byte[], String> coordinator = hashRing.ceilingEntry(node);
        //System.out.println(DatatypeConverter.printHexBinary(node));
        Map.Entry<byte[], String> replica1 = getHigherEntry(this.hashRing, node);
        //System.out.println(replica1.getValue());
        if (!replica1.getKey().equals(node)) {
            nodes.add(replica1.getKey());
        }
        Map.Entry<byte[], String> replica2 = getHigherEntry(this.hashRing, replica1.getKey());
        //System.out.println(replica2.getValue());
        if (!replica2.getKey().equals(node) && !replica2.equals(replica1)) {
            nodes.add(replica2.getKey());
        }
        return nodes;
    }

    @Override
    public ArrayList<byte[]> syncRange(String node) {
        byte[] hashedKey = md.digest(node.getBytes());
        Map.Entry<byte[], String> replica1 = getLowerEntry(hashRing,hashedKey);
        Map.Entry<byte[], String> replica2 = getLowerEntry(hashRing,replica1.getKey());
        Map.Entry<byte[], String> start_range = getLowerEntry(hashRing,replica2.getKey());
        ArrayList<byte[]> range = new ArrayList<>();
        //range.add(hashedKey);
        //range.add(hashedKey);
        if (!replica1.getKey().equals(node) && !replica2.getKey().equals(node) && !start_range.getKey().equals(node)) {
            range.add(start_range.getKey());
            range.add(hashedKey);
        }
        return range;
    }

    @Override
    public ArrayList<KeyTransferInfo> syncData(String newMetaData) throws NoSuchAlgorithmException {
        ArrayList<KeyTransferInfo> infoList = getTransferInfo(newMetaData);
        ArrayList<KeyTransferInfo> replicaList = new ArrayList<>();
        HashRing newRing = new HashRing();
        TreeMap<byte[], String> newMap = newRing.deserialize(newMetaData);
        for (KeyTransferInfo info : infoList) {
            byte[] senderKey = md.digest(info.sender.getBytes());
            byte[] receiverKey = md.digest(info.receiver.getBytes());
            //byte[] senderKey = info.sender.getBytes();
            //byte[] receiverKey = info.receiver.getBytes();
            int index = infoList.indexOf(info);
            //System.out.println("info: " + info.toString());
            if (!newMap.containsKey(senderKey)) {
                ArrayList<byte[]> replicas = getReplicaServers(senderKey);
                //System.out.println("replicas: " + hashRing.get(replicas.get(0)) + ", " + hashRing.get(replicas.get(1)));
                if (replicas.size() > 0 && newMap.containsKey(replicas.get(0))) {
                    KeyTransferInfo newInfo = new KeyTransferInfo(newMap.get(replicas.get(0)),info.receiver,info.range_start,info.range_end);
                    infoList.set(index, newInfo);
                } else if (replicas.size() > 1 && newMap.containsKey(replicas.get(1))) {
                    KeyTransferInfo newInfo = new KeyTransferInfo(newMap.get(replicas.get(1)),info.receiver,info.range_start,info.range_end);
                    infoList.set(index, newInfo);
                }
            }
            info = infoList.get(index);
            ArrayList<byte[]> receivers = newRing.getReplicaServers(receiverKey);
            for (byte[] receiver : receivers) {
                KeyTransferInfo newInfo = new KeyTransferInfo(info.sender,newMap.get(receiver),info.range_start,info.range_end);

                replicaList.add(newInfo);
            }
        }
        infoList.addAll(replicaList);

        return infoList;
    }

    @Override
    public ArrayList<String> getReplicas(String coordinatorConnettionString){
        if (hashRing.size() <= 1)
            return new ArrayList<>();
        byte[] coordinatorKeyBytes = md.digest(coordinatorConnettionString.getBytes());
        ArrayList<byte[]> replicaListByte = getReplicaServers(coordinatorKeyBytes);
        ArrayList<String> replicaConnectionList = new ArrayList<>();
        for (byte[] replicaByte : replicaListByte) {
            String replicaConnectionString = hashRing.get(replicaByte);
            if (!coordinatorConnettionString.equals(replicaConnectionString))
                replicaConnectionList.add(replicaConnectionString);
        }
        return replicaConnectionList;
    }


    @Override
    public boolean isEmpty() {
        return hashRing.isEmpty();
    }
}


