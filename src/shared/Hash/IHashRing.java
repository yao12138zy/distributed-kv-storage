package shared.Hash;

import com.sun.source.tree.Tree;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public interface IHashRing {
    public List<String> addServer(String key);

    public List<String> removeServer(String key);

    boolean keyInRange(String key, byte[] start, byte[] end);

    public String getServerByKey(String key);
    public String serialize();
    public TreeMap<byte[], String> deserialize(String ser_ring);
    public boolean serverExist(String key);

    public TreeMap<byte[], String>getHashRingMap();

    public List<String> affectedServers(String newServer);
    public ArrayList<KeyTransferInfo> getTransferInfo(String newMetaData) throws NoSuchAlgorithmException;


    ArrayList<byte[]> syncRange(String node);

    ArrayList<KeyTransferInfo> syncData(String newMetaData) throws NoSuchAlgorithmException;

    ArrayList<String> getReplicas(String coordinatorKey);

    public boolean isEmpty();
}
