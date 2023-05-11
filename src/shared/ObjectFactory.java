package shared;

import app_kvClient.IKVClient;
import app_kvECS.ECSClient;
import app_kvECS.IECSClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import app_kvServer.cache.EmptyCache;
import app_kvServer.cache.IKVCache;
import app_kvServer.cache.LRUCache;
import app_kvServer.storage.IKVStorage;
import shared.Hash.HashRing;
import shared.Hash.IHashRing;

import java.security.NoSuchAlgorithmException;

public final class ObjectFactory {
	/*
	 * Creates a KVClient object for auto-testing purposes
	 */
    public static IKVClient createKVClientObject() {
        // TODO Auto-generated method stub
    	return null;
    }
    
    /*
     * Creates a KVServer object for auto-testing purposes
     */
	public static IKVServer createKVServerObject(int port, String address, int cacheSize, String strategy, IKVStorage storage, String leaderAddress) {
		IKVServer server = new KVServer(port, address, 10, strategy, storage, leaderAddress);
		return server;
	}

	public static IKVCache createKVCache(String strategy, int cacheSize) {
		if (IKVServer.CacheStrategy.LRU.toString().equals(strategy)) {
			return new LRUCache(cacheSize);
		} else {
			return new EmptyCache();
		}
	}

	public static IECSClient createECSClient(String address, int port, IHashRing metadata) {
		IECSClient ecs = new ECSClient(address, port, metadata);
		return ecs;
	}

	public static IHashRing createHashRing() {
		try	{
			return new HashRing();
		}
		catch (NoSuchAlgorithmException e) {
			System.out.println("CreateHashRing error!" + e.getMessage());
		}
		return null;
	}
}