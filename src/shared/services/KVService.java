package shared.services;

import app_kvServer.cache.IKVCache;
import app_kvServer.storage.IKVStorage;
import shared.Hash.IHashRing;
import shared.messages.serialization.IKVMessageSerializer;

public class KVService {
    private static KVService instance;
    public static void setInstance(KVService service){
        KVService.instance = service;
    }
    public static KVService getInstance(){
        if (instance == null){
            instance = new KVService();
            return instance;
        }
        return instance;
    }

    private IKVMessageSerializer kvMessageSerializer;

    public void setKvSerializer(IKVMessageSerializer kvMessageSerializer){
        this.kvMessageSerializer = kvMessageSerializer;
    }

    public IKVMessageSerializer getKvMessageSerializer(){
        return kvMessageSerializer;
    }

}
