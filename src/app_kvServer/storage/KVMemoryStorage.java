package app_kvServer.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class KVMemoryStorage implements IKVStorage {
    private final HashMap<String, String> storage = new HashMap<>();
    public KVMemoryStorage(){

    }
    @Override
    public boolean inStorage(String key) {
        return storage.containsKey(key);
    }

    @Override
    public String getKV(String key) throws Exception {
        if (!inStorage(key))
            throw new Exception("key:" + key + "Does not exist!");
        return storage.get(key);
    }

    @Override
    public PutOperationResult putKV(String key, String value) throws Exception {
        if ("".equals(value)) {
            value = null;
        }
        String previousValue = storage.put(key, value);
        if (previousValue == null)
            return PutOperationResult.SUCCESS;
        else
            return PutOperationResult.UPDATE;
    }

    @Override
    public void clearStorage() {
        storage.clear();
    }

    @Override
    public Set<String> getKeys() {
        return null;
    }

    @Override
    public void deleteKeys(ArrayList<String> keys) {

    }
}
