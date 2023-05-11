package app_kvServer.cache;

import app_kvServer.IKVServer;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache extends LinkedHashMap<String, String> implements IKVCache {
    private int capacity;
    public LRUCache()
    {
        super(100, 0.75F, true);
        this.capacity = capacity;
    }

    public LRUCache(int capacity)
    {
        super(capacity, 0.75F, true);
        this.capacity = capacity;
    }


    @Override
    public String get(String key) {
        return super.getOrDefault(key, null);
    }

    @Override
    public String put(String key, String value) {
        if (value == null) {
            super.remove(key);
        } else {
            super.put(key, value);
        }
        return key;
    }

    @Override
    public boolean inCache(String key) {
        return super.containsKey(key);
    }

    @Override
    public int getCacheSize() {
        return this.capacity;
    }

    @Override
    public IKVServer.CacheStrategy getCacheStrategy() {
        return IKVServer.CacheStrategy.LRU;
    }

    @Override
    public void clearCache() {
        this.clear();
    }


    @Override
    protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
        return size() > capacity;
    }
}
