package app_kvServer.cache;

import app_kvServer.IKVServer;

public interface IKVCache {
    public String get(String key);
    public String put(String key, String value);
    public boolean inCache(String key);
    public int getCacheSize();
    public IKVServer.CacheStrategy getCacheStrategy();
    public void clearCache();
}
