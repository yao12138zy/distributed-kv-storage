package app_kvServer.cache;

import app_kvServer.IKVServer;

public class EmptyCache implements IKVCache {
    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public String put(String key, String value) {
        return null;
    }

    @Override
    public boolean inCache(String key) {
        return false;
    }

    @Override
    public int getCacheSize() {
        return 0;
    }

    @Override
    public IKVServer.CacheStrategy getCacheStrategy() {
        return IKVServer.CacheStrategy.None;
    }

    @Override
    public void clearCache() {

    }
}
