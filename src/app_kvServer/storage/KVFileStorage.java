package app_kvServer.storage;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVFileStorage implements IKVStorage
{
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Lock writeLock = readWriteLock.writeLock();
    private Lock readLock = readWriteLock.readLock();
    private String dbPath = "";
    public KVFileStorage(String dbPath)
    {
        this.dbPath = dbPath;
        File db = new File(dbPath);
        if (!db.exists()) {
            this.clearStorage();
        }
    }

    @Override
    public boolean inStorage(String key) {
        FileInputStream fi;
        ObjectInputStream in;
        Object obj;
        try {
            readLock.lock();
            fi = new FileInputStream(dbPath);
            in = new ObjectInputStream(fi);
            obj = in.readObject();
            in.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            readLock.unlock();
        }
        HashMap<String, String> hashMap = new HashMap<String, String>((HashMap) obj);
        return hashMap.containsKey(key);
    }

    @Override
    public String getKV(String key) {
        String value;
        FileInputStream fi;
        ObjectInputStream in;
        Object obj;
        try {
            readLock.lock();
            fi = new FileInputStream(dbPath.toString());
            in = new ObjectInputStream(fi);
            obj = in.readObject();
            in.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            readLock.unlock();
        }
        HashMap<String, String> hashMap = new HashMap<String, String>((HashMap) obj);
        value = hashMap.get(key);
        return value;
    }

    @Override
    public PutOperationResult putKV(String key, String value) throws Exception {
        // delete key
        FileInputStream fi;
        FileOutputStream fo;
        ObjectOutputStream out;
        ObjectInputStream in;
        Object obj;
        int rc = 0;
        boolean keyExist;
        try {
            writeLock.lock();
            fi = new FileInputStream(dbPath.toString());
            in = new ObjectInputStream(fi);
            obj = in.readObject();
            HashMap<String, String> hashMap = new HashMap<String, String>((HashMap) obj);
            keyExist = hashMap.containsKey(key);
            if (value == null) { // delete section
                rc = -3;
                if (keyExist) {
                    hashMap.remove(key);
                }
            } else {
                if (keyExist) {
                    rc = -2;
                } else {
                    rc = -1;
                }
                hashMap.put(key, value);
            }
            fo = new FileOutputStream(dbPath);
            out = new ObjectOutputStream(fo);
            out.writeObject(hashMap);
            in.close();
            fi.close();
            out.close();
            fo.close();
            rc = rc * -1;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            writeLock.unlock();
        }
        //return rc;
        if (rc == 1)
            return PutOperationResult.SUCCESS;
        else if (rc == 2)
            return PutOperationResult.UPDATE;
        else if (rc == 3)
            return PutOperationResult.DELETE;
        else
            throw new Exception("putKV Error");
    }

    @Override
    public void clearStorage() {
        FileOutputStream fo;
        ObjectOutputStream out;
        try {
            writeLock.lock();
            fo = new FileOutputStream(dbPath);
            out = new ObjectOutputStream(fo);
            HashMap<String, String> emptyHash = new HashMap<String, String>();
            out.writeObject(emptyHash);
            out.close();
            fo.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<String> getKeys() {
        FileInputStream fi;
        ObjectInputStream in;
        Object obj;
        try {
            readLock.lock();
            fi = new FileInputStream(dbPath.toString());
            in = new ObjectInputStream(fi);
            obj = in.readObject();
            in.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            readLock.unlock();
        }
        HashMap<String, String> hashMap = new HashMap<String, String>((HashMap) obj);
        return hashMap.keySet();
    }

    @Override
    public void deleteKeys(ArrayList<String> keys) {
        FileInputStream fi;
        FileOutputStream fo;
        ObjectOutputStream out;
        ObjectInputStream in;
        Object obj;
        try {
            writeLock.lock();
            fi = new FileInputStream(dbPath.toString());
            in = new ObjectInputStream(fi);
            obj = in.readObject();
            HashMap<String, String> hashMap = new HashMap<String, String>((HashMap) obj);
            for (String key : keys) {
                hashMap.remove(key);
            }
            fo = new FileOutputStream(dbPath);
            out = new ObjectOutputStream(fo);
            out.writeObject(hashMap);
            in.close();
            fi.close();
            out.close();
            fo.close();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            writeLock.unlock();
        }
    }
}
