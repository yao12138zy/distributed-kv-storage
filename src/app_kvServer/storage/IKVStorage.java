package app_kvServer.storage;

import java.util.ArrayList;
import java.util.Set;

public interface IKVStorage {
    public enum PutOperationResult {
        SUCCESS,
        UPDATE,
        DELETE, // for delete success
        ERROR  // for error in (delete, insert, update)
    }
    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);
    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      return code:
     *      1 -> put successfully; -1 put failed
     *      2 -> update successfully; -2 update failed
     *      3 -> delete success; -3 delete failed
     *      0 -> unexpected occurs
     */
    public PutOperationResult putKV(String key, String value) throws Exception;

    /**
     * Clear the storage of the server
     */
    public void clearStorage();
    public Set<String> getKeys();
    public void deleteKeys(ArrayList<String> keys);


}
