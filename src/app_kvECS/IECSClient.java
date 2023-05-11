package app_kvECS;

import java.util.Map;
import java.util.Collection;

import ecs.IECSNode;

public interface IECSClient extends Runnable {

    /**
     * Removes nodes with names matching the nodeNames array
     * @param nodeNames names of nodes to remove
     * @return  true on success, false otherwise
     */
    public boolean removeNodes(Collection<String> nodeNames);

    /**
     * Get a map of all nodes
     */
    public Map<String, IECSNode> getNodes();

    /**
     * Get the specific node responsible for the given key
     */
    public IECSNode getNodeByKey(String Key);

    public boolean joinNode(String hashIdentity, StorageServerHandler node);
    public boolean removeNode(String hashIdentity, StorageServerHandler node);

    public void reportNodeTransferComplete(int proposalNumber);
}
