package app_kvECS;




public class RequestQueueItem {
    private RequestQueueType queueType;
    private StorageServerHandler kvServer;
    public RequestQueueItem(RequestQueueType queueType, StorageServerHandler kvServer) {
        this.queueType = queueType;
        this.kvServer = kvServer;
    }
    public RequestQueueType getRequestQueueType() {
        return queueType;
    }
    public StorageServerHandler getStorageServerHandler() {
        return kvServer;
    }
}
