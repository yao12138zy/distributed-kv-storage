package client;

import shared.KVMessageFactory;
import shared.messages.IKVMessage;

import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class KVSocketResponseListener implements IKVSocketListener {
    private Set<IKVMessage.StatusType> statusTypes;
    private IKVMessage msg = null;
    private Semaphore semCon = new Semaphore(0);
    private Semaphore semProd = new Semaphore(1);
    public KVSocketResponseListener(Set<IKVMessage.StatusType> statusTypes) {
        this.statusTypes = statusTypes;
    }
    @Override
    public void handleNewMessage(IKVMessage msg) throws InterruptedException {
        IKVMessage.StatusType statusType = msg.getStatus();
        //System.out.println("[Debug]NewListenerMessage:" + statusType);
        //System.out.println("[Debug]NewListenerMessage2:" + statusTypes.contains(statusType));
        //System.out.println("[DEBUG]getQueueLength"+ semCon.getQueueLength());
        //System.out.println("[DEBUG]availablePermits"+ semCon.availablePermits());
        //System.out.println("[DEBUG]drainPermits"+ semCon.drainPermits());
        if (statusTypes.contains(statusType) && semCon.getQueueLength() > 0) {
            semProd.tryAcquire(5, TimeUnit.SECONDS);
            this.msg = msg;
            semCon.release();
        }
    }
    public IKVMessage getMsg() throws InterruptedException {
        try{
            if (!semCon.tryAcquire(5, TimeUnit.SECONDS)) {
                this.msg = KVMessageFactory.createFailedMessage("The server did not respond");
            }
            return this.msg;
        }
        finally {
            semProd.release();
        }
    }
    @Override
    public void handleStatus(SocketStatus status) {

    }
}
