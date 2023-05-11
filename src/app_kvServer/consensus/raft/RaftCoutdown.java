package app_kvServer.consensus.raft;

import java.util.concurrent.CountDownLatch;

public class RaftCoutdown {
    private final CountDownLatch latch;
    private int clusterSize;
    public RaftCoutdown(int clusterSize) {

        this.clusterSize = clusterSize;
        int count = getQuorum();
        latch = new CountDownLatch(count);
    }

    public int getQuorum() {
        return (clusterSize / 2) + 1;
    }


    public void voteReceived() {

        latch.countDown();
        System.out.println("Vote received, still need:" + latch.getCount());
    }

    public boolean waitForVotes(long timeoutMs) {
        try {
            return latch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Thread interrupted: " + e.getMessage());
            return false;
        }
    }

}
